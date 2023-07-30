import os
import re
import asyncio
import zlib
import time
from datetime import date
from typing import Optional, Union
import logging
from functools import wraps

import m3u8
from m3u8.model import InitializationSection, Segment
import aiohttp

from utils import dump_data, get_searcher, get_downloader_session, Logging, AttrObj
from remuxer import RemuxerInterface, FMp4Remuxer
from config import config

EXPIRING_THRES = 120
DURATION_FALLBACK = 1
STALE_PLAYLIST_TIMEOUT = 30
TARGET_POOLED_PLAYLIST = 3
PLAYLIST_MAX_RETRIES = 10


_module_formater = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s', datefmt='%y-%m-%d %H:%M:%S')
_module_handlers = {}


def _get_handlers(logpath):
    if not _module_handlers.get(logpath):
        if logpath == 'stream':
            _module_handlers[logpath] = logging.StreamHandler()
            _module_handlers[logpath].formatter = _module_formater
            _module_handlers[logpath].setLevel(logging.WARNING)
        else:
            os.makedirs(os.path.dirname(logpath), exist_ok=True)
            _module_handlers[logpath] = logging.FileHandler(logpath, encoding='utf-8')
            _module_handlers[logpath].formatter = _module_formater
    return _module_handlers[logpath]


def _get_download_logger(name, logpath=f'rec/sisrec_dl-{date.today().strftime("%y%m%d")}.log'):
    logger = logging.getLogger(name)
    if logger.propagate:  # new logger instance
        logger.propagate = False
        logger.addHandler(_get_handlers('stream'))
        logger.addHandler(_get_handlers(logpath))
    return logger


logger = logging.getLogger(__name__)

_basedir_searcher = get_searcher(r'/(live[^/]+)(:?/index)?\.m3u8')
_expire_searcher = get_searcher(r'expires=(\d+)', transform=int)


class Playurl:
    def __init__(self, baseurl: str, url_info: dict, group: list["Playurl"]):
        self.group = group
        self.host: str = url_info['host']
        self.query: str = url_info['extra']
        self.baseurl: str = baseurl
        self.basedir: str = _basedir_searcher(baseurl) or baseurl.split('/', maxsplit=1)[-1]
        self.expire_ts: int = _expire_searcher(self.query)
        self.is_valid: bool = True
        self.full_url = f'{self.host}{self.baseurl}{self.query}'
        self.require_seq_auth: Optional[bool] = None

    @classmethod
    def from_playurl(cls, hls_format: AttrObj) -> list["Playurl"]:
        group = []
        for url_info in hls_format.url_info:
            playurl = cls(hls_format.base_url.value, url_info.value, group)
            group.append(playurl)
        return group

    @classmethod
    def from_playurl_orig(cls, hls_format: AttrObj) -> list["Playurl"]:
        group = []
        baseurl = re.sub(r'(/\d+/live_\d+(?:_bs)?_\d+)_[\d\w]+/index', r'\1/index', hls_format.base_url.value)
        for url_info in hls_format.url_info:
            playurl = Playurl(baseurl, url_info.value, group)
            group.append(playurl)
        return group


def async_exception_report(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            pass
        except Exception:
            if isinstance(args[0], Logging):
                args[0].exception(f'fail to exec func {func}')
            else:
                logger.exception(f'fail to exec func {func}')
            raise
    return wrapper


def exception_report(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            if isinstance(args[0], Logging):
                args[0].exception(f'fail to exec func {func}')
            else:
                logger.exception(f'fail to exec func {func}')
            raise
    return wrapper


class HlsDownloader(Logging):
    def __init__(self, room_id: int, outdir: str = 'rec',
                 session: Optional[aiohttp.ClientSession] = None,
                 remuxer_class: Optional[RemuxerInterface] = FMp4Remuxer) -> None:
        self._session = session or get_downloader_session(config)
        self.room_id = room_id
        self.outdir = outdir
        self.remuxer_class = remuxer_class

        self.pools: dict[str, M3u8Pool] = {}
        self.remuxers: dict[str, RemuxerInterface] = {}
        self.segments = {}

        self.logger = _get_download_logger(
            f'HlsDownloader][{room_id}', logpath=os.path.join(outdir, f'sisrec-dl-{room_id}.log'))

    def __str__(self) -> str:
        return f'<Downloader {[str(pool) for pool in self.pools.values()]}>'

    def load_hls_format(self, hls_format: AttrObj):
        self.add_playurls(Playurl.from_playurl_orig(hls_format))
        self.add_playurls(Playurl.from_playurl(hls_format))
        self.debug(f'Current Downloader: {self}')

    def add_playurls(self, playurls: list[Playurl]):
        for playurl in playurls:
            if playurl.basedir not in self.pools:
                self.debug(f'creating m3u8 pool {playurl.basedir}')
                self.pools[playurl.basedir] = M3u8Pool(self)
            self.pools[playurl.basedir].add_url(playurl)
            self.adjust_enabled()

    def adjust_enabled(self):
        self.debug(f'adjusting enables in downloader {self.room_id}')
        if not self.pools:
            self.warn('no playlist pool in downloader {self}')
            return
        if len(self.pools) == 1:
            list(self.pools.values())[0].start()
        else:
            self.best_pool.start()
        for pool in self.pools.values():
            pool.adjust_enabled()
        self.debug(f'adjusted downloader: {self}')

    @property
    def best_pool(self) -> Optional["M3u8Pool"]:
        for pattern in [
            r'live_\d+(?:_bs)?_\d+$',
            r'live_\d+(?:_bs)?_\d+_[A-Za-z]+$',
            r'live_\d+(?:_bs)?_\d+_\d+$',
        ]:
            for key in self.pools:
                if re.match(pattern, key):
                    pool = self.valid_or_loading_pool(key)
                    if pool:
                        return pool
        for key in sorted(self.pools, key=lambda i: len(i)):
            pool = self.valid_or_loading_pool(key)
            if pool:
                return pool

    def valid_or_loading_pool(self, key):
        return self.pools[key] if self.pools[key].is_expiring is not True else None

    def close(self):
        for pool in self.pools.values():
            pool.close()
        for remuxer in self.remuxers.values():
            remuxer.close()

    async def join(self):
        await asyncio.gather(*[pool.join() for pool in self.pools.values()])
        self.info('all pools have ended')
        self.close()
        await asyncio.gather(*[remuxer.join() for remuxer in self.remuxers.values()])
        self.info('all remuxers have ended')

    @exception_report
    def get_remuxer(self, basedir: str):
        if not self.remuxer_class:
            return
        if basedir not in self.remuxers:
            self.info(f'creating remuxer {self.remuxer_class} for {basedir}')
            self.remuxers[basedir] = self.remuxer_class(self.outdir, basedir, logger=self.logger)
        return self.remuxers[basedir]

    @async_exception_report
    async def _on_success_seg(self, basedir: str, seg: Segment, seg_fn: str):
        remuxer = self.get_remuxer(basedir)
        self.debug(f'adding segment {seg.uri} {seg_fn} to remuxer {remuxer}')
        if remuxer:
            await remuxer.add_segment(seg, seg_fn)

    @async_exception_report
    async def _on_success_init_segs(self, basedir: str, segs: list[tuple[InitializationSection, str]]):
        remuxer = self.get_remuxer(basedir)
        self.debug(f'adding init segments to remuxer {remuxer}')
        if remuxer:
            await remuxer.add_init_segments(segs)

    @async_exception_report
    async def _download_seg(self, basedir: str,
                            seg: Union[Segment, InitializationSection], playurl: Playurl) -> Optional[bytes]:
        if playurl.is_valid is False:
            return
        try:
            if not playurl.require_seq_auth:
                url = f'{playurl.host}{seg.base_uri}{seg.uri}'
                self.debug(f'downloading {basedir}/{seg.uri} from {url}')
                async with self._session.get(url) as resp:
                    if resp.status == 403:
                        if playurl.query:
                            playurl.require_seq_auth = True
                            return await self._download_seg(basedir, seg, playurl)
                        else:
                            playurl.is_valid = False
                    elif resp.status == 200:
                        playurl.require_seq_auth = False
                        return await resp.read()
                    elif resp.status == 404:
                        self.debug(f'HTTP 404 for {resp.status} when downloading fragment from {url}')
                    else:
                        self.warn(f'Unexpected response {resp.status} when downloading fragment from {url}')
            else:
                url = f'{playurl.host}{seg.base_uri}{seg.uri}?{playurl.query}'
                self.debug(f'downloading {basedir}/{seg.uri} from {url}')
                async with self._session.get(url) as resp:
                    if resp.status == 403:
                        playurl.is_valid = False
                    elif resp.status == 200:
                        return await resp.read()
                    elif resp.status == 404:
                        self.debug(f'HTTP 404 for {resp.status} when downloading fragment from {url}')
                    else:
                        self.warn(f'Unexpected response {resp.status} when downloading fragment from {url}')
        except asyncio.CancelledError:
            pass
        except Exception:
            self.debug(f'Failed to download fragment from {url}', exc_info=True)

    @async_exception_report
    async def download_seg_and_dump(self, basedir: str, uri: str,
                                    seg_urls: list[tuple[Union[Segment, InitializationSection], Playurl]],
                                    crc32: Optional[str]):
        results = await asyncio.gather(*[
            self._download_seg(basedir, seg, playurl) for seg, playurl in seg_urls])
        for result, (seg, playurl) in zip(results, seg_urls):
            if result:
                if crc32 and crc32 != f'{zlib.crc32(result):X}'.lower():
                    self.warning(f'fragment {uri} crc32 checksum failed')
                    continue
                else:
                    out_fn = os.path.join(self.outdir, basedir, uri)
                    await dump_data(result, out_fn)
                    self.debug(f'fragment {uri} downloaded to {out_fn}')
                    self.segments[(basedir, uri)]['file'] = out_fn
                    if isinstance(seg, Segment):
                        asyncio.ensure_future(self._on_success_seg(basedir, seg, out_fn))
                    return out_fn

    @async_exception_report
    async def download_seg_worker(self, basedir, uri):
        segment_info = self.segments[(basedir, uri)]
        while segment_info['seg_urls'] and not segment_info.get('file'):
            seg_urls = list(segment_info['seg_urls'].values())
            segment_info['seg_urls'] = {}
            await self.download_seg_and_dump(basedir, uri, seg_urls, segment_info.get('crc32'))
        segment_info['dl_future'] = None

    @async_exception_report
    async def download_seg(self, basedir: str, seg: Union[Segment, InitializationSection],
                           crc32: Optional[str], playurl: Playurl):
        uri = seg.uri
        if (basedir, uri) not in self.segments:
            self.debug(f'Adding {basedir}/{uri} to tasks')
            self.segments[(basedir, uri)] = {
                'crc32': crc32,
                'seg_urls': {f'{seg.base_uri}/{seg.uri}?{playurl.full_url}': (seg, playurl)},
                'dl_future': asyncio.create_task(self.download_seg_worker(basedir, uri)),
            }
            await asyncio.sleep(0.5)
        if self.segments[(basedir, uri)].get('file'):
            return self.segments[(basedir, uri)]['file']
        if crc32 != self.segments[(basedir, uri)]['crc32']:
            self.error(f'conflicting crc32 for {basedir}/{uri}, {crc32} != {self.segments[(basedir, uri)]["crc32"]}')
            return
        self.segments[(basedir, uri)]['seg_urls'].update({
            f'{seg.base_uri}/{seg.uri}?{playurl.full_url}': (seg, playurl) for playurl in playurl.group
        })
        if not self.segments.get('dl_future'):
            self.segments['dl_future'] = asyncio.create_task(self.download_seg_worker(basedir, uri))

    @async_exception_report
    async def download_init(self, basedir: str, segs: list[InitializationSection], playurl: Playurl):
        dl_futures = []
        for seg in segs:
            await self.download_seg(basedir, seg, None, playurl)
            dl_futures.append(self.segments[(basedir, seg.uri)].get('dl_future'))
        dl_futures = [i for i in dl_futures if i]
        if dl_futures:
            await asyncio.gather(*dl_futures)
        seg_files = [self.segments[(basedir, seg.uri)].get('file') for seg in segs]
        if all(seg_files):
            await self._on_success_init_segs(basedir, list(zip(segs, seg_files)))


class M3u8Pool(Logging):
    def __init__(self, downloader: HlsDownloader, playurls: list[Playurl] = [], is_enabled=False):
        self.downloader = downloader
        self.outdir = downloader.outdir
        self._session = downloader._session
        self.room_id = self.downloader.room_id
        self.logger = _get_download_logger(
            f'M3u8Pool][{self.room_id}', logpath=os.path.join(self.outdir, f'sisrec-dl-{self.room_id}.log'))

        self.playlists: dict[str, M3u8Playlist] = {}
        self.basedir = None
        self.is_enabled = is_enabled
        self.add_urls(playurls)

    def __str__(self) -> str:
        return f'<Pool on={self.is_enabled} {list(self.playlists.values())}>'

    def add_urls(self, playurls: list[Playurl]):
        for playurl in playurls:
            self.add_url(playurl)

    def add_url(self, playurl: Playurl):
        self.basedir = playurl.basedir
        full_url = f'{playurl.host}{playurl.baseurl}{playurl.query}'
        if full_url not in self.playlists:
            self.playlists[full_url] = M3u8Playlist(self, playurl)

    def adjust_enabled(self):
        self.debug(f'adjusting enables in pool {self.basedir}')
        for full_url, playlist in list(self.playlists.items()):
            if playlist.is_valid is False:
                self.playlists.pop(full_url)
        to_enabled = self.target_num - len([p for p in self.playlists.values() if p.is_enabled])
        for full_url, playlist in self.playlists.items():
            if not playlist.is_enabled and to_enabled > 0:
                self.debug(f'enabling playlist {full_url}')
                playlist.start()
                to_enabled -= 1
        self.debug(f'adjusted pool: {self}')

    def start(self):
        if not self.is_enabled:
            self.info(f'starting recording for pool {self.basedir}')
            self.is_enabled = True
            self.adjust_enabled()

    @property
    def target_num(self):
        if self.is_enabled:
            return TARGET_POOLED_PLAYLIST
        return 0

    def close(self):
        for playlist in self.playlists.values():
            playlist.close()

    async def join(self):
        await asyncio.gather(*[playlist.join() for playlist in self.playlists.values()])
        self.info(f'all playlists have ended for pool {self.basedir}')

    def download_init(self, segs: list[Optional[InitializationSection]], playurl: Playurl):
        if self.is_enabled:
            asyncio.create_task(self.downloader.download_init(
                self.basedir, [seg for seg in segs if seg], playurl))

    def download_segs(self, segs: list[Segment], playurl: Playurl):
        if self.is_enabled:
            for seg in segs:
                if seg.title and '|' in seg.title:
                    crc32 = seg.title.split('|')[-1]
                else:
                    crc32 = None
                asyncio.create_task(self.downloader.download_seg(self.basedir, seg, crc32, playurl))

    @property
    def is_valid(self):
        is_valid = False
        for playlist in self.playlists.values():
            if playlist.is_valid:
                return True
            is_valid = None if playlist.is_valid is None else False
        return is_valid

    @property
    def is_expiring(self):
        is_expiring = True
        for playlist in self.playlists.values():
            if not playlist.is_expiring:
                return False
            is_expiring = None if playlist.is_expiring is None else True
        return is_expiring


class M3u8Playlist(Logging):
    def __init__(self, pool: M3u8Pool, playurl: Playurl):
        self.pool = pool
        self._session = pool._session
        self.outdir = pool.outdir
        self.room_id = self.pool.room_id
        self.logger = _get_download_logger(
            f'M3u8Playlist][{self.room_id}', logpath=os.path.join(self.outdir, f'sisrec-dl-{self.room_id}.log'))

        self.playurl = playurl
        self.url = playurl.host + playurl.baseurl
        self.expire_ts = None

        self._is_valid = None
        self.is_enabled = False
        self._network_future = asyncio.create_task(self._fetch_worker())

        self.last_sequence = -1
        self.last_update = 0
        self.strike = 0

    def __str__(self):
        return f'<Playlist on={self.is_enabled} "{self.brief_url}">'

    def __repr__(self) -> str:
        return str(self)

    @property
    def brief_url(self):
        return self.url.split("&")[0]

    @property
    def is_expiring(self):
        if self.is_valid is False:
            return True
        elif self.is_valid is None:
            return None
        if self.expire_ts and self.expire_ts - time.time() < EXPIRING_THRES:
            return True
        return False

    @property
    def is_valid(self):
        return self._is_valid

    @is_valid.setter
    def is_valid(self, value: bool):
        if self._is_valid is None:
            self.debug(f'playlist is loaded {self.brief_url}')
        self.debug(f'playlist valid status change: {self._is_valid}')
        self._is_valid = value
        self.pool.downloader.adjust_enabled()

    def start(self):
        self.is_enabled = True

    def close(self):
        self._network_future.cancel()

    async def join(self):
        await self._network_future

    async def _fetch_worker(self):
        try:
            self.debug(f'loading playlist {self.brief_url}')
            async with self._session.get(self.url) as resp:
                if resp.status >= 400:
                    self.url += self.playurl.query
                    self.expire_ts = self.playurl.expire_ts
                else:
                    await self.parse_rsp(await resp.text())
        except Exception:
            self._is_valid = False
            self.warning('playlist loading failed', exc_info=True)
        while self.is_valid is not False:
            if self.is_enabled or self.is_valid is None:
                try:
                    self.debug(f'reloading playlist {self.brief_url}')
                    async with self._session.get(self.url) as resp:
                        if resp.status >= 400:
                            self.debug(f'Failed to load playlist, status {resp.status}, {self.brief_url}')
                            self.is_valid = False
                            return
                        await self.parse_rsp(await resp.text())
                    self.strike = 0
                except asyncio.CancelledError:
                    break
                except (asyncio.TimeoutError, aiohttp.ClientOSError):
                    self.warning(f'playlist loading failed {self.brief_url}')
                    self.strike += 1
                    if self.strike > PLAYLIST_MAX_RETRIES:
                        self.is_valid = False
                    await asyncio.sleep(3)
                except Exception:
                    self.warning(f'playlist loading failed {self.brief_url}', exc_info=True)
                    self.strike += 1
                    if self.strike > PLAYLIST_MAX_RETRIES:
                        self.is_valid = False
                    await asyncio.sleep(3)
            else:
                await asyncio.sleep(2)

    async def parse_rsp(self, text):
        playlist = m3u8.loads(text, self.playurl.baseurl)
        if playlist.segments and playlist.segments[-1].media_sequence != self.last_sequence:
            self.last_sequence = playlist.segments[-1].media_sequence
            self.last_update = time.time()
        else:
            if time.time() - self.last_update > STALE_PLAYLIST_TIMEOUT:
                self.info(f'No new segments for {STALE_PLAYLIST_TIMEOUT}s from {self.brief_url}')
                self.is_valid = False
        self.pool.download_init(playlist.segment_map, self.playurl)
        self.pool.download_segs(playlist.segments, self.playurl)
        await asyncio.sleep(playlist.target_duration or DURATION_FALLBACK)
        if playlist.is_endlist:
            self.info(f'Got ENDLIST from {self.brief_url}')
            self.is_valid = False
