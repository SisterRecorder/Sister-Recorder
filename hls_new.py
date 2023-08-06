import asyncio
from functools import cached_property, wraps
import os
import re
import time
import zlib
import logging
import inspect
import traceback
from typing import Optional, Union, TypeVar, TYPE_CHECKING

import m3u8
from m3u8.model import Segment, InitializationSection
import aiohttp
import aiofile

from config import config
from utils import re_search_group, get_dict_value, get_downloader_session, Logging
if TYPE_CHECKING:
    from rooms import Room


DOWNLOAD_RETRY_INTERVAL = 3
BASEDIR = 'rec'
CACHE_SEG_LIMIT = 10
CACHE_SEG_WAIT_LIMIT = 20
DUMMY_WRITE = False
DEBUG_ALWAYS_EXPIRE_TS = True

T = TypeVar('T')

_file_handlers: dict[str, logging.FileHandler] = {}
_file_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s', datefmt='%y-%m-%d %H:%M:%S')


def get_handler(path: Optional[str] = None):
    if path is None:
        handler = logging.StreamHandler()
        handler.formatter = _file_formatter
        handler.setLevel(logging.WARNING)
        return handler
    else:
        if path not in _file_handlers:
            _file_handlers[path] = logging.FileHandler(path, encoding='utf-8')
            _file_handlers[path].formatter = _file_formatter
            _file_handlers[path].setLevel(logging.DEBUG)
        return _file_handlers[path]


def get_logger(name: str, room_id: int):
    logger = logging.getLogger(f'{name}:{room_id}')
    if logger.propagate:
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        logger.addHandler(get_handler(os.path.join(BASEDIR, f'sisrec-{room_id}.log')))
        logger.addHandler(get_handler())
    return logger


def calc_crc32_digest(data: bytes):
    return f'{zlib.crc32(data):X}'.lower()


def equal_seg(seg: Segment, other: Segment) -> bool:
    if seg.uri != other.uri:
        return False
    if seg.title != other.title:
        return False
    if not equal_init_sec(seg.init_section, other.init_section):
        return False
    return True


def equal_init_sec(seg: Optional[Union[InitializationSection, Segment]],
                   other: Optional[Union[InitializationSection, Segment]]) -> bool:
    if isinstance(seg, Segment):
        return equal_init_sec(seg.init_section, other)
    if isinstance(other, Segment):
        return equal_init_sec(seg, other.init_section)
    if seg is None and other is None:
        return True
    elif seg is None or other is None:
        return False
    elif seg.uri != other.uri:
        return False
    return True


def log_exception_async(func: T) -> T:
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except Exception:
                self.exception(f'error from {func}')
        return async_wrapper
    else:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception:
                self.exception(f'error from {func}')
        return wrapper


class Playurl:
    def __init__(self, host: str, baseurl: str, query: str, group: list["Playurl"] = []):
        self.host = host
        self.baseurl = baseurl
        self.query = query
        self.group = group
        self.seg_add_query = None if query else False

    @cached_property
    def expire_ts(self):
        return re_search_group(r'expires=(\d+)', self.query, transform=int)

    @cached_property
    def full_url(self):
        return f'{self.host}{self.baseurl}{self.query}'

    @cached_property
    def short_url(self):
        return f'{self.host}{self.baseurl}'

    @cached_property
    def basedir(self):
        return re_search_group(r'/(live[^/]+)(:?/index)?\.m3u8', self.baseurl) or \
            os.path.basename(os.path.dirname(self.baseurl))

    @classmethod
    def from_format_data(cls, format_data: dict) -> list['Playurl']:
        group = []
        for host_info in format_data['url_info']:
            if 'mcdn.bilivideo.cn' in host_info['host']:
                continue
            group.append(Playurl(host_info['host'], format_data['base_url'], host_info['extra'], group))
            new_baseurl = re.sub(r'(/live_\d+(?:_bs)?_\d+)(?:_\w+)([/\.])', r'\1\2', format_data['base_url'])
            if new_baseurl != format_data['base_url']:
                group.append(Playurl(host_info['host'], new_baseurl, '', group))
        return group


class PlaylistPool(Logging):
    def __init__(self, room: 'Room', session: Optional[aiohttp.ClientSession] = None):
        self._session = session or get_downloader_session(config)
        self.room = room
        self.room_id = room.room_id
        self.logger = get_logger('Pool', self.room_id)

        self.playlists: list[Playlist] = []
        self.playlist_groups: list[PlaylistGroup] = []

    def close(self):
        for group in self.playlist_groups:
            group.close()
        for playlist in self.playlists:
            playlist.close()

    def check_group_enabled(self):
        def _group_score(group: Optional[PlaylistGroup]):
            def __get_score():
                if not group:
                    return -10000
                m = re.search(r'/live_\d+(_bs)?_\d+(_\w+)?/', group.basedir)
                if m:
                    bs, quality = m.groups()
                    if not quality:
                        return 10000 if not bs else 9000
                    elif quality == '_bluray':
                        return 1000 if not bs else 900
                    return 0
                else:
                    return -len(group.basedir)
            score = __get_score()
            self.debug(f'calc group score {score} for group {group}')
            return score

        if any([g.running and g.is_valid and not g.is_expiring for g in self.playlist_groups]):
            return
        best_group = None
        for group in self.playlist_groups:
            if group.basedir and group.is_valid and not group.is_expiring:
                if _group_score(group) > _group_score(best_group):
                    best_group = group
        if best_group:
            self.info(f'Starting Playlist group {best_group}')
            best_group.start()
        else:
            self.info('No available playlist group, requesting new playurl')
            self.room.check_live()

    @log_exception_async
    def add_playlist(self, playlist: 'Playlist'):
        self.debug(f'Adding playlist {playlist.url} to pool')
        self.playlists.append(playlist)
        playlist.pool = self
        for playlist_group in self.playlist_groups:
            if playlist_group.add_playlist(playlist):
                return
        self.debug('Creating new playlist group')
        playlist_group = PlaylistGroup(self._session, self)
        playlist_group.add_playlist(playlist)
        self.playlist_groups.append(playlist_group)

    @log_exception_async
    async def add_from_format(self, format_data: dict):
        playlists = [Playlist(self._session, playurl, self) for playurl in Playurl.from_format_data(format_data)]
        await asyncio.gather(*[playlist.load_result for playlist in playlists])
        for playlist in playlists:
            if playlist.is_valid:
                self.add_playlist(playlist)
            else:
                self.debug(f'invalid playlist {playlist.url}')
        self.check_playlists()

    @log_exception_async
    def check_playlists(self):
        def _filter(list: Union[list[Playlist], list[PlaylistGroup]]):
            new_list = []
            for i in list:
                if i.is_valid:
                    new_list.append(i)
                else:
                    i.close()
            return new_list
        for playlist in self.playlists:
            playlist.reload()
        self.playlists = _filter(self.playlists)
        for playlist_group in self.playlist_groups:
            playlist_group.check_playlists()
        self.playlist_group = _filter(self.playlist_groups)
        self.debug(f'checking playlists from pool: {[str(playlist) for playlist in self.playlists]}')
        self.check_group_enabled()

    def __str__(self):
        return f'<pool {self.room_id}>'


class HlsDownloader(Logging):
    def __init__(self, session: aiohttp.ClientSession, playlist_group: "PlaylistGroup"):
        self._session = session
        self.logger = get_logger('HlsDL', playlist_group.pool.room_id)
        self.playlist_group = playlist_group

        self.running = True
        self.write_queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self.out_prefix = None
        self.last_segment: Optional[Segment] = None

        self.segment_data: dict[int, tuple[bytes, Segment]] = {}
        self.init_sections: dict[str, Optional[bytes]] = {}
        self._download_futures = {}

        self.download_segs()
        self._dumper_future = asyncio.create_task(self.dump_worker())

    def close(self):
        self.running = False
        asyncio.ensure_future(self.check_for_write(clear_cache=True))

    @log_exception_async
    async def download_and_verify(self, seg: Union[Segment, InitializationSection],
                                  playurl: Playurl) -> Optional[bytes]:
        if playurl.seg_add_query:
            url = f'{playurl.host}{seg.base_uri}{seg.uri}?{playurl.query}'
        else:
            url = f'{playurl.host}{seg.base_uri}{seg.uri}'
        try:
            self.debug(f'Downloading segment {seg.uri} from {url}')
            async with self._session.get(url) as rsp:
                if rsp.status == 403 and playurl.seg_add_query is None:
                    playurl.seg_add_query = True
                    return await self.download_and_verify(seg, playurl)
                elif rsp.status == 200:
                    data = await rsp.read()
                else:
                    self.error(f'Got HTTP {rsp.status} when downloading segment {seg.uri} from {url}')
                    return
            if isinstance(seg, Segment):
                if seg.crc32 and seg.crc32 != calc_crc32_digest(data):
                    self.error(f'Crc32 checksum failed for segment {seg.uri} from {url}')
            return data
        except (asyncio.TimeoutError, aiohttp.ClientError):
            self.warning(f'Failed to donwload segment {seg.uri} from {url}: {traceback.format_exc(limit=0).strip()}')
        except Exception:
            self.exception(f'Failed to donwload segment {seg.uri} from {url}')

    @log_exception_async
    async def download_init_sec(self, seg: InitializationSection, playurl: Playurl, retries=10) -> Optional[bytes]:
        for _ in range(retries):
            data = await self.download_and_verify(seg, playurl)
            if data:
                return data
            await asyncio.sleep(DOWNLOAD_RETRY_INTERVAL)

    @log_exception_async
    async def get_init_sec(self, seg: Optional[InitializationSection], playurl: Playurl) -> Optional[bytes]:
        if not seg:
            return b''
        seg_key = f'{seg.base_uri}{seg.uri}'
        if not self.init_sections.get(seg_key):
            self.init_sections[seg_key] = await self.download_init_sec(seg, playurl)
        return self.init_sections[seg_key]

    @log_exception_async
    async def download_seg_and_init(self, seg: Segment, playurl: Playurl) -> Optional[bytes]:
        init_data, seg_data = await asyncio.gather(
            self.get_init_sec(seg.init_section, playurl),
            self.download_and_verify(seg, playurl),
        )
        if init_data is not None and seg_data is not None:
            return init_data + seg_data

    @log_exception_async
    async def download_with_retry(self, media_seq: int, retries=2):
        _states = {'success': False, 'start_ts': time.time()}

        async def _download_and_save(seg: Segment, playurl: Playurl) -> bool:
            _states['start_ts'] = time.time()
            data = await self.download_seg_and_init(seg, playurl)
            if data and not _states['success']:
                _states['success'] = True
                self.debug(f'Semgent {media_seq} downloaded')
                self.segment_data[media_seq] = (data, seg)
                return True
            return False

        seg, playurl = get_dict_value(self.playlist_group.segments[media_seq])
        success = await _download_and_save(seg, playurl)
        if success:
            await self.check_for_write()
        else:
            for _ in range(retries):
                to_sleep = DOWNLOAD_RETRY_INTERVAL - (time.time() - _states['start_ts'])
                if to_sleep > 0:
                    self.debug(f'Retry downloading segment {seg.uri} in {to_sleep:.1f}s')
                    await asyncio.sleep(to_sleep)
                else:
                    self.debug(f'Retry downloading segment {seg.uri}')
                seg_playurl = list({
                    (seg.uri, playurl.full_url): (seg, playurl)
                    for seg, p in self.playlist_group.segments[media_seq].values()
                    for playurl in p.group
                }.values())
                results = await asyncio.gather(*[_download_and_save(seg, playurl) for seg, playurl in seg_playurl])
                for success in results:
                    if success:
                        await self.check_for_write()
                        return
            self.error(f'Failed to download segment {media_seq}')

    def download_segs(self):
        for media_seq in self.playlist_group.segments:
            if media_seq not in self._download_futures:
                self._download_futures[media_seq] = asyncio.create_task(
                    self.download_with_retry(media_seq))

    def pop_segment(self, media_seq: int):
        self.playlist_group.min_seq = media_seq
        self.playlist_group.segments.pop(media_seq, None)
        return self.segment_data.pop(media_seq)

    @log_exception_async
    async def check_for_write(self, clear_cache=False):
        to_write = []

        async def _put_data():
            if to_write:
                await self.write_queue.put(b''.join(to_write))
            to_write.clear()
        if len(self.segment_data) > CACHE_SEG_LIMIT or clear_cache:
            for i, media_seq in enumerate(sorted(self.segment_data)):
                last_sequence = getattr(self.last_segment, 'media_sequence', None) or media_seq
                if last_sequence > media_seq:
                    self.warning(f'ignore previous segs {media_seq} < {last_sequence}')
                    self.pop_segment(media_seq)
                    continue
                if self.last_segment and self.last_segment.media_sequence != media_seq - 1:
                    if not clear_cache:
                        if len(self.segment_data) < CACHE_SEG_WAIT_LIMIT or i > 0:
                            break
                    self.warning(f'Missing segments ({self.last_segment.media_sequence}, {media_seq - 1}]')
                data, seg = self.pop_segment(media_seq)
                self.debug(f'To write segment {seg.uri}')
                to_write.append(data)
                if seg.discontinuity:
                    self.warning(f'Got discontinuity at segment {seg.uri}')
                    await _put_data()
                    await self.write_queue.put(None)
                if self.last_segment and not equal_init_sec(self.last_segment, seg):
                    self.warning(f'Init segment changed at segment {seg.uri}')
                    await _put_data()
                    await self.write_queue.put(None)
                self.last_segment = seg
            await _put_data()
        if not self.running and clear_cache:
            await self.write_queue.put(None)

    @log_exception_async
    async def dump_worker(self):
        while self.running:
            out_fn = os.path.join(
                BASEDIR,
                f'{self.out_prefix or self.playlist_group.pool.room_id}'
                f'-{time.strftime("%y%m%d-%H%M%S")}'
                f'-{self.playlist_group.basedir}.fmp4'
            )
            self.info(f'begin writing to new file {out_fn}')
            os.makedirs(os.path.dirname(out_fn), exist_ok=True)
            async with aiofile.async_open(out_fn, 'wb') as f:
                while True:
                    data = await self.write_queue.get()
                    if data:
                        n_bytes = await f.write(data) if not DUMMY_WRITE else len(data)
                        self.debug(f'Wrote {n_bytes} bytes to file {out_fn}')
                    else:
                        break
            self.info(f'closing file {out_fn}')
        self.info('writing ended')


class PlaylistGroup(Logging):
    def __init__(self, session: aiohttp.ClientSession, pool: PlaylistPool):
        self._session = session
        self.pool = pool
        self.logger = get_logger('Group', pool.room_id)

        self.playlists: dict[str, "Playlist"] = {}
        self.running = False

        self.segments: dict[int, dict[tuple, tuple[Segment, Playurl]]] = {}
        self.min_seq: Optional[int] = None
        self.last_update = 0
        self.downloader: Optional[HlsDownloader] = None

    def start(self):
        self.running = True
        self.downloader = HlsDownloader(self._session, self)
        for playlist in self.playlists.values():
            playlist.enabled = True

    def close(self):
        if self.downloader:
            self.downloader.close()
        for playlist in self.playlists.values():
            playlist.close()

    @property
    def basedir(self):
        if self.playlists:
            return get_dict_value(self.playlists).playurl.basedir

    @property
    def is_valid(self):
        return any([playlist.is_valid for playlist in self.playlists.values()])

    @property
    def expire_ts(self):
        expire_ts = 0
        for playlist in self.playlists.values():
            if playlist.expire_ts is None:
                return None
            else:
                expire_ts = max(playlist.expire_ts, expire_ts)
        return expire_ts

    @property
    def is_expiring(self):
        if self.expire_ts:
            return self.expire_ts - time.time() < 120

    def check_playlists(self):
        for key, playlist in list(self.playlists.items()):
            if not playlist.is_valid:
                self.playlists.pop(key)

    @log_exception_async
    def add_playlist(self, playlist: "Playlist"):
        if not self.playlists:
            self.load_segments(playlist)
            playlist.group = self
            playlist.enabled = self.running
            self.playlists[playlist.playurl.host] = playlist
            self.info(f'Adding playlist {playlist.url} to group {self.basedir}')
            return True
        elif self.check_compatible(playlist):
            playlist.group = self
            playlist.enabled = self.running
            if playlist.playurl.host in self.playlists:
                self.info(f'Closing dup playlist {playlist.url} from group {self.basedir}')
                self.playlists[playlist.playurl.host].close()
            self.playlists[playlist.playurl.host] = playlist
            self.info(f'Adding playlist {playlist.url} to group {self.basedir}')
            return True
        return False

    def check_compatible(self, playlist: "Playlist"):
        if not set(playlist.segments) & set(self.segments):
            return False
        basedir, host = playlist.playurl.basedir, playlist.playurl.host
        for media_seq, seg in playlist.segments.items():
            if media_seq in self.segments:
                if not seg.crc32 and (basedir != self.basedir or host not in self.playlists):
                    return False
                if not equal_seg(seg, get_dict_value(self.segments[media_seq])[0]):
                    return False
        return True

    @log_exception_async
    def load_segments(self, playlist: "Playlist"):
        segments, playurl = playlist.segments, playlist.playurl
        for media_seq, seg in segments.items():
            if media_seq not in self.segments:
                if media_seq > (self.min_seq or media_seq - 1):
                    self.last_update = time.time()
                    self.segments[media_seq] = {(seg.uri, playurl.full_url): (seg, playurl)}
            else:
                if not equal_seg(seg, get_dict_value(self.segments[media_seq])[0]):
                    self.error(f'Got conflict segment {seg.uri} from playlist {playlist.url}')
                    playlist.is_valid = False
                    return
                else:
                    self.segments[media_seq][(seg.uri, playurl.full_url)] = (seg, playurl)
        if self.downloader:
            self.downloader.download_segs()
        if not self.running:
            for media_seq in set(self.segments) - set(segments):
                self.segments.pop(media_seq)

    def __str__(self):
        return f'<group {self.basedir}>'


class Playlist(Logging):
    def __init__(self, session: aiohttp.ClientSession, playurl: Playurl, pool: PlaylistPool):
        self._session = session
        self.playurl = playurl
        self.logger = get_logger('Playlist', pool.room_id)
        self.pool = pool
        self.group: Optional[PlaylistGroup] = None

        self.url = self.playurl.short_url
        self._is_valid = None
        self.expire_ts = None if not DEBUG_ALWAYS_EXPIRE_TS else playurl.expire_ts
        self.load_result = asyncio.Future()
        self._reload_future = asyncio.create_task(self.reload_worker())
        self._sleep_future = None

        self.enabled = False
        self._need_reload = False
        self.sleep_multiplier = 1
        self.is_endlist = False

        self._segments = {}

    def close(self):
        self.info(f'Closing playlist {self.url}')
        self._is_valid = False
        self._reload_future.cancel()

    @property
    def is_valid(self):
        return self._is_valid

    @is_valid.setter
    def is_valid(self, value: bool):
        self.debug(f'playlist valid status changed to {value}')
        self._is_valid = value
        if self.pool and not value:
            self.pool.check_playlists()

    @property
    def segments(self):
        return self._segments

    @segments.setter
    def segments(self, value):
        self._segments = value
        if self.group:
            self.group.load_segments(self)

    def reload(self):
        self._need_reload = True
        asyncio.ensure_future(self.interrupt_sleep())

    @log_exception_async
    async def interrupt_sleep(self, delay=0):
        if delay:
            await asyncio.sleep(delay)
        try:
            self._sleep_future.set_result(None)
        except Exception:
            pass

    @log_exception_async
    async def schedule_sleep(self, sleep):
        await asyncio.sleep(sleep)
        if self.sleep_multiplier > 1:
            self._sleep_future = asyncio.Future()
            asyncio.ensure_future(self.interrupt_sleep((self.sleep_multiplier - 1) * sleep))
            await self._sleep_future

    @log_exception_async
    async def reload_worker(self):
        sleep = await self._load()
        while self.is_valid and sleep:
            await self.schedule_sleep(sleep)
            if self.enabled or self._need_reload:
                sleep = await self._reload()
                self._need_reload = False

    async def _reload(self, retries=3):
        try:
            self.debug(f'reloading playlist {self.url}')
            async with self._session.get(self.url) as rsp:
                if rsp.status == 200:
                    return await self.parse_m3u8(await rsp.text(encoding='utf-8'))
                elif rsp.status >= 400:
                    self.info(f'Got status {rsp.status} when reloading playlist from {self.url}')
                    self.is_valid = False
                else:
                    raise ValueError(f'Unexpected status {rsp.status} when reloading playlist')
        except asyncio.CancelledError:
            self.is_valid = False
        except Exception:
            if retries > 0:
                return await self._reload(retries=retries-1)
            else:
                self.exception(f'Failed to reload playlist from {self.url}')
                self.is_valid = False

    async def _load(self, retry_403=True, retry_exception=True):
        try:
            self.debug(f'loading playlist {self.url}')
            async with self._session.get(self.url) as rsp:
                if rsp.status == 200:
                    self.is_valid = True
                    self.load_result.set_result(True)
                    return await self.parse_m3u8(await rsp.text(encoding='utf-8'))
                elif rsp.status == 403 and retry_403 and self.playurl.query:
                    self.url = self.playurl.full_url
                    self.expire_ts = self.playurl.expire_ts
                    return await self._load(retry_403=False)
                else:
                    self.is_valid = False
                    self.load_result.set_result(False)
                    self.warning(f'Failed to load playlist with status {rsp.status} from {self.url}')
        except Exception:
            if retry_exception:
                return await self._load(retry_exception=False, retry_403=retry_403)
            else:
                self.load_result.set_result(False)

    @log_exception_async
    async def parse_m3u8(self, text) -> float:
        self.debug('parsing m3u8 playlist')
        playlist = m3u8.loads(text, self.playurl.baseurl)
        if playlist.is_endlist:
            self.info(f'Got endlist from {self.url}')
            self.is_valid = False
            self.is_endlist = True
        new_segments = {}
        for seg in playlist.segments:
            seg.crc32 = (seg.title or '').split('|')[-1]
            if not equal_seg(self.segments.get(seg.media_sequence, seg), seg):
                self.error(f'Conflict segment "{self.segments.get(seg.media_sequence)}" != "{seg}"')
                self.is_valid = False
                return
            new_segments[seg.media_sequence] = seg
        self.segments = new_segments
        return playlist.target_duration

    def __str__(self):
        return f'<Playlist url="{self.url}" valid={self.is_valid} enabled={self.enabled} group={self.group} pool={self.pool} >'


from utils import AttrObj

async def test2():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
        async def _get_fmp4():
            async with session.get('http://biliapi.imfurry.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=440&protocol=0,1&format=0,1,2&codec=0,1&qn=10000&platform=web&dolby=5&panorama=1') as rsp:
                data = await rsp.json()
            return AttrObj(data).data.playurl_info.playurl.stream.filter_one(lambda _, v: v.protocol_name == 'http_hls').format.filter_one(lambda _, v: v.format_name == 'fmp4').codec[0].value
        fmp4_data = await _get_fmp4()
        print(fmp4_data)
        await asyncio.sleep(3)
        pool = PlaylistPool(session, 395113)
        await pool.add_from_format(fmp4_data)
        await asyncio.sleep(60)
        await pool.add_from_format(await _get_fmp4())
        await asyncio.sleep(60)
        pool.close()
        await asyncio.sleep(3)


if __name__ == '__main__':
    asyncio.run(test2())
