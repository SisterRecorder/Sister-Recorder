import re
import os
import json
import time
import asyncio
import traceback
import logging
import urllib.parse

from aiofile import async_open

from blivedm.blivedm import BLiveClient, HandlerInterface
from utils import get_live_api_session, get_danmaku_session, dump_stdout, AttrObj, Logging
from config import config


class DumpHandler(HandlerInterface):
    def __init__(self, room_id, flush_len=30) -> None:
        super().__init__()
        self.room_id = room_id
        self.outfile = f'chat/{room_id}.jsonl'
        os.makedirs(os.path.dirname(self.outfile), exist_ok=True)
        self.flush_len = flush_len
        self._write_queue = asyncio.Queue()
        self._dump_list = []
        self.running = True
        self._writer_coroutine = asyncio.create_task(self._writer_worker())

    async def stop(self):
        self.running = False
        await self.flush_cache()
        await self._writer_coroutine

    async def flush_cache(self):
        to_write = '\n'.join(self._dump_list)
        self._dump_list.clear()
        await self._write_queue.put(to_write + '\n')

    async def _writer_worker(self):
        while self.running:
            try:
                to_write = await self._write_queue.get()
                async with async_open(self.outfile, 'at', encoding='utf-8') as afp:
                    await afp.write(to_write)
            except Exception:
                traceback.print_exc()

    async def handle(self, client, command: dict):
        cmd = command.get('cmd', '').split(':')[0]
        self._dump_list.append(json.dumps([cmd, time.time(), command]))
        if len(self._dump_list) > self.flush_len:
            await self.flush_cache()


class LiveStartHandler(HandlerInterface):
    def __init__(self, room):
        self.room = room
        self.logger = logging.getLogger(f'{__name__}.LiveStartHandler')

    async def handle(self, client, command: dict):
        cmd = command.get('cmd', '').split(':')[0]
        if cmd == 'LIVE':
            self.info(f'live start detected for room {client.room_id}')
            self.room.check_live()
        elif cmd == 'ROOM_CHANGE':
            # 分区(或标题)改变
            self.info(f'title or area change detected for room {client.room_id}')
            self.room.check_live()


class Room(Logging):
    def __init__(self, room_id):
        self.logger = logging.getLogger(f'{__name__}.Room][{room_id}')
        self.room_id = int(room_id)

        self.dm_client = BLiveClient(room_id, session=get_danmaku_session(config))
        self.dump_handler = DumpHandler(room_id)
        self.dm_client.add_handler(self.dump_handler)
        self.dm_client.add_handler(LiveStartHandler(self))

        self._session = get_live_api_session(config)

        self.playurl_retry_interval = 5
        self.sleep_interval = 300
        self._sleep_future = None
        self._playurl_futhre = None

    @classmethod
    def get_room_id(cls, url: str):
        match = re.search(r'(?:^|https?://live\.bilibili\.com/(?:(?:blanc|h5)/)?)(\d+)', url)
        if match:
            return match[1]

    @classmethod
    def from_url(cls, url: str):
        room_id = cls.get_room_id(url)
        if room_id:
            return cls(room_id)

    def start(self):
        self.debug('staring room')
        self.dm_client.start()
        self._playurl_future = asyncio.create_task(self._get_playurl_worker())

    async def stop(self):
        self.debug('stopping room')
        self.dm_client.stop()
        self._playurl_future.cancel()
        asyncio.ensure_future(self.dump_handler.stop())
        await self.dm_client.join()
        await self.dm_client.stop_and_close()

    def check_live(self):
        try:
            self._sleep_future.cancel()
        except AttributeError:
            pass
        except Exception:
            pass

    async def extract_url(self, hls_format):
        baseurl = hls_format.base_url.value
        host = hls_format.url_info.filter_one(lambda _, v: 'gotcha' not in v.host.value)
        if host:
            self.debug('try to modify m3u8 url')
            baseurl = re.sub(r'(/\d+/live_\d+(?:_bs)?_\d+)_[\d\w]+/index', r'\1/index', baseurl)
            new_url = host.host.value + baseurl
            async with self._session.get(new_url) as rsp:
                if rsp.status == 200:
                    return new_url
                else:
                    self.debug(f'modified m3u8 url failed, got HTTP {rsp.status}')
        self.debug('use original m3u8 url')
        host = hls_format.url_info._first
        return f'{host.host.value}{baseurl}{host.extra.value}'

    async def record(self, playurl_info: AttrObj):
        streams = playurl_info.playurl.stream
        if config.only_if_no_flv and streams.filter_one(lambda _, v: v.protocol_name == 'http_stream'):
            self.info('skip record because flv is found')
            await self.sleep()
            return
        hls_formats = streams.filter_one(lambda _, v: v.protocol_name == 'http_hls')
        if not hls_formats:
            self.warn('no hls formats found')
            await asyncio.sleep(self.playurl_retry_interval)
            return
        fmp4_format = hls_formats.format.filter_one(lambda _, v: v.format_name == 'fmp4').codec._first
        if config.only_fmp4 and not fmp4_format:
            self.warn('no fMp4 formats found')
            await asyncio.sleep(self.playurl_retry_interval)
            return
        hls_format = fmp4_format or hls_formats.format._first.codec._first
        self.debug(f'will use format: {hls_format}')
        m3u8_url = await self.extract_url(hls_format)
        self.debug(f'will use url {m3u8_url}')

        outname = f'rec/{self.room_id}-{round(time.time() * 1000)}'
        os.makedirs('rec', exist_ok=True)
        self.info(f'starting record to {outname} using {config.record_backend}')
        if config.record_backend == 'streamlink':
            proc = await asyncio.create_subprocess_exec(
                'streamlink', m3u8_url, 'best',
                '--loglevel', 'trace',
                '--logfile', f'{outname}.log',
                '--stream-segment-threads', '10',
                '--hls-live-restart',
                '-o', f'{outname}.ts',
                stderr=asyncio.subprocess.DEVNULL,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                'ffmpeg', '-hide_banner', '-nostats',
                '-loglevel', config.ffmpeg_loglevel,
                '-i', m3u8_url,
                '-c', 'copy',
                f'{outname}.{config.output_ext}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            asyncio.ensure_future(dump_stdout(proc, f'{outname}.log'))
        await proc.wait()
        self.info(f'record ended with exitcode {proc.returncode}')

    async def sleep(self):
        self._sleep_future = asyncio.create_task(asyncio.sleep(self.sleep_interval))
        self.debug(f'sleep for {self.sleep_interval}s before getting playurl')
        try:
            await self._sleep_future
        except asyncio.CancelledError:
            self.debug('sleep interrupted')
        finally:
            self._sleep_future = None

    async def _get_playurl_worker(self):
        while True:
            try:
                self.info('checking playurl')
                async with self._session.get(
                    urllib.parse.urljoin(config.live_api_host, '/xlive/web-room/v2/index/getRoomPlayInfo'),
                    params={
                        'room_id': self.room_id,
                        'protocol': '0,1',
                        'format': '0,1,2',
                        'codec': '0,1',
                        'qn': 10000,
                        'platform': 'web',
                        'dolby': 5,
                        'panorama': 1,
                    }
                ) as res:
                    rsp = AttrObj(await res.json())

                if rsp.data.playurl_info:
                    self.debug('got playurl, starting record')
                    await self.record(rsp.data.playurl_info)
                elif rsp.data.live_status.value == 1:
                    await asyncio.sleep(self.playurl_retry_interval)
                else:
                    await self.sleep()
            except Exception:
                self.exception('exception in playurl loop')
                await asyncio.sleep(30)
