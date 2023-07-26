import re
import os
import json
import time
import asyncio
import traceback

from aiofile import async_open
import aiohttp
from aiohttp_socks import ProxyConnector

from blivedm.blivedm import BLiveClient, HandlerInterface
from utils import AttrObj


class DumpHandler(HandlerInterface):
    def __init__(self, room_id, flush_len=10) -> None:
        super().__init__()
        self.room_id = room_id
        self.outfile = f'{room_id}.jsonl'
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

    async def handle(self, client, command: dict):
        cmd = command.get('cmd', '').split(':')[0]
        if cmd == 'LIVE':
            self.room.check_live()


class Room:
    def __init__(self, room_id):
        self.room_id = int(room_id)

        self.dm_client = BLiveClient(room_id)
        self.dump_handler = DumpHandler(room_id)
        self.dm_client.add_handler(self.dump_handler)
        self.dm_client.add_handler(LiveStartHandler(self))

        aiohttp_config = {
            'timeout': aiohttp.ClientTimeout(total=10),
        }
        proxy_url = os.environ.get('SISREC_PROXY_URL')
        if proxy_url:
            aiohttp_config['connector'] = ProxyConnector.from_url(proxy_url)
        self._session = aiohttp.ClientSession(**aiohttp_config)

        self.playurl_retry_interval = 5
        self.playurl_sleep_interval = 300
        self._sleep_future = None
        self._playurl_futhre = None

        self.only_if_no_flv = False
        self.only_fmp4 = False

    @classmethod
    def from_url(cls, url: str):
        room_id = re.search(r'(?:^|https?://live\.bilibili\.com/(?:(?:blanc|h5)/)?)(\d+)', url)[1]
        return cls(room_id)

    def start(self):
        self.dm_client.start()
        self._playurl_future = asyncio.create_task(self._get_playurl_worker())

    async def stop(self):
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

    def extract_url(self, hls_format):
        baseurl = hls_format.base_url.value
        host = hls_format.url_info.filter_one(lambda _, v: 'gotcha' not in v.host.value)
        if host:
            print('try to modify url')
            baseurl = re.sub(r'(/\d+/live_\d+(?:_bs)?_\d+)_[\d\w]+/index', r'\1/index', baseurl)
            return host.host.value + baseurl
        else:
            print('use original url')
            host = hls_format.url_info._first
            return f'{host.host.value}{baseurl}{host.extra.value}'

    async def record(self, playurl_info: AttrObj):
        streams = playurl_info.playurl.stream
        if self.only_if_no_flv and streams.filter_one(lambda _, v: v.protocol_name == 'http_stream'):
            print('skip record because flv is found')
            await self.sleep()
            return
        hls_formats = streams.filter_one(lambda _, v: v.protocol_name == 'http_hls')
        if not hls_formats:
            print('no hls formats found')
            await asyncio.sleep(self.playurl_retry_interval)
            return
        fmp4_format = hls_formats.format.filter_one(lambda _, v: v.format_name == 'fmp4').codec._first
        if self.only_fmp4 and not fmp4_format:
            print('no fMp4 formats found')
            await asyncio.sleep(self.playurl_retry_interval)
            return
        hls_format = fmp4_format or hls_formats.format._first.codec._first
        print('will use format', hls_format)
        m3u8_url = self.extract_url(hls_format)
        print('will use url', m3u8_url)

        outname = f'rec/{self.room_id}-{round(time.time() * 1000)}'
        print(f'starting rec to {outname}')
        proc = await asyncio.create_subprocess_exec(
            'streamlink', m3u8_url, 'best',
            '--loglevel', 'debug',
            '--logfile', f'{outname}.log',
            '--stream-segment-threads', '10',
            '-o', f'{outname}.ts',
        )
        await proc.wait()

    async def sleep(self):
        self._sleep_future = asyncio.create_task(asyncio.sleep(300))
        print('sleep before getting playurl')
        try:
            await self._sleep_future
        except asyncio.CancelledError:
            print('sleep interrupted')
            pass
        finally:
            self._sleep_future = None

    async def _get_playurl_worker(self):
        while True:
            try:
                print('checking playurl')
                async with self._session.get(
                    'https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo',
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
                    print('got playurl, starting record')
                    await self.record(rsp.data.playurl_info)
                elif rsp.data.live_status.value == 1:
                    await asyncio.sleep(self.playurl_retry_interval)
                else:
                    await self.sleep()

            except Exception:
                traceback.print_exc()
                await asyncio.sleep(30)
