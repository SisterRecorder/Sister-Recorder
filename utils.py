from typing import Callable, Any, TypeVar
import re
import os
import asyncio
import logging
from http.cookies import SimpleCookie
import traceback
import linecache
import tracemalloc
import gc

import aiofile
import aiohttp
from aiohttp_socks import ProxyConnector
import httpx


logger = logging.getLogger(__name__)
_sessions: dict[str, aiohttp.ClientSession] = {}
_httpx_sessions: dict[str, httpx.AsyncClient] = {}


T = TypeVar('T')

HLS_CLIENT_TIMEOUT = 5


def snapshot_ram_top(logger=logger, key_type='lineno', limit=10):
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics(key_type)

    lines = []
    lines.append("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        lines.append("#%s: %s:%s: %.1f KiB"
              % (index, frame.filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            lines.append('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        lines.append("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    lines.append("Total allocated size: %.1f KiB" % (total / 1024))
    logger.debug('Malloc top\n%s\n' % "\n".join(lines))
    gc.collect()


def get_live_api_session(config, name='live_api'):
    if not _sessions.get(name):
        aiohttp_config = {
            'timeout': aiohttp.ClientTimeout(total=10),
        }
        if config.api_proxy:
            try:
                aiohttp_config['connector'] = ProxyConnector.from_url(config.api_proxy)
            except ValueError:
                logger.error('invalid proxy url, ignore api_proxy setting')
        if config.live_api_cookie_string:
            aiohttp_config['cookie_jar'] = aiohttp.CookieJar()
            aiohttp_config['cookie_jar'].update_cookies(parse_cookies_string(
                config.live_api_cookie_string, '.bilibili.com'))
        _sessions[name] = aiohttp.ClientSession(**aiohttp_config)
    return _sessions[name]


def get_danmaku_session(config):
    if not _sessions.get('chat_ws'):
        _sessions['chat_ws'] = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    return _sessions['chat_ws']


def get_downloader_session(config, name='hls-dl'):
    if config.http2_download:
        if not _httpx_sessions.get(name):
            _httpx_sessions[name] = httpx.AsyncClient(timeout=HLS_CLIENT_TIMEOUT, http2=True)
        return _httpx_sessions[name]
    else:
        if not _sessions.get(name):
            _sessions[name] = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HLS_CLIENT_TIMEOUT))
        return _sessions[name]


async def close_sessions():
    await asyncio.gather(
        *[session.close() for session in _sessions.values()],
        *[session.aclose() for session in _httpx_sessions.values()],
    )


def parse_cookies_string(cookie_str: str, domain: str):
    cookies = SimpleCookie()
    cookies.load(cookie_str)
    for cookie in cookies.values():
        cookie['domain'] = cookie['domain'] or domain
    return cookies


def get_searcher(pattern, flags=0, group=1, transform=None):
    regex = re.compile(pattern, flags=flags)

    def searcher(s: str, *pos, match_group=None):
        m = regex.search(s, *pos)
        if m:
            value = m.group(match_group or group)
            if transform:
                return transform(value)
            else:
                return value
    return searcher


def re_search_group(pattern, string, group=1, transform=None, **kwargs):
    m = re.search(pattern, string)
    if m:
        value = m.group(group)
        if transform:
            return transform(value)
        else:
            return value


async def dump_data(data, fn: str, mode='wb') -> int:
    try:
        async with aiofile.async_open(fn, mode) as afp:
            return await afp.write(data)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        return await dump_data(data, fn, mode)


async def dump_stdout(proc: asyncio.subprocess.Process, fn: str, mode='ab', timeout=300, cache_line=10):
    if proc.stdout is None:
        return
    try:
        cache = []
        async with aiofile.async_open(fn, mode) as afp:
            while proc.returncode is None:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout)
                cache.append(line)
                if len(cache) > cache_line:
                    await afp.write(b''.join(cache))
            await afp.write(b''.join(cache))
            await afp.write(await asyncio.wait_for(proc.stdout.read(), timeout))
    except asyncio.TimeoutError:
        pass
    except Exception:
        traceback.print_exc()


def get_dict_value(d: dict[Any, T], index=0) -> T:
    return list(d.values())[index]


class AttrObj:
    def __init__(self, obj):
        self.__obj = obj

    @property
    def _first(self):
        return self[0]

    def filter(self, func: Callable[[Any, Any], Any]):
        def _filter(iterator):
            return AttrObj([v for k, v in iterator if func(k, AttrObj(v))])
        if isinstance(self.__obj, dict):
            return _filter(self.__obj.items())
        elif isinstance(self.__obj, list):
            return _filter(enumerate(self.__obj))
        return AttrObj(None)

    def filter_one(self, func: Callable[[Any, Any], Any]):
        return self.filter(func)[0]

    def __getattr__(self, name):
        if isinstance(self.__obj, dict):
            value = self.__obj.get(name)
        else:
            value = None
        return AttrObj(value)

    def __getitem__(self, index):
        if isinstance(self.__obj, list):
            try:
                return AttrObj(self.__obj[index])
            except IndexError:
                return AttrObj(None)
        return AttrObj(None)

    def __iter__(self):
        if isinstance(self.__obj, list):
            def _iter():
                for obj in self.__obj:
                    yield AttrObj(obj)
            return _iter()
        elif isinstance(self.__obj, dict):
            return iter(self.__obj)

    def __str__(self):
        return str(self.__obj)

    def __repr__(self):
        return f'<AttrObj {self.__obj}>'

    def __bool__(self):
        return bool(self.__obj)

    def __eq__(self, other):
        return self.__obj == other

    @property
    def value(self):
        return self.__obj


class Logging:
    def log(self, level, msg, *args, **kwargs):
        if isinstance(getattr(self, 'logger', None), logging.Logger):
            self.logger.log(level, str(msg), *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        self.error(msg, *args, exc_info=exc_info, **kwargs)
