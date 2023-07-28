from typing import Callable, Any
import asyncio
import logging
from http.cookies import SimpleCookie
import traceback

import aiofile
import aiohttp
from aiohttp_socks import ProxyConnector

logger = logging.getLogger(__name__)
_sessions = {}


def get_live_api_session(config):
    if not _sessions.get('live_api'):
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
        _sessions['live_api'] = aiohttp.ClientSession(**aiohttp_config)
    return _sessions['live_api']


def get_danmaku_session(config):
    if not _sessions.get('chat_ws'):
        _sessions['chat_ws'] = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    return _sessions['chat_ws']


def parse_cookies_string(cookie_str: str, domain: str):
    cookies = SimpleCookie()
    cookies.load(cookie_str)
    for cookie in cookies.values():
        cookie['domain'] = cookie['domain'] or domain
    return cookies


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
            return _filter(self.__obj.items)
        elif isinstance(self.__obj, list):
            return _filter(enumerate(self.__obj))
        return AttrObj(None)

    def filter_one(self, func: Callable[[Any, Any], Any]):
        return self.filter(func)[0]

    def __getattr__(self, name):
        if isinstance(self.__obj, dict):
            return AttrObj(self.__obj.get(name))
        return AttrObj(None)

    def __getitem__(self, index):
        if isinstance(self.__obj, list):
            try:
                return AttrObj(self.__obj[index])
            except IndexError:
                return AttrObj(None)
        return AttrObj(None)

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
