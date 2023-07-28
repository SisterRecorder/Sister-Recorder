import configparser
import os
import re
import time
import logging
from datetime import date

_log_queue = []
logger = None


class Config:
    def __init__(self):
        self.config_list = [
            'config.ini',
            'config.example.ini',
        ]
        self._last_load = 0

    def log(self, *args, **kwargs):
        if logger:
            logger.log(*args, **kwargs)
        else:
            _log_queue.append((args, kwargs))

    def load(self):
        parser = configparser.ConfigParser()
        for fn in self.config_list:
            if os.path.exists(fn):
                if os.stat(fn).st_mtime < self._last_load:
                    return
                parser.read(fn, encoding='utf-8')
                self._last_load = time.time()
                break
        self._load(parser)

    def _load(self, parser):
        def _load(option, transform=lambda i: i, getter='get', validator=lambda i: True):
            value = getattr(parser, getter)(section, option, fallback=None)
            if transform and value:
                value = transform(value)
            if value and not validator(value):
                self.log(logging.WARNING, f'invalid value for {section}.{option}: {value}')
            else:
                return value

        def _load_choices(option, choices, **kwargs):
            value = _load(option, **kwargs)
            if value in choices:
                return value
            else:
                if value:
                    self.log(logging.WARNING, f'invalid value for {section}.{option}: {value}')
                return choices[0]

        section = 'global'
        self.loglevel = _load_choices(
            'loglevel', ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], transform=lambda i: i.upper())
        self.logfile = _load('logfile') or f'log/sisrec-{date.today().strftime("%y%m%d")}-.log'
        self.record_backend = _load_choices('record_backend', ['streamlink', 'ffmpeg'])
        self.output_ext = _load_choices('output_ext', ['ts', 'mp4'])
        self.ffmpeg_loglevel = _load_choices('ffmpeg_loglevel', [
            'warning', 'quiet', 'panic', 'fatal', 'error', 'info', 'verbose', 'debug', 'trace'])
        self.api_proxy = _load('api_proxy', validator=lambda i: re.search(r'^(https?|socks\d?)://', i))
        self.live_api_host = _load('live_api_host', validator=lambda i: re.search(r'^https?://', i)
                                   ) or 'https://api.live.bilibili.com'
        self.live_api_cookie_string = _load('live_api_cookie_string')
        self.only_if_no_flv = _load('only_if_no_flv', getter='getboolean')
        self.only_fmp4 = _load('only_fmp4', getter='getboolean')


config = Config()
config.load()

os.makedirs(os.path.dirname(config.logfile), exist_ok=True)
logging.basicConfig(
    format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s',
    level=config.loglevel,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config.logfile),
    ]
)
logger = logging.getLogger(__name__)
for args, kwargs in _log_queue:
    logger.log(*args, **kwargs)
