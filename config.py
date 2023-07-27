import configparser
import os
import re
import time


class Config:
    def __init__(self):
        self.config_list = [
            'config.ini',
            'config.example.ini',
        ]
        self._last_load = 0

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
            value = transform(getattr(parser, getter)(section, option, fallback=None))
            if value and not validator(value):
                print('invalid {section}.{option} value: {value}')
            else:
                return value

        def _load_choices(option, choices, **kwargs):
            value = _load(option, **kwargs)
            if value in choices:
                return value
            else:
                return choices[0]

        section = 'global'
        self.record_backend = _load_choices('record_backend', ['streamlink', 'ffmpeg'])
        self.output_ext = _load_choices('output_ext', ['ts', 'mp4'])
        self.api_proxy = _load('api_proxy', validator=lambda i: re.search(r'^(https?|socks\d?)://[^/]+(:\d+)?(/|$)', i))
        self.only_if_no_flv = _load('only_if_no_flv', getter='getboolean')
        self.only_fmp4 = _load('only_fmp4', getter='getboolean')


config = Config()
config.load()
