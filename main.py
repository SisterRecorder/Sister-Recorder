#!/usr/bin/env python3
import os
import sys
import pathlib
import asyncio
import subprocess
import logging
import json
import concurrent.futures
import multiprocessing

from config import config
from rooms import Room
from utils import close_sessions
from hls_new import _file_handlers

import tracemalloc
import gc
tracemalloc.start(25)

logger = logging.getLogger(__name__)


logger.info('loading recorder')
if os.path.exists('streamlink/bin/'):
    logger.debug('adding bundled streamlink to PATH')
    extra_path = f'{os.path.abspath("streamlink/ffmpeg")}{os.pathsep}{os.path.abspath("streamlink/bin")}{os.pathsep}'
    os.environ['PATH'] = extra_path + os.environ["PATH"]

if config.record_backend == 'streamlink':
    try:
        logger.debug('checking streamlink executable')
        subprocess.run(['streamlink', '--version'], stdout=subprocess.DEVNULL)
    except FileNotFoundError:
        logger.error('Error: You need to install streamlink to use this recorder')
        sys.exit(1)

if config.record_backend == 'ffmpeg':
    try:
        logger.debug('checking ffmpeg executable')
        subprocess.run(['ffmpeg', '-version'], stdout=subprocess.DEVNULL)
    except FileNotFoundError:
        logger.error('Error: You need to install ffmpeg to use this recorder')
        sys.exit(1)


async def main():
    rooms: dict[int, Room] = {}
    logger.info('start loading rooms')
    try:
        while True:
            try:
                try:
                    with open('urls.txt', 'rt') as f:
                        urls = [line.strip() for line in f.readlines()]
                except FileNotFoundError:
                    pathlib.Path('urls.txt').touch()
                    urls = []
                if not urls:
                    logger.warn('Please put livestream room urls in `urls.txt`')
                else:
                    room_ids = {Room.get_room_id(url) for url in urls}
                    for room_id in room_ids - set(rooms):
                        if room_id:
                            logger.info(f'adding new room {room_id}')
                            rooms[room_id] = Room(room_id)
                            rooms[room_id].start()
                            await asyncio.sleep(1)
                    for room_id in set(rooms) - room_ids:
                        await rooms.pop(room_id).stop()
                await asyncio.sleep(30)
            except (KeyboardInterrupt, SystemExit):
                break
            except Exception:
                logger.exception('error while running main loop')
                await asyncio.sleep(30)
    finally:
        logger.info('shutting down')
        await asyncio.gather(*[room.stop() for room in rooms.values()])
        await close_sessions()

if __name__ == '__main__':
    logger.info('starting recorder')
    try:
        asyncio.run(main())
    finally:
        for handler in _file_handlers.values():
            handler.close()
        logging.shutdown()
