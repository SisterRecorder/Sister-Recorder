#!/usr/bin/env python3
import os
import sys
import pathlib
import asyncio
import subprocess
import logging

from config import config
from rooms import Room

logger = logging.getLogger(__name__)


logger.info('loading recorder')
if os.path.exists('streamlink/bin/'):
    logger.debug('adding bundled streamlink to PATH')
    extra_path = f'{os.path.abspath("streamlink/ffmpeg")}{os.pathsep}{os.path.abspath("streamlink/bin")}{os.pathsep}'
    os.environ['PATH'] = extra_path + os.environ["PATH"]

try:
    logger.debug('checking streamlink executable')
    subprocess.run(['streamlink', '--version'], stdout=subprocess.DEVNULL)
except FileNotFoundError:
    logger.error('Error: You need to install streamlink to use this recorder')
    sys.exit(1)
try:
    logger.debug('checking ffmpeg executable')
    subprocess.run(['ffmpeg', '-version'], stdout=subprocess.DEVNULL)
except FileNotFoundError:
    logger.error('Error: You need to install ffmpeg to use this recorder')
    sys.exit(1)


async def main():
    rooms = {}
    logger.info('start loading rooms')
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
                await asyncio.sleep(30)
            else:
                for url in urls:
                    room_id = Room.get_room_id(url)
                    if room_id and room_id not in rooms:
                        logger.info(f'adding new room {room_id}')
                        rooms[room_id] = Room(room_id)
                        rooms[room_id].start()
                await asyncio.sleep(30)
        except Exception:
            logger.exception('error while running main loop')
            await asyncio.sleep(30)


if __name__ == '__main__':
    asyncio.run(main())
