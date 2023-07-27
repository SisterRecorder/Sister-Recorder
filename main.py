#!/usr/bin/env python3
import os
import sys
import pathlib
import asyncio
import subprocess

from rooms import Room

if os.path.exists('streamlink/bin/'):
    extra_path = f'{os.path.abspath("streamlink/ffmpeg")}{os.pathsep}{os.path.abspath("streamlink/bin")}{os.pathsep}'
    os.environ['PATH'] = extra_path + os.environ["PATH"]

try:
    subprocess.run(['streamlink', '--version'])
except FileNotFoundError:
    print('Error: You need to install streamlink to use this recorder')
    sys.exit(1)
try:
    subprocess.run(['ffmpeg', '-version'])
except FileNotFoundError:
    print('Error: You need to install ffmpeg to use this recorder')
    sys.exit(1)


async def main():
    rooms = {}
    while True:
        try:
            with open('urls.txt', 'rt') as f:
                urls = [line.strip() for line in f.readlines()]
        except FileNotFoundError:
            pathlib.Path('urls.txt').touch()
            urls = []
        if not urls:
            print('Please put live urls in `urls.txt`')
            sys.exit(1)
        for url in urls:
            room_id = Room.get_room_id(url)
            if room_id and room_id not in rooms:
                rooms[room_id] = Room(room_id)
                rooms[room_id].start()
        await asyncio.sleep(300)


if __name__ == '__main__':
    asyncio.run(main())
