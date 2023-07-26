#!/usr/bin/env python3
import os
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
    exit(1)


async def main():
    try:
        with open('urls.txt', 'rt') as f:
            urls = [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        with open('urls.txt', 'wt') as f:
            urls = []
    if not urls:
        print('Please put live urls in `urls.txt`')
        exit(1)
    rooms = [Room.from_url(url) for url in urls if url]
    for room in rooms:
        room.start()
    while True:
        await asyncio.sleep(1000)


if __name__ == '__main__':
    asyncio.run(main())
