#!/usr/bin/env python3
import asyncio

from rooms import Room


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

asyncio.run(main())
