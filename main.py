#!/usr/bin/env python3
import asyncio

from rooms import Room


async def main():
    with open('urls.txt', 'rt') as f:
        urls = [line.strip() for line in f.readlines()]
    rooms = [Room.from_url(url) for url in urls if url]
    for room in rooms:
        room.start()
    while True:
        await asyncio.sleep(1000)

asyncio.run(main())
