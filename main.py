#!/usr/bin/env python3
import asyncio

from rooms import Room

URLS = [
    'https://live.bilibili.com/1',
]


async def main():
    rooms = [Room.from_url(url) for url in URLS]
    for room in rooms:
        room.start()
    while True:
        await asyncio.sleep(1000)

asyncio.run(main())
