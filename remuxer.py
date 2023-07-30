import os
import asyncio
import time
from typing import Optional, Protocol

import aiofile
from m3u8.model import Segment, InitializationSection

from utils import Logging


SEGMENT_QUEUE_WAIT_LIMIT = 50


class RemuxerInterface(Protocol):
    def __init__(self, outdir: str, basedir: str, outname=None, logger=None):
        raise NotImplementedError

    async def add_init_segments(self, segs: list[tuple[InitializationSection, str]]):
        raise NotImplementedError

    async def add_segment(self, seg: Segment, seg_fn: str):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    async def join(self):
        raise NotImplementedError


class FMp4Remuxer(RemuxerInterface, Logging):
    def __init__(self, outdir: str, basedir: str, outname=None, logger=None):
        self.segments = {}
        self._init_segments = []
        self.running = False
        self.logger = logger
        self.concat_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        if not outname:
            outname = f'remux-{time.strftime("%y%m%d-%H%M%S")}.mp4'
        self.out_fn = os.path.join(outdir, basedir, outname)
        self.outdir = outdir
        self.last_segment: int = -1
        self._merge_future = None

    def close(self):
        async def _close():
            for seq in sorted(self.segments):
                await self._put_segment(seq)
        self.info('closing remuxer')
        asyncio.ensure_future(_close())
        self.running = False

    async def join(self):
        await self.concat_queue.put(None)
        if self._merge_future:
            await self._merge_future
        self.info('remuxer ended')

    async def add_init_segments(self, segs: list[tuple[InitializationSection, str]]):
        if self.running:
            return
        self._init_segments = [fn for seg, fn in segs]
        self.running = True
        self.info(f'starting remuxer with init segments to {self.out_fn}')
        self._merge_future = asyncio.create_task(self._merge_worker())

    async def _put_segment(self, sequence: int):
        seg_fn = self.segments.pop(sequence)
        await self.concat_queue.put(seg_fn)

    def warn_missing_segs(self, last: int, current: int):
        if last == -1:
            return
        missing = (last + 1, current - 1)
        if missing[0] == missing[1]:
            self.info(f'fMp4 Missing segment {missing[0]} for output {self.out_fn}')
        else:
            self.info(f'fMp4 Missing segment {missing[0]}-{missing[1]} for output {self.out_fn}')

    async def add_segment(self, seg: Segment, seg_fn: str):
        if not seg.media_sequence:
            return
        self.segments[seg.media_sequence] = seg_fn
        if len(self.segments) > SEGMENT_QUEUE_WAIT_LIMIT:
            self.warn_missing_segs(self.last_segment, min(self.segments))
            self.last_segment = min(self.segments)
            await self._put_segment(self.last_segment)
        while (self.last_segment + 1) in self.segments:
            self.last_segment += 1
            await self._put_segment(self.last_segment)

    async def _merge_worker(self, mode='wb'):
        async with aiofile.async_open(self.out_fn, mode) as out_f:
            for seg_fn in self._init_segments:
                async with aiofile.async_open(seg_fn, 'rb') as seg_f:
                    n_bytes = await out_f.write(await seg_f.read())
                    self.debug(f'writting {n_bytes}bytes from init segment '
                               f'{os.path.basename(seg_fn)} to {self.out_fn}, total {out_f.tell()}')
            while self.running or self.concat_queue.qsize():
                seg_fn = await self.concat_queue.get()
                if seg_fn:
                    async with aiofile.async_open(seg_fn, 'rb') as seg_f:
                        n_bytes = await out_f.write(await seg_f.read())
                        self.debug(f'writting {n_bytes}bytes from segment '
                                   f'{os.path.basename(seg_fn)} to {self.out_fn}, total {out_f.tell()}')
        self.info(f'remux to {self.out_fn} ended')
