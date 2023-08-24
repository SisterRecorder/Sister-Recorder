import asyncio
import logging
from collections import deque
from typing import Deque, Dict, cast, AsyncIterator
from urllib.parse import urlparse
from contextlib import asynccontextmanager

import httpx
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, ErrorCode, H3Connection
from aioquic.h3.events import (
    DataReceived,
    H3Event,
    Headers,
    HeadersReceived,
)
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

logger = logging.getLogger(__name__)

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'


class H3ResponseStream(httpx.AsyncByteStream):
    def __init__(self, aiterator: AsyncIterator[bytes]):
        self._aiterator = aiterator

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for part in self._aiterator:
            yield part


class H3Transport(QuicConnectionProtocol, httpx.AsyncBaseTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._http = H3Connection(self._quic)
        self._read_queue: Dict[int, Deque[H3Event]] = {}
        self._read_ready: Dict[int, asyncio.Event] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)

        stream_id = self._quic.get_next_available_stream_id()
        self._read_queue[stream_id] = deque()
        self._read_ready[stream_id] = asyncio.Event()

        # prepare request
        self._http.send_headers(
            stream_id=stream_id,
            headers=[
                (b":method", request.method.encode()),
                (b":scheme", request.url.raw_scheme),
                (b":authority", request.url.netloc),
                (b":path", request.url.raw_path),
            ] + [
                (k.lower(), v)
                for (k, v) in request.headers.raw
                if k.lower() not in (b"connection", b"host")
            ],
        )
        async for data in request.stream:
            self._http.send_data(stream_id=stream_id, data=data, end_stream=False)
        self._http.send_data(stream_id=stream_id, data=b"", end_stream=True)

        # transmit request
        self.transmit()

        # process response
        status_code, headers, stream_ended = await self._receive_response(stream_id)

        return httpx.Response(
            status_code=status_code,
            headers=headers,
            stream=H3ResponseStream(self._receive_response_data(stream_id, stream_ended)),
            extensions={"http_version": b"HTTP/3"},
        )

    def http_event_received(self, event: H3Event):
        if isinstance(event, (HeadersReceived, DataReceived)):
            stream_id = event.stream_id
            if stream_id in self._read_queue:
                self._read_queue[event.stream_id].append(event)
                self._read_ready[event.stream_id].set()

    def quic_event_received(self, event: QuicEvent):
        for http_event in self._http.handle_event(event):
            self.http_event_received(http_event)

    async def _receive_response(self, stream_id: int) -> tuple[int, Headers, bool]:
        """
        Read the response status and headers.
        """
        stream_ended = False
        while True:
            event = await self._wait_for_http_event(stream_id)
            if isinstance(event, HeadersReceived):
                stream_ended = event.stream_ended
                break

        headers = []
        status_code = 0
        for header, value in event.headers:
            if header == b":status":
                status_code = int(value.decode())
            else:
                headers.append((header, value))
        return status_code, headers, stream_ended

    async def _receive_response_data(self, stream_id: int, stream_ended: bool) -> AsyncIterator[bytes]:
        """
        Read the response data.
        """
        while not stream_ended:
            event = await self._wait_for_http_event(stream_id)
            if isinstance(event, DataReceived):
                stream_ended = event.stream_ended
                yield event.data
            elif isinstance(event, HeadersReceived):
                stream_ended = event.stream_ended

    async def _wait_for_http_event(self, stream_id: int) -> H3Event:
        """
        Returns the next HTTP/3 event for the given stream.
        """
        if not self._read_queue[stream_id]:
            await self._read_ready[stream_id].wait()
        event = self._read_queue[stream_id].popleft()
        if not self._read_queue[stream_id]:
            self._read_ready[stream_id].clear()
        return event


class H3Client:
    def __init__(self, timeout=10) -> None:
        self.timeout = timeout

        self._sessions: dict[str, asyncio.Future[httpx.AsyncClient]] = {}
        self._running_event = asyncio.Future()
        self._join: list[asyncio.Task] = []

    async def get_session(self, url: str):
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        host = parsed.hostname
        assert host
        port = parsed.port or 443

        loc = f'{host}:{port}'
        if loc not in self._sessions:
            self._sessions[loc] = asyncio.Future()
            self._join.append(asyncio.ensure_future(self._connect_session(host, port)))
            logger.debug(f'creating session at {loc}')
        return await self._sessions[loc]

    async def _connect_session(self, host: str, port: int):
        try:
            configuration = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)

            async with connect(
                host, port, configuration=configuration, create_protocol=H3Transport,
            ) as session:
                async with httpx.AsyncClient(transport=cast(httpx.AsyncBaseTransport, session), timeout=self.timeout) as client:
                    self._sessions[f'{host}:{port}'].set_result(client)
                    await self._running_event
                session._quic.close(error_code=ErrorCode.H3_NO_ERROR)
        except Exception as e:
            self._sessions.pop(f'{host}:{port}').set_exception(e)

    @asynccontextmanager
    async def stream(self, method: str, url: str, **kwargs):
        logger.debug(f'getting for {url}')
        session = await self.get_session(url)
        async with session.stream(method, url, **kwargs) as rsp:
            logger.debug(f'got response for {url}')
            yield rsp
            logger.debug(f'response processed for {url}')

    async def close(self):
        self._running_event.set_result(None)
        await asyncio.gather(*self._join)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


async def _main():
    async with H3Client() as client:
        async with client.stream('GET', 'https://www.google.com/') as rsp:
            print(rsp.status_code)
            print(type(rsp))
            print(await rsp.aread())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(_main())
