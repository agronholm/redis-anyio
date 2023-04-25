from __future__ import annotations

import random
import sys
from collections import defaultdict, deque
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from ssl import SSLContext
from types import TracebackType

from anyio import (
    BrokenResourceError,
    CapacityLimiter,
    ClosedResourceError,
    aclose_forcefully,
    connect_tcp,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    sleep,
)
from anyio.abc import AnyByteSendStream, AnyByteStream, TaskGroup, TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from tenacity import AsyncRetrying, retry_if_exception_type

from ._pipeline import RedisPipeline
from ._resp3 import (
    RESP3Attribute,
    RESP3BlobError,
    RESP3Parser,
    RESP3PushData,
    RESP3SimpleError,
    RESP3Value,
    serialize_command,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class RedisConnection:
    timeout: float | None
    _send_stream: AnyByteSendStream = field(init=False)
    _response_stream: MemoryObjectReceiveStream[RESP3Value] = field(init=False)
    _parser: RESP3Parser = field(init=False, default_factory=RESP3Parser)
    # type -> channel/pattern key -> set of streams
    _push_data_receivers: dict[
        str, dict[bytes, set[MemoryObjectSendStream[RESP3Value]]]
    ] = field(
        init=False,
        default_factory=lambda: defaultdict(lambda: defaultdict(set)),
    )

    async def _handle_push_data(self, item: RESP3PushData) -> None:
        if not (receivers := self._push_data_receivers.get(item.type)):
            return

        if item.type in ("message", "smessage"):
            assert isinstance(item.data[0], bytes)
            assert isinstance(item.data[1], bytes)
            target_key = item.data[0]
            channel = item.data[0].decode("utf-8", errors="replace")
            value = channel, item.data[1]
        elif item.type == "pmessage":
            assert isinstance(item.data[0], bytes)
            assert isinstance(item.data[1], bytes)
            assert isinstance(item.data[2], bytes)
            target_key = item.data[0]
            channel = item.data[1].decode("utf-8", errors="replace")
            value = channel, item.data[2]
        else:
            return

        if streams := receivers.get(target_key):
            async with create_task_group() as tg:
                for stream in streams:
                    tg.start_soon(stream.send, value)

    async def _handle_attribute(self, attribute: RESP3Attribute) -> None:
        pass  # Drop attributes on the floor for now

    async def run(self, stream: AnyByteStream, *, task_status: TaskStatus) -> None:
        send, self._response_stream = create_memory_object_stream(100)
        async with stream, send:
            self._send_stream = stream
            task_status.started()
            try:
                async for data in stream:
                    self._parser.feed_bytes(data)
                    for item in self._parser:
                        if isinstance(item, RESP3PushData):
                            await self._handle_push_data(item)
                        elif isinstance(item, RESP3Attribute):
                            await self._handle_attribute(item)
                        else:
                            await send.send(item)
            except ClosedResourceError:
                pass

    async def validate(self) -> bool:
        nonce = str(random.randint(0, 100000))
        if await self.execute_command("PING", nonce) != nonce:
            await aclose_forcefully(self._send_stream)
            return False

        return True

    async def execute_command(
        self, command: str, *args: object, wait_reply: bool = True
    ) -> RESP3Value:
        # Send the command
        payload = serialize_command(command, *args)
        # with fail_after(self.timeout):
        print("sent:", payload)
        await self._send_stream.send(payload)

        if not wait_reply:
            return None

        # Read back the response
        while True:
            # with fail_after(self.timeout):
            response = await self._response_stream.receive()
            if isinstance(response, Exception):
                raise response

            return response

    async def execute_pipeline(
        self, pipeline: RedisPipeline
    ) -> list[RESP3Value | RESP3BlobError | RESP3SimpleError]:
        # Send the commands
        payload = b"".join(
            [serialize_command(command, *args) for command, args in pipeline]
        )
        await self._send_stream.send(payload)

        # Read back the responses
        responses: list[RESP3Value | RESP3BlobError | RESP3SimpleError] = []
        while len(responses) < len(pipeline):
            with fail_after(self.timeout):
                responses.append(await self._response_stream.receive())

        return responses

    @contextmanager
    def add_push_data_receiver(
        self,
        stream: MemoryObjectSendStream[RESP3Value],
        keys: Sequence[str],
        type_: str,
    ) -> Generator[None, None, None]:
        receivers = self._push_data_receivers[type_]
        for key in keys:
            receivers[key.encode("utf-8")].add(stream)

        yield

        for key in keys:
            receivers[key.encode("utf-8")].remove(stream)

        if not receivers:
            del self._push_data_receivers[type_]


@dataclass(frozen=True)
class RedisConnectionPoolStatistics:
    max_connections: int
    total_connections: int
    idle_connections: int
    busy_connections: int


@dataclass
class RedisConnectionPool:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    timeout: float = 10
    connect_timeout: float = 10
    username: str | None = None
    password: str | None = None
    ssl_context: SSLContext | None = None
    capacity: int = 2**31
    _closed: bool = field(init=False, default=False)
    _idle_connections: deque[RedisConnection] = field(init=False, default_factory=deque)
    _limiter: CapacityLimiter = field(init=False)
    _parser: RESP3Parser = field(init=False, default_factory=RESP3Parser)
    _connections_task_group: TaskGroup = field(init=False)

    async def __aenter__(self) -> Self:
        self._limiter = CapacityLimiter(self.capacity)
        self._connections_task_group = create_task_group()
        await self._connections_task_group.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self._closed = True
        self._connections_task_group.cancel_scope.cancel()
        await self._connections_task_group.__aexit__(exc_type, exc_val, exc_tb)
        self._idle_connections.clear()

    def statistics(self) -> RedisConnectionPoolStatistics:
        """Return statistics about the max/busy/idle connections in this pool."""
        acquired_connections = self._limiter.statistics().borrowed_tokens
        idle_connections = len(self._idle_connections)
        return RedisConnectionPoolStatistics(
            max_connections=self.capacity,
            total_connections=acquired_connections + idle_connections,
            idle_connections=idle_connections,
            busy_connections=acquired_connections,
        )

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[RedisConnection, None]:
        if self._closed:
            raise RuntimeError("This pool is closed")

        async with self._limiter:
            while self._idle_connections:
                conn = self._idle_connections.popleft()
                if await conn.validate():
                    break
            else:
                conn = await self._add_connection()

            try:
                yield conn
            finally:
                self._idle_connections.append(conn)

    async def _add_connection(self) -> RedisConnection:
        # Connect to the Redis server
        with fail_after(self.connect_timeout):
            if self.ssl_context:
                stream: AnyByteStream = await connect_tcp(
                    self.host, self.port, ssl_context=self.ssl_context
                )
            else:
                stream = await connect_tcp(self.host, self.port)

        try:
            conn = RedisConnection(self.timeout)
            await self._connections_task_group.start(conn.run, stream)

            # Assemble authentication arguments for the HELLO command
            if self.username is not None and self.password is not None:
                auth_args: tuple[str, str, str] | tuple[()] = (
                    "AUTH",
                    self.username,
                    self.password,
                )
            else:
                auth_args = ()

            # Switch to the RESP3 protocol
            await conn.execute_command("HELLO", "3", *auth_args)

            # Switch to the selected database, if it's not the default of 0
            if self.db:
                await conn.execute_command("SELECT", self.db)
        except BaseException:
            # Force close the connection
            await aclose_forcefully(stream)
            raise

        return conn

    async def execute_command(self, command: str, *args: object) -> RESP3Value:
        async for attempt in AsyncRetrying(
            sleep=sleep, retry=retry_if_exception_type(BrokenResourceError)
        ):
            with attempt:
                async with self.acquire() as conn:
                    return await conn.execute_command(command, *args)

        raise AssertionError("Execution should never get to this point")

    async def execute_pipeline(
        self, pipeline: RedisPipeline
    ) -> list[RESP3Value | RESP3BlobError | RESP3SimpleError]:
        async for attempt in AsyncRetrying(
            sleep=sleep, retry=retry_if_exception_type(BrokenResourceError)
        ):
            with attempt:
                async with self.acquire() as conn:
                    return await conn.execute_pipeline(pipeline)

        raise AssertionError("Execution should never get to this point")
