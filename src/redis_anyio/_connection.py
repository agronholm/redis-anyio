from __future__ import annotations

import logging
import random
import sys
from collections import defaultdict, deque
from collections.abc import AsyncGenerator, Generator
from contextlib import (
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass, field
from ssl import SSLContext
from types import TracebackType
from typing import Any

from anyio import (
    BrokenResourceError,
    ClosedResourceError,
    EndOfStream,
    Semaphore,
    aclose_forcefully,
    connect_tcp,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    sleep,
)
from anyio.abc import AnyByteSendStream, AnyByteStream, TaskGroup, TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream
from tenacity import AsyncRetrying, retry_if_exception_type
from tenacity.stop import stop_base
from tenacity.wait import wait_base

from ._exceptions import ConnectivityError, ResponseError
from ._resp3 import (
    RESP3Attributes,
    RESP3BlobError,
    RESP3Parser,
    RESP3PushData,
    RESP3SimpleError,
    serialize_command,
)
from ._subscription import Subscription
from ._types import ResponseValue
from ._utils import decode_response_value

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

PUBSUB_REPLIES = frozenset(
    [
        "subscribe",
        "ssubscribe",
        "psubscribe",
        "unsubscribe",
        "sunsubscribe",
        "punsubscribe",
    ]
)

logger = logging.getLogger("redis_anyio")


@dataclass
class RedisConnection:
    timeout: float | None
    _send_stream: AnyByteSendStream = field(init=False)
    _response_stream: MemoryObjectReceiveStream[ResponseValue | ResponseError] = field(
        init=False
    )
    _parser: RESP3Parser = field(init=False, default_factory=RESP3Parser)
    # type -> channel/pattern key -> list of subscription objects
    _subscriptions: dict[str, dict[bytes, list[Subscription[Any]]]] = field(
        init=False,
        default_factory=lambda: defaultdict(lambda: defaultdict(list)),
    )

    async def _handle_push_data(self, item: RESP3PushData) -> None:
        assert isinstance(item.data[0], bytes)
        subscriptions_by_channel = self._subscriptions.get(item.type) or {}
        if subscriptions := subscriptions_by_channel.get(item.data[0]):
            assert isinstance(item.data[-1], bytes)
            assert isinstance(item.data[-2], bytes)
            channel = item.data[-2].decode("utf-8", errors="backslashreplace")
            async with create_task_group() as tg:
                for subscription in subscriptions:
                    tg.start_soon(subscription.deliver, channel, item.data[-1])

    async def _handle_attribute(self, attribute: RESP3Attributes) -> None:
        pass  # Drop attributes on the floor for now

    async def aclose(self, *, force: bool = True) -> None:
        all_subscriptions = {
            sub
            for subs_dict in self._subscriptions.values()
            for subs in subs_dict.values()
            for sub in subs
        }
        for subscription in all_subscriptions:
            await subscription.aclose()

        if force:
            await aclose_forcefully(self._send_stream)
        else:
            await self._send_stream.aclose()

    async def run(self, stream: AnyByteStream, *, task_status: TaskStatus) -> None:
        send, self._response_stream = create_memory_object_stream(100)
        async with stream, send:
            self._send_stream = stream
            task_status.started()
            while True:
                try:
                    data = await stream.receive()
                except ClosedResourceError:
                    await self.aclose()
                    return
                except (EndOfStream, BrokenResourceError):
                    await self.aclose(force=True)
                    return

                logger.debug("Received data from server: %r", data)
                self._parser.feed_bytes(data)
                for item in self._parser:
                    if isinstance(item, RESP3PushData):
                        if item.type in PUBSUB_REPLIES:
                            await send.send(None)
                        else:
                            await self._handle_push_data(item)
                    elif isinstance(item, RESP3Attributes):
                        await self._handle_attribute(item)
                    elif isinstance(item, RESP3SimpleError):
                        await send.send(ResponseError(item.code, item.message))
                    elif isinstance(item, RESP3BlobError):
                        await send.send(
                            ResponseError(
                                item.code.decode("utf-8", errors="replace"),
                                item.message.decode("utf-8", errors="replace"),
                            )
                        )
                    else:
                        await send.send(item)

    async def validate(self) -> bool:
        nonce = str(random.randint(0, 100000))
        if await self.execute_command("PING", nonce) != nonce:
            await aclose_forcefully(self._send_stream)
            return False

        return True

    async def send_command(self, command: str, *args: object) -> None:
        payload = serialize_command(command, *args)
        with fail_after(self.timeout):
            await self._send_stream.send(payload)
            logger.debug("Sent data to server: %r", payload)

    async def read_next_response(
        self, *, decode: bool
    ) -> ResponseValue | ResponseError:
        with fail_after(self.timeout):
            try:
                response = await self._response_stream.receive()
            except EndOfStream:
                raise ConnectivityError from None

            if decode and not isinstance(response, ResponseError):
                response = decode_response_value(response)

            return response

    async def execute_command(
        self, command: str, *args: object, decode: bool = True
    ) -> ResponseValue:
        """
        Send an arbitrary command to the server, and wait for a response.

        :return: the response sent by the server
        :raises ResponseError: if the server returns an error response

        """
        await self.send_command(command, *args)

        # Read back the response
        with fail_after(self.timeout):
            response = await self.read_next_response(decode=decode)
            if isinstance(response, ResponseError):
                raise response

        return response

    @contextmanager
    def add_subscription(
        self, subscription: Subscription[Any]
    ) -> Generator[None, None, None]:
        for type_ in (subscription.subscribe_category, subscription.message_category):
            subscriptions_by_channel = self._subscriptions[type_]
            for key in subscription.channels:
                subscriptions_by_channel[key.encode("utf-8")].append(subscription)

        yield

        for type_ in (subscription.subscribe_category, subscription.message_category):
            subscriptions_by_channel = self._subscriptions[type_]
            for key in subscription.channels:
                subscriptions_by_channel[key.encode("utf-8")].remove(subscription)

        for type_ in (subscription.subscribe_category, subscription.message_category):
            subscriptions_by_channel = self._subscriptions[type_]
            if not subscriptions_by_channel:
                del self._subscriptions[type_]


@dataclass(frozen=True)
class RedisConnectionPoolStatistics:
    """
    .. attribute:: max_connections
        :type: int

        This is the maximum number of connections the connection pool allows.

    .. attribute:: total_connections
        :type: int

        This is the total number of connections in the pool (idle + busy).

    .. attribute:: idle_connections
        :type: int

        This is the maximum number of open connections available to be acquired.

    .. attribute:: busy_connections
        :type: int

        This is the maximum number of connections currently acquired from the pool.
    """

    max_connections: int
    total_connections: int
    idle_connections: int
    busy_connections: int


@dataclass
class RedisConnectionPool:
    host: str
    port: int
    db: int
    ssl_context: SSLContext | None
    username: str | None
    password: str | None
    max_connections: int
    timeout: float
    connect_timeout: float
    retry_wait: wait_base
    retry_stop: stop_base
    _closed: bool = field(init=False, default=False)
    _idle_connections: deque[RedisConnection] = field(init=False, default_factory=deque)
    _capacity_semaphore: Semaphore = field(init=False)
    _parser: RESP3Parser = field(init=False, default_factory=RESP3Parser)
    _connections_task_group: TaskGroup = field(init=False)

    async def __aenter__(self) -> Self:
        self._capacity_semaphore = Semaphore(
            self.max_connections, max_value=self.max_connections
        )
        self._connections_task_group = create_task_group()
        await self._connections_task_group.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._closed = True
        self._connections_task_group.cancel_scope.cancel()
        await self._connections_task_group.__aexit__(exc_type, exc_val, exc_tb)
        self._idle_connections.clear()

    def _retry(self, *exceptions: type[Exception]) -> AsyncRetrying:
        return AsyncRetrying(
            wait=self.retry_wait,
            stop=self.retry_stop,
            sleep=sleep,
            reraise=True,
            retry=retry_if_exception_type(exceptions),
        )

    def statistics(self) -> RedisConnectionPoolStatistics:
        """Return statistics about the max/busy/idle connections in this pool."""
        acquired_connections = self.max_connections - self._capacity_semaphore.value
        idle_connections = len(self._idle_connections)
        return RedisConnectionPoolStatistics(
            max_connections=self.max_connections,
            total_connections=acquired_connections + idle_connections,
            idle_connections=idle_connections,
            busy_connections=acquired_connections,
        )

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[RedisConnection, None]:
        if self._closed:
            raise RuntimeError("This pool is closed")

        async with self._capacity_semaphore:
            while self._idle_connections:
                conn = self._idle_connections.popleft()
                if await conn.validate():
                    break
            else:
                conn = await self._add_connection()

            try:
                yield conn
            except BaseException:
                # Throw away the connection when there's any trouble
                await conn.aclose(force=True)
                raise
            else:
                # Otherwise, put it back in the pool
                self._idle_connections.append(conn)

    async def _add_connection(self) -> RedisConnection:
        # Connect to the Redis server
        async for attempt in self._retry(
            OSError, ConnectivityError, ClosedResourceError, TimeoutError
        ):
            with attempt:
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
