from __future__ import annotations

import logging
import random
import sys
from collections import defaultdict, deque
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass, field
from ssl import SSLContext
from types import TracebackType
from typing import Any, AnyStr, Generic

from anyio import (
    ClosedResourceError,
    Semaphore,
    aclose_forcefully,
    connect_tcp,
    create_memory_object_stream,
    create_task_group,
    fail_after,
)
from anyio.abc import AnyByteSendStream, AnyByteStream, TaskGroup, TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from ._exceptions import ResponseError
from ._resp3 import (
    RESP3Attributes,
    RESP3BlobError,
    RESP3Parser,
    RESP3PushData,
    RESP3SimpleError,
    serialize_command,
)
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


@dataclass(eq=False)
class Subscription(Generic[AnyStr]):
    pool: RedisConnectionPool
    channels: tuple[str, ...]
    subscribe_category: str
    message_category: str
    decode: bool
    subscribe_command: str
    unsubscribe_command: str
    _exit_stack: AsyncExitStack = field(init=False, default_factory=AsyncExitStack)
    _conn: RedisConnection = field(init=False)
    _send_stream: MemoryObjectSendStream[tuple[str, AnyStr]] = field(init=False)
    _receive_stream: MemoryObjectReceiveStream[tuple[str, AnyStr]] = field(init=False)

    async def _subscribe(self) -> None:
        self._conn = await self._exit_stack.enter_async_context(self.pool.acquire())
        self._send_stream, self._receive_stream = create_memory_object_stream(5)
        await self._exit_stack.enter_async_context(self._receive_stream)
        self._exit_stack.enter_context(self._conn.add_subscription(self))
        await self._conn.execute_command(self.subscribe_command, *self.channels)
        self._exit_stack.push_async_callback(
            self._conn.execute_command, self.unsubscribe_command, *self.channels
        )

    async def __aenter__(self) -> Self:
        await self._exit_stack.__aenter__()
        await self._subscribe()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> tuple[str, AnyStr]:
        try:
            return await self._receive_stream.receive()
        except BaseException:
            await self._exit_stack.__aexit__(*sys.exc_info())
            raise

    async def deliver(self, channel: str, message: AnyStr) -> None:
        await self._send_stream.send((channel, message))


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
                    if subscription.decode:
                        message: str | bytes = item.data[-1].decode(
                            "utf-8", errors="backslashreplace"
                        )
                    else:
                        message = item.data[-1]

                    tg.start_soon(subscription.deliver, channel, message)

    async def _handle_attribute(self, attribute: RESP3Attributes) -> None:
        pass  # Drop attributes on the floor for now

    async def aclose(self, *, force: bool = True) -> None:
        if force:
            await aclose_forcefully(self._send_stream)
        else:
            await self._send_stream.aclose()

    async def run(self, stream: AnyByteStream, *, task_status: TaskStatus) -> None:
        send, self._response_stream = create_memory_object_stream(100)
        async with stream, send:
            self._send_stream = stream
            task_status.started()
            try:
                async for data in stream:
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
            except ClosedResourceError:
                pass

    async def validate(self) -> bool:
        nonce = str(random.randint(0, 100000))
        if await self.execute_command("PING", nonce) != nonce:
            await aclose_forcefully(self._send_stream)
            return False

        return True

    async def execute_command(
        self, command: str, *args: object, decode: bool = True
    ) -> ResponseValue:
        """
        Send an arbitrary command to the server, and wait for a response.

        :return: the response sent by the server
        :raises ResponseError: if the server returns an error response

        """
        # Send the command
        payload = serialize_command(command, *args)
        with fail_after(self.timeout):
            await self._send_stream.send(payload)
            logger.debug("Sent data to server: %r", payload)

        # Read back the response
        while True:
            with fail_after(self.timeout):
                response = await self._response_stream.receive()
                if isinstance(response, ResponseError):
                    raise response

            if decode:
                return decode_response_value(response)

            return response

    async def execute_pipeline(
        self, commands: Sequence[bytes]
    ) -> Sequence[ResponseValue | ResponseError]:
        """
        Send a pipeline of commands to the server and wait for all the replies.

        :return: a list of responses or :class:`ResponseError` exceptions, in the same
            order as the original commands

        """
        # Send the commands
        payload = b"".join(commands)
        await self._send_stream.send(payload)

        # Read back the responses
        responses: list[ResponseValue | ResponseError] = []
        while len(responses) < len(commands):
            with fail_after(self.timeout):
                responses.append(await self._response_stream.receive())

        return responses

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
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    timeout: float = 10
    connect_timeout: float = 10
    username: str | None = None
    password: str | None = None
    ssl_context: SSLContext | None = None
    capacity: int = 2**16 - 1  # TCP port numbers are 16 bit unsigned ints
    _closed: bool = field(init=False, default=False)
    _idle_connections: deque[RedisConnection] = field(init=False, default_factory=deque)
    _capacity_semaphore: Semaphore = field(init=False)
    _parser: RESP3Parser = field(init=False, default_factory=RESP3Parser)
    _connections_task_group: TaskGroup = field(init=False)

    async def __aenter__(self) -> Self:
        self._capacity_semaphore = Semaphore(self.capacity, max_value=self.capacity)
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

    def statistics(self) -> RedisConnectionPoolStatistics:
        """Return statistics about the max/busy/idle connections in this pool."""
        acquired_connections = self.capacity - self._capacity_semaphore.value
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
