from __future__ import annotations

import sys
from contextlib import (
    AsyncExitStack,
)
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, AnyStr, Generic

from anyio import ClosedResourceError, create_memory_object_stream
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

if TYPE_CHECKING:
    from ._connection import RedisConnection, RedisConnectionPool

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass(frozen=True)
class Message(Generic[AnyStr]):
    topic: str
    message: AnyStr

    def __str__(self) -> str:
        if isinstance(self.message, str):
            return self.message
        else:
            return self.message.decode("utf-8", errors="backslashreplace")

    def __bytes__(self) -> bytes:
        if isinstance(self.message, str):
            return self.message.encode("utf-8")
        else:
            return self.message


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
    _send_stream: MemoryObjectSendStream[tuple[str, bytes]] = field(init=False)
    _receive_stream: MemoryObjectReceiveStream[tuple[str, bytes]] = field(init=False)

    async def _subscribe(self) -> None:
        self._conn = await self._exit_stack.enter_async_context(self.pool.acquire())
        self._send_stream, self._receive_stream = create_memory_object_stream(5)
        await self._exit_stack.enter_async_context(self._receive_stream)
        self._exit_stack.enter_context(self._conn.add_subscription(self))
        await self._conn.execute_command(self.subscribe_command, *self.channels)
        self._exit_stack.push_async_callback(self._unsubscribe)

    async def _unsubscribe(self) -> None:
        try:
            await self._conn.execute_command(self.unsubscribe_command, *self.channels)
        except ClosedResourceError:
            pass

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

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Message[AnyStr]:
        try:
            topic, raw_message = await self._receive_stream.receive()
        except BaseException:
            await self._exit_stack.__aexit__(*sys.exc_info())
            raise

        if self.decode:
            message = raw_message.decode("utf-8", errors="backslashreplace")
            return Message(topic, message)  # type: ignore[arg-type]
        else:
            return Message(topic, raw_message)  # type: ignore[arg-type]

    async def deliver(self, channel: str, message: bytes) -> None:
        await self._send_stream.send((channel, message))
