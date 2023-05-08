from __future__ import annotations

import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING, AnyStr, Generic, cast

from anyio import (
    ClosedResourceError,
    EndOfStream,
    create_memory_object_stream,
)
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

if TYPE_CHECKING:
    from ._connection import RedisConnection, RedisConnectionPool

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

CONNECTION_STATE_CHANNEL = "_connection_state"
DISCONNECTED = "disconnected"
RECONNECTED = "reconnected"


@dataclass(frozen=True)
class Message(Generic[AnyStr]):
    """
    .. attribute:: channel
        :type: str

        This is the name of the channel the message was received on.

    .. attribute:: message
        :type: str | bytes

        This is the actual received message. Unless you specified ``decode=False`` when
        subscribing, this will be a :class:`str`. Otherwise it will be a :class:`bytes`
        object.
    """

    channel: str
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
    send_connection_state_changes: bool
    decode: bool
    subscribe_command: str
    unsubscribe_command: str
    _exit_stack: AsyncExitStack = field(init=False, default_factory=AsyncExitStack)
    _conn: RedisConnection = field(init=False)
    _send_stream: MemoryObjectSendStream[Message[AnyStr]] = field(init=False)
    _receive_stream: MemoryObjectReceiveStream[Message[AnyStr]] = field(init=False)

    async def _subscribe(self) -> None:
        async with AsyncExitStack() as exit_stack:
            self._conn = await exit_stack.enter_async_context(self.pool.acquire())
            self._send_stream, self._receive_stream = create_memory_object_stream(5)
            await exit_stack.enter_async_context(self._receive_stream)
            exit_stack.enter_context(self._conn.add_subscription(self))
            await self._conn.execute_command(self.subscribe_command, *self.channels)
            exit_stack.push_async_callback(self._unsubscribe)
            self._exit_stack = exit_stack.pop_all()

    async def _unsubscribe(self) -> None:
        try:
            await self._conn.execute_command(self.unsubscribe_command, *self.channels)
        except ClosedResourceError:
            pass

    async def __aenter__(self) -> Self:
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
        while True:
            if not self._receive_stream.statistics().open_receive_streams:
                await self._subscribe()
                if self.send_connection_state_changes:
                    msg = cast(
                        AnyStr,
                        RECONNECTED if self.decode else RECONNECTED.encode("ascii"),
                    )
                    return Message(CONNECTION_STATE_CHANNEL, msg)

            try:
                return await self._receive_stream.receive()
            except EndOfStream:
                # The backing connection was closed
                await self._exit_stack.__aexit__(*sys.exc_info())
                if self.send_connection_state_changes:
                    msg = cast(
                        AnyStr,
                        DISCONNECTED if self.decode else DISCONNECTED.encode("ascii"),
                    )
                    return Message(CONNECTION_STATE_CHANNEL, msg)
            except BaseException:
                await self._exit_stack.__aexit__(*sys.exc_info())
                raise

    async def aclose(self) -> None:
        await self._send_stream.aclose()

    async def deliver(self, channel: str, raw_message: bytes) -> None:
        message: Message[str] | Message[bytes]
        if self.decode:
            decoded = raw_message.decode("utf-8", errors="backslashreplace")
            message = Message(channel, decoded)
        else:
            message = Message(channel, raw_message)

        await self._send_stream.send(cast(Message[AnyStr], message))
