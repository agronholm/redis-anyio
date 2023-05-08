from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import chain
from typing import Literal

from ._connection import RedisConnectionPool
from ._exceptions import ResponseError
from ._types import ResponseValue
from ._utils import as_milliseconds, as_seconds, as_unix_timestamp, as_unix_timestamp_ms


@dataclass(frozen=True)
class QueuedCommand:
    command: str
    args: tuple[object, ...]
    decode: bool


@dataclass(frozen=True, eq=False, repr=False)
class RedisPipeline:
    pool: RedisConnectionPool
    _queued_commands: list[QueuedCommand] = field(init=False, default_factory=list)

    async def execute(self) -> Sequence[ResponseValue | ResponseError]:
        """
        Execute the commands queued thus far.

        :return: a sequence of result values (or response errors) corresponding to the
            number of queued commands

        """
        async with self.pool.acquire() as conn:
            for command in self._queued_commands:
                await conn.send_command(command.command, *command.args)

            return [
                await conn.read_next_response(decode=command.decode)
                for command in self._queued_commands
            ]

    def queue_command(self, command: str, *args: object, decode: bool = True) -> None:
        """
        Queue an arbitrary command to be executed.

        .. seealso:: :meth:`RedisClient.execute_command`

        """
        self._queued_commands.append(QueuedCommand(command, args, decode))

    def get(self, key: str, *, decode: bool = True) -> None:
        """
        Queue a GET command.

        .. seealso:: :meth:`RedisClient.set`

        """
        self.queue_command("GET", key, decode=decode)

    def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: int | timedelta | None = None,
        px: int | timedelta | None = None,
        exat: int | datetime | None = None,
        pxat: int | datetime | None = None,
        keepttl: bool = False,
        decode: bool = True,
    ) -> None:
        """
        Queue a SET command.

        .. seealso:: :meth:`RedisClient.set`

        """
        extra_args: list[object] = []
        if nx:
            extra_args.append("NX")
        elif xx:
            extra_args.append("XX")

        if get:
            extra_args.append("GET")

        if ex is not None:
            extra_args.extend(["EX", as_seconds(ex)])
        elif px is not None:
            extra_args.extend(["PX", as_milliseconds(px)])
        elif exat is not None:
            extra_args.extend(["PXAT", as_unix_timestamp(exat)])
        elif pxat is not None:
            extra_args.extend(["PXAT", as_unix_timestamp_ms(pxat)])

        if keepttl:
            extra_args.append("KEEPTTL")

        self.queue_command("SET", key, value, *extra_args, decode=decode)

    def hset(self, key: str, values: Mapping[str | bytes, object]) -> None:
        """
        Queue an HSET command.

        .. seealso:: :meth:`RedisClient.hset`

        """
        self.queue_command("HSET", key, *chain.from_iterable(values.items()))

    def pexpire(
        self,
        key: str,
        milliseconds: int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> None:
        """
        Queue a PEXPIRE command.

        .. seealso:: :meth:`RedisClient.pexpire`

        """
        extra_args: list[object] = []
        if how is not None:
            extra_args.append(how.upper())

        self.queue_command("PEXPIRE", key, milliseconds, *extra_args)

    def pexpireat(
        self,
        key: str,
        timestamp: datetime | int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> None:
        args: list[object] = []
        if isinstance(timestamp, datetime):
            args.append(timestamp.timestamp())
        else:
            args.append(timestamp)

        if how is not None:
            args.append(how.upper())

        self.queue_command("PEXPIREAT", key, *args)

    def pexpiretime(self, key: str) -> None:
        self.queue_command("PEXPIRETIME", key)

    def pttl(self, key: str) -> None:
        self.queue_command("PTTL", key)


class RedisTransaction(RedisPipeline):
    """
    Represents a Redis transaction.

    This differs from :class:`RedisPipeline` in the following ways:

    * If any of the commands fails to be queued, the transaction is discarded
        (using ``DISCARD``) and the response error is raised
    * The commands in the pipeline are wrapped with ``MULTI`` and ``EXEC`` commands
    """

    async def execute(self) -> Sequence[ResponseValue | ResponseError]:
        """
        Execute the commands queued thus far in a transaction.

        :return: a sequence of result values (or response errors) corresponding to the
            number of queued commands (except for the implied ``MULTI`` and ``EXEC``
            commands)
        :raises ResponseError: if the server returns an error response when queuing one
            of the commands

        """
        async with self.pool.acquire() as conn:
            await conn.send_command("MULTI")
            for command in self._queued_commands:
                await conn.send_command(command.command, *command.args)

            for _ in range(len(self._queued_commands) + 1):
                response = await conn.read_next_response(decode=False)
                if isinstance(response, ResponseError):
                    await conn.execute_command("DISCARD")
                    raise response

            exec_response = await conn.execute_command("EXEC")
            assert isinstance(exec_response, list)
            return exec_response
