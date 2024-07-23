from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import chain
from typing import Any, Generic, Literal, TypeVar, overload

from ._connection import RedisConnectionPool
from ._exceptions import ResponseError
from ._utils import as_milliseconds, as_seconds, as_unix_timestamp, as_unix_timestamp_ms

T = TypeVar("T")


@dataclass
class QueuedCommand(Generic[T]):
    """
    Represents a command queued in a pipeline or transaction.

    .. attribute:: result
        The result of the command, once the pipeline or transaction has been executed.
    """

    command: str
    args: tuple[object, ...]
    decode: bool
    _result: T | ResponseError = field(init=False, repr=False)

    def result(self) -> T:
        if isinstance(self._result, ResponseError):
            raise self._result

        return self._result


@dataclass(frozen=True, eq=False, repr=False)
class RedisPipeline:
    pool: RedisConnectionPool
    _queued_commands: list[QueuedCommand[Any]] = field(init=False, default_factory=list)

    async def execute(self) -> None:
        """Execute the commands queued thus far."""
        async with self.pool.acquire() as conn:
            for command in self._queued_commands:
                await conn.send_command(command.command, *command.args)

            for command in self._queued_commands:
                command._result = await conn.read_next_response(decode=command.decode)

    def queue_command(
        self, command: str, *args: object, decode: bool = True
    ) -> QueuedCommand[Any]:
        """
        Queue an arbitrary command to be executed.

        .. seealso:: :meth:`RedisClient.execute_command`

        """
        cmd = QueuedCommand[Any](command, args, decode)
        self._queued_commands.append(cmd)
        return cmd

    def delete(self, *keys: str) -> QueuedCommand[int]:
        """
        Queue a DELETE command.

        .. seealso:: :meth:`RedisClient.delete`

        """
        return self.queue_command("DELETE", *keys)

    @overload
    def get(self, key: str, *, decode: Literal[True] = ...) -> QueuedCommand[str]: ...

    @overload
    def get(self, key: str, *, decode: Literal[False]) -> QueuedCommand[bytes]: ...

    @overload
    def get(
        self, key: str, *, decode: bool
    ) -> QueuedCommand[str] | QueuedCommand[bytes]: ...

    def get(
        self, key: str, *, decode: bool = True
    ) -> QueuedCommand[str] | QueuedCommand[bytes]:
        """
        Queue a GET command.

        .. seealso:: :meth:`RedisClient.set`

        """
        return self.queue_command("GET", key, decode=decode)

    def keys(self, pattern: str) -> QueuedCommand[list[str]]:
        """
        Queue a KEYS command.

        .. seealso:: :meth:`RedisClient.keys`

        """
        return self.queue_command("KEYS", pattern)

    @overload
    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool = ...,
        xx: bool = ...,
        get: bool = ...,
        ex: int | timedelta | None = ...,
        px: int | timedelta | None = ...,
        exat: int | datetime | None = ...,
        pxat: int | datetime | None = ...,
        keepttl: bool = ...,
        decode: Literal[True] = ...,
    ) -> QueuedCommand[str | None]: ...

    @overload
    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool = ...,
        xx: bool = ...,
        get: bool = ...,
        ex: int | timedelta | None = ...,
        px: int | timedelta | None = ...,
        exat: int | datetime | None = ...,
        pxat: int | datetime | None = ...,
        keepttl: bool = ...,
        decode: Literal[False],
    ) -> QueuedCommand[bytes | None]: ...

    @overload
    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool = ...,
        xx: bool = ...,
        get: bool = ...,
        ex: int | timedelta | None = ...,
        px: int | timedelta | None = ...,
        exat: int | datetime | None = ...,
        pxat: int | datetime | None = ...,
        keepttl: bool = ...,
        decode: bool,
    ) -> QueuedCommand[str | None] | QueuedCommand[bytes | None]: ...

    def set(
        self,
        key: str,
        value: object,
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
    ) -> QueuedCommand[str | None] | QueuedCommand[bytes | None]:
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

        return self.queue_command("SET", key, value, *extra_args, decode=decode)

    def hset(
        self, key: str, values: Mapping[str | bytes, object]
    ) -> QueuedCommand[int]:
        """
        Queue an HSET command.

        .. seealso:: :meth:`RedisClient.hset`

        """
        return self.queue_command("HSET", key, *chain.from_iterable(values.items()))

    def flushall(self, sync: bool = True) -> QueuedCommand[str]:
        """
        Queue an FLUSHALL command.

        .. seealso:: :meth:`RedisClient.flushall`

        """
        mode = "SYNC" if sync else "ASYNC"
        return self.queue_command("FLUSHALL", mode)

    def flushdb(self, sync: bool = True) -> QueuedCommand[str]:
        """
        Queue an FLUSHDB command.

        .. seealso:: :meth:`RedisClient.flushall`

        """
        mode = "SYNC" if sync else "ASYNC"
        return self.queue_command("FLUSHDB", mode)

    def pexpire(
        self,
        key: str,
        milliseconds: int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> QueuedCommand[int]:
        """
        Queue a PEXPIRE command.

        .. seealso:: :meth:`RedisClient.pexpire`

        """
        extra_args: list[object] = []
        if how is not None:
            extra_args.append(how.upper())

        return self.queue_command("PEXPIRE", key, milliseconds, *extra_args)

    def pexpireat(
        self,
        key: str,
        timestamp: datetime | int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> QueuedCommand[int]:
        """
        Queue a PEXPIREAT command.

        .. seealso:: :meth:`RedisClient.pexpireat`

        """
        args: list[object] = []
        if isinstance(timestamp, datetime):
            args.append(timestamp.timestamp())
        else:
            args.append(timestamp)

        if how is not None:
            args.append(how.upper())

        return self.queue_command("PEXPIREAT", key, *args)

    def pexpiretime(self, key: str) -> QueuedCommand[int]:
        """
        Queue a PEXPIRETIME command.

        .. seealso:: :meth:`RedisClient.pexpiretime`

        """
        return self.queue_command("PEXPIRETIME", key)

    def pttl(self, key: str) -> QueuedCommand[int]:
        """
        Queue a PTTL command.

        .. seealso:: :meth:`RedisClient.pttl`

        """
        return self.queue_command("PTTL", key)


class RedisTransaction(RedisPipeline):
    """
    Represents a Redis transaction.

    This differs from :class:`RedisPipeline` in the following ways:

    * If any of the commands fails to be queued, the transaction is discarded
        (using ``DISCARD``) and the response error is raised
    * The commands in the pipeline are wrapped with ``MULTI`` and ``EXEC`` commands
    """

    async def execute(self) -> None:
        """
        Execute the commands queued thus far in a transaction.

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

            results = await conn.execute_command("EXEC")
            assert isinstance(results, list)
            for command, result in zip(self._queued_commands, results):
                command._result = result
