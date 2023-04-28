from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from typing import TYPE_CHECKING, Literal

from ._connection import RedisConnectionPool
from ._resp3 import serialize_command

if TYPE_CHECKING:
    from ._resp3 import RESP3BlobError, RESP3SimpleError, RESP3Value


@dataclass
class RedisPipeline:
    pool: RedisConnectionPool
    _queued_commands: list[bytes] = field(init=False, default_factory=list)

    async def execute(self) -> list[RESP3Value | RESP3BlobError | RESP3SimpleError]:
        return await self.pool.execute_pipeline(self._queued_commands)

    def _queue_command(self, command: str, *args: object) -> None:
        self._queued_commands.append(serialize_command(command, *args))

    def hset(self, key: str, values: Mapping[str | bytes, object]) -> None:
        self._queue_command("HSET", key, *chain.from_iterable(values.items()))

    def pexpire(
        self,
        key: str,
        milliseconds: int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> None:
        extra_args: list[object] = []
        if how is not None:
            extra_args.append(how.upper())

        self._queue_command("PEXPIRE", key, milliseconds, *extra_args)

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

        self._queue_command("PEXPIREAT", key, *args)

    def pexpiretime(self, key: str) -> None:
        self._queue_command("PEXPIRETIME", key)

    def pttl(self, key: str) -> None:
        self._queue_command("PTTL", key)
