from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from itertools import chain
from typing import TYPE_CHECKING, Literal

from ._exceptions import ResponseError
from ._resp3 import serialize_command
from ._types import ResponseValue

if TYPE_CHECKING:
    from ._client import RedisClient


@dataclass(frozen=True)
class RedisPipeline:
    client: RedisClient
    transaction: bool
    queued_commands: list[bytes] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if self.transaction:
            self._queue_command("MULTI")

    async def execute(self) -> Sequence[ResponseValue | ResponseError] | None:
        """
        Execute the pipeline.

        :return: a sequence of result values (or response errors) corresponding to the
            number of queued commands

        """
        if self.transaction:
            self._queue_command("EXEC")

        return await self.client.execute_pipeline(self)

    def _queue_command(self, command: str, *args: object) -> None:
        self.queued_commands.append(serialize_command(command, *args))

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
