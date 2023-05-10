from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass, field
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import uuid4

from anyio import Event, Lock, create_task_group, move_on_after, sleep
from anyio.abc import TaskGroup, TaskStatus

from ._exceptions import RedisError, ResponseError
from ._types import ResponseValue

if TYPE_CHECKING:
    from ._client import RedisClient

ACQUIRE_SCRIPT = """\
    local token = redis.call("get", KEYS[1])
    if not token then
        redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
        return 1
    elseif token == ARGV[1] and redis.call("pexpire", KEYS[1], ARGV[2]) > 0 then
        return 1
    else
        return 0
    end
"""
ACQUIRE_SCRIPT_SHA = hashlib.new("sha1", ACQUIRE_SCRIPT.encode("ascii")).hexdigest()

EXTEND_SCRIPT = """\
    local token = redis.call("get", KEYS[1])
    if token == ARGV[1] and redis.call("pexpire", KEYS[1], ARGV[2]) > 0 then
        return 1
    else
        return 0
    end
"""
EXTEND_SCRIPT_SHA = hashlib.new("sha1", EXTEND_SCRIPT.encode("ascii")).hexdigest()

RELEASE_SCRIPT = """\
    if redis.call("get", KEYS[1]) == ARGV[1] then
        redis.call("del", KEYS[1])
    end
"""
RELEASE_SCRIPT_SHA = hashlib.new("sha1", RELEASE_SCRIPT.encode("ascii")).hexdigest()


class LostLockError(RedisError):
    """Raised when we failed to extend our lease on the Redis lock."""


@dataclass
class RedisLock:
    name: str
    lifetime: int = field(compare=False, hash=False)
    extend_interval: int = field(compare=False, hash=False)
    retry_interval: int = field(compare=False, hash=False)
    client: RedisClient = field(compare=False, hash=False)
    _token: bytes = field(
        init=False,
        compare=False,
        hash=False,
        default_factory=lambda: uuid4().hex.encode("ascii"),
    )
    _local_lock: Lock = field(
        init=False, compare=False, hash=False, default_factory=Lock
    )
    _task_group: TaskGroup = field(init=False, compare=False, hash=False)
    _stop_event: Event = field(init=False, compare=False, hash=False)

    async def _run_script(
        self, source: str, script_sha1: str, *args: object
    ) -> ResponseValue:
        try:
            return await self.client.evalsha(script_sha1, [self.name], list(args))
        except ResponseError as exc:
            if exc.code != "NOSCRIPT":
                raise

            await self.client.script_load(source)
            return await self.client.evalsha(script_sha1, [self.name], list(args))

    async def _run(self, *, task_status: TaskStatus) -> None:
        while not await self._run_script(
            ACQUIRE_SCRIPT, ACQUIRE_SCRIPT_SHA, self._token, self.lifetime
        ):
            await sleep(self.retry_interval / 1000)

        task_status.started()
        try:
            while not self._stop_event.is_set():
                with move_on_after(self.extend_interval):
                    await self._stop_event.wait()

                if not await self._run_script(
                    EXTEND_SCRIPT, EXTEND_SCRIPT_SHA, self._token, self.lifetime
                ):
                    raise LostLockError
        finally:
            with move_on_after(self.retry_interval, shield=True):
                await self._run_script(RELEASE_SCRIPT, RELEASE_SCRIPT_SHA, self._token)

    async def __aenter__(self) -> None:
        await self._local_lock.__aenter__()

        # Start a background task that continuously extends the expiration time of the
        # lock
        self._stop_event = Event()
        self._task_group = create_task_group()
        await self._task_group.__aenter__()
        try:
            await self._task_group.start(self._run)
        except BaseException:
            await self._task_group.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            self._stop_event.set()
            await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            del self._task_group, self._stop_event
            await self._local_lock.__aexit__(exc_type, exc_val, exc_tb)
