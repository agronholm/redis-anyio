from __future__ import annotations

import random
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from itertools import chain
from types import TracebackType
from typing import TYPE_CHECKING, Literal, cast, overload

from ._connection import (
    RedisConnectionPool,
    RedisConnectionPoolStatistics,
    Subscription,
)
from ._lock import RedisLock
from ._pipeline import RedisPipeline
from ._utils import as_string

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from ._resp3 import RESP3Value


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379, *, db: int = 0):
        self._pool = RedisConnectionPool(host=host, port=port, db=db)

    async def __aenter__(self) -> Self:
        await self._pool.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return await self._pool.__aexit__(exc_type, exc_val, exc_tb)

    def statistics(self) -> RedisConnectionPoolStatistics:
        """Return statistics for the connection pool."""
        return self._pool.statistics()

    async def execute_command(
        self, command: str, *args: object, decode: bool = True
    ) -> RESP3Value:
        """
        Execute a command on the Redis server.

        Argument values that are not already bytes will be first converted to strings,
        and then encoded to bytes using the UTF-8 encoding.

        :param command: the command
        :param args: arguments to be sent
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the return value of the command

        """
        return await self._pool.execute_command(command.upper(), *args, decode=decode)

    def lock(
        self,
        name: str,
        *,
        lifetime: int | timedelta = 30000,
        extend_interval: int | timedelta | None = None,
        retry_interval: int | timedelta = 1000,
    ) -> RedisLock:
        """
        Create a lock object bound to this client.

        :param name: unique name of the lock
        :param lifetime: the expiration time of the lock (in milliseconds or as
            a timedelta)
        :param extend_interval: Wait this long until extending the expiration time of
            the lock (in milliseconds or as a timedelta). If omitted, extending happens
            5 seconds before the expiration is due, or lifetime / 2 if the life time is
            under 10 seconds.
        :param retry_interval: if the lock was already acquired by another client,
            wait this long until trying to reacquire it (in milliseconds or as a
            timedelta)

        Usage::

            async with client.lock("lockname", 30000):
                ...

        Alternatively, you can share the lock object among multiple tasks::

            from anyio import create_task_group

            async def task_function(lock):
                async with lock:
                    ...

            lock = client.lock("lockname", 30000)
            async with create_task_group() as tg:
                for _ in range(5):
                    tg.start_soon(task_function, lock)

        """
        if isinstance(lifetime, timedelta):
            lifetime_ms = int(lifetime.total_seconds() * 1000)
        else:
            lifetime_ms = lifetime

        if isinstance(retry_interval, timedelta):
            retry_interval_ms = int(retry_interval.total_seconds() * 1000)
        else:
            retry_interval_ms = retry_interval

        if isinstance(extend_interval, timedelta):
            extend_interval_ms = int(extend_interval.total_seconds() * 1000)
        elif extend_interval is not None:
            extend_interval_ms = extend_interval
        elif lifetime_ms >= 10000:
            extend_interval_ms = lifetime_ms - 5000
        else:
            extend_interval_ms = int(lifetime_ms / 2)

        return RedisLock(name, lifetime_ms, extend_interval_ms, retry_interval_ms, self)

    def pipeline(self) -> RedisPipeline:
        """Create a new command pipeline bound to this client."""
        return RedisPipeline(self._pool)

    #
    # Basic key operations
    #

    async def delete(self, /, key: str, *keys: str) -> int:
        """
        Delete one or more keys in the database.

        :return: the number of keys that was removed.

        .. seealso:: `Official manual page <https://redis.io/commands/del/>`_

        """
        return cast(int, await self._pool.execute_command("DEL", key, *keys))

    @overload
    async def get(self, key: str, *, decode: Literal[False]) -> bytes | None:
        ...

    @overload
    async def get(self, key: str, *, decode: Literal[True] = ...) -> str | None:
        ...

    async def get(self, key: str, *, decode: bool = True) -> str | bytes | None:
        """
        Retrieve the value of a key.

        :param key: the key whose value to retrieve
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the value of the key, or ``None`` if the key did not exist

        .. seealso:: `Official manual page for GET <https://redis.io/commands/get/>`_

        """
        retval = await self._pool.execute_command("GET", key, decode=decode)
        assert isinstance(retval, (str, bytes)) or retval is None
        return retval

    @overload
    async def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: int | None = None,
        px: int | None = None,
        exat: int | None = None,
        pxat: int | None = None,
        keepttl: bool = False,
        decode: Literal[True] = ...,
    ) -> str | None:
        ...

    @overload
    async def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: int | None = None,
        px: int | None = None,
        exat: int | None = None,
        pxat: int | None = None,
        keepttl: bool = False,
        decode: Literal[False],
    ) -> bytes | None:
        ...

    async def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
        ex: int | None = None,
        px: int | None = None,
        exat: int | None = None,
        pxat: int | None = None,
        keepttl: bool = False,
        decode: bool = True,
    ) -> str | bytes | None:
        """
        Set ``key`` hold the string ``value``.

        If both ``nx`` and ``xx`` are ``True``, the ``nx`` setting wins.

        If more than one of the ``ex`` and ``px`` are ``exat`` and  ``pxat`` settings
        have been set, the order of preference is ``ex`` > ``px`` > ``exat`` >
        ``pxat``, so ``ex`` would win if they all were defined.

        :param key: the key to set
        :param value: the value to set for the key
        :param nx: if ``True``, only set the key if it doesn't already exist
        :param xx: if ``True``, only set the key if it already exists
        :param ex: specified expire time, in seconds
        :param px: specified expire time, in milliseconds
        :param exat: specified UNIX timestamp for expiration, in seconds
        :param pxat: specified UNIX timestamp for expiration, in milliseconds
        :param get: if ``True``, return the previous value of the key
        :param keepttl: if ``True``, retain the time to live associated with the key
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the previous value, if ``get=True``

        .. seealso:: `Official manual page for SET <https://redis.io/commands/set/>`_

        """
        extra_args: list[object] = []
        if nx:
            extra_args.append("NX")
        elif xx:
            extra_args.append("XX")

        if get:
            extra_args.append("GET")

        if ex is not None:
            extra_args.extend(["EX", ex])
        elif px is not None:
            extra_args.extend(["PX", px])
        elif exat is not None:
            extra_args.extend(["PXAT", exat])
        elif pxat is not None:
            extra_args.extend(["PXAT", pxat])

        if keepttl:
            extra_args.append("KEEPTTL")

        retval = await self._pool.execute_command(
            "SET", key, value, *extra_args, decode=decode
        )
        assert isinstance(retval, (str, bytes)) or retval is None
        return retval

    @overload
    async def mget(self, *keys: str, decode: Literal[True] = ...) -> list[str]:
        ...

    @overload
    async def mget(self, *keys: str, decode: Literal[False]) -> list[bytes]:
        ...

    async def mget(self, *keys: str, decode: bool = True) -> list[str] | list[bytes]:
        """
        Retrieve the values of multiple key.

        :param keys: the keys to retrieve
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the key values, with ``None`` as a placeholder for keys that didn't
            exist

        .. seealso:: `Official manual page for MGET <https://redis.io/commands/mget/>`_

        """
        retval = await self._pool.execute_command("MGET", *keys, decode=decode)
        assert isinstance(retval, list)
        return cast("list[str] | list[bytes]", retval)

    async def mset(self, values: Mapping[str | bytes, object]) -> None:
        """
        Set the values of multiple keys.

        :param values: a mapping of keys to their values

        .. seealso:: `Official manual page for MSET <https://redis.io/commands/mset/>`_

        """
        await self._pool.execute_command("MSET", *chain.from_iterable(values.items()))

    async def pexpire(
        self,
        key: str,
        milliseconds: int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> int:
        """
        Set the timeout of the given key (in milliseconds).

        :return: 1 if the timeout was set, 0 if the key doesn't exist or the operation
            was skipped due to the operation modified (``how``)

        .. seealso::
            `Official manual pagefor PEXPIRE <https://redis.io/commands/pexpire/>`_

        """
        args: list[object] = []
        if how is not None:
            args.append(how.upper())

        retval = await self._pool.execute_command("PEXPIRE", key, milliseconds, *args)
        assert isinstance(retval, int)
        return retval

    async def pexpireat(
        self,
        key: str,
        timestamp: datetime | int,
        *,
        how: Literal["nx", "xx", "gt", "lt"] | None = None,
    ) -> int:
        """
        Set the expiration time of the given key as a UNIX timestamp (in milliseconds).

        :return: 1 if the timeout was set, 0 if the key doesn't exist or the operation
            was skipped due to the operation modified (``how``)

        .. seealso::
            `Official manual page for PEXPIREAT <https://redis.io/commands/pexpireat/>`_

        """
        args: list[object] = []
        if isinstance(timestamp, datetime):
            args.append(timestamp.timestamp())
        else:
            args.append(timestamp)

        if how is not None:
            args.append(how.upper())

        retval = await self._pool.execute_command("PEXPIREAT", key, *args)
        assert isinstance(retval, int)
        return retval

    async def pexpiretime(self, key: str) -> int:
        """
        Return the expiration time of the given key as a UNIX timestamp (in
        milliseconds).

        :return: the expiration time as an UNIX timestamp (in milliseconds), or -1 if
            the key exists but has no associated expiration time, or -2 if the key
            doesn't exist

        .. seealso::
            `Official manual page for EXPIRETIME
            <https://redis.io/commands/pexpiretime/>`_

        """
        retval = await self._pool.execute_command("PEXPIRETIME", key)
        assert isinstance(retval, int)
        return retval

    async def pttl(self, key: str) -> int:
        """
        Return the remaining time to live of a key, in milliseconds.

        :return: the time to live, or -1 if the key exists but has no associated
            expiration time, or -2 if the key doesn't exist

        .. seealso:: `Official manual page for PTTL <https://redis.io/commands/pttl/>`_

        """
        retval = await self._pool.execute_command("PTTL", key)
        assert isinstance(retval, int)
        return retval

    @asynccontextmanager
    async def scan(
        self, *, match: str | None = None, count: int | None, type_: str | None = None
    ) -> AsyncGenerator[AsyncIterator[str], None]:
        """
        Iterate over the set of keys in the current database.

        :param str|None match: glob-style pattern to use for matching against keys
        :param int|None count: maximum number of items to fetch on each iteration
        :param str|None type_: type of keys to match
        :return: an async context manager yielding an async iterator yielding keys

        Usage::

            async with client.scan(match="patter*") as iterator:
                async for key in iterator:
                    print(f"Found key: {key}")

        .. seealso:: `Official manual page for SCAN <https://redis.io/commands/scan/>`_
        """

        async def iterate_keys(retval: RESP3Value) -> AsyncGenerator[str, None]:
            while True:
                assert isinstance(retval, list) and len(retval) == 2
                cursor, items = retval
                assert isinstance(items, list)
                for item in items:
                    assert isinstance(item, str)
                    yield item

                if cursor == "0":
                    break

                retval = await conn.execute_command("SCAN", cursor, *args)

        args: list[object] = []
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", count])
        if type_ is not None:
            args.extend(["TYPE", type_.upper()])

        async with self._pool.acquire() as conn:
            retval_ = await conn.execute_command("SCAN", 0, *args)
            iterator = iterate_keys(retval_)
            try:
                yield iterator
            finally:
                await iterator.aclose()

    async def time(self) -> tuple[int, int]:
        """
        Return the current server time.

        :return: a tuple of (UNIX timestamp in seconds, microseconds elapsed in the
            current second)

        .. seealso:: `Official manual page for TIME <https://redis.io/commands/time/>`_

        """
        retval = await self._pool.execute_command("TIME")
        assert isinstance(retval, list) and len(retval) == 2
        return int(cast(bytes, retval[0])), int(cast(bytes, retval[1]))

    #
    # List operations
    #

    @overload
    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        timeout: float = 0,
        decode: Literal[True] = ...,
    ) -> str:
        ...

    @overload
    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        timeout: float = 0,
        decode: Literal[False],
    ) -> bytes:
        ...

    @overload
    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        timeout: float = 0,
        decode: bool,
    ) -> str | bytes:
        ...

    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        timeout: float = 0,
        decode: bool = True,
    ) -> str | bytes:
        """
        Atomically move an element from one array to another.

        Unlike :meth:`lmove`, this method first waits for an element to appear on
        ``source`` if it's currently empty.

        :param source: the source key
        :param destination: the destination key
        :param wherefrom: either ``left`` or ``right``
        :param whereto: either ``left`` or ``right``
        :param timeout: seconds to wait for an element to appear on ``source``; 0 to
            wait indefinitely
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the element being popped from ``source`` and moved to ``destination``,
            or ``None`` if the timeout was reached

        .. seealso::
            `Official manual page for BLMOVE <https://redis.io/commands/blmove/>`_

        """
        retval = await self._pool.execute_command(
            "BLMOVE", source, destination, wherefrom, whereto, timeout, decode=decode
        )
        assert isinstance(retval, (str, bytes))
        return retval

    @overload
    async def blmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        timeout: float = 0,
        decode: Literal[True] = ...,
    ) -> tuple[str, list[bytes]]:
        ...

    @overload
    async def blmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        timeout: float = 0,
        decode: Literal[False],
    ) -> tuple[str, list[str]]:
        ...

    @overload
    async def blmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        timeout: float = 0,
        decode: bool,
    ) -> tuple[str, list[str] | list[bytes]]:
        ...

    async def blmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        timeout: float = 0,
        decode: bool = True,
    ) -> tuple[str, list[str] | list[bytes]]:
        """
        Remove and return one or more elements from one of the given lists.

        Unlike :meth:`lmpop`, this method first waits for an element to appear on
        any of the lists if all of them are empty.

        :param wherefrom: ``left`` to remove an element from the beginning of the list,
            ``right`` to remove one from the end
        :param keys: the lists to remove elements from
        :param count: the maximum number of elements to remove (omit
        :param timeout: seconds to wait for an element to appear on ``source``; 0 to
            wait indefinitely
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: a tuple of (key, list of removed elements)

        .. seealso::
            `Official manual page for BLMPOP <https://redis.io/commands/blmpop/>`_

        """
        args: list[object] = []
        if count is not None:
            args.extend(["COUNT", count])

        retval = await self._pool.execute_command(
            "BLMPOP", timeout, len(keys), *keys, wherefrom.upper(), *args, decode=decode
        )
        assert isinstance(retval, list)
        assert isinstance(retval[1], list)
        return as_string(retval[0]), cast("list[str] | list[bytes]", retval[1])

    @overload
    async def blpop(
        self, *keys: str, timeout: float = 0, decode: Literal[True] = ...
    ) -> tuple[str, str] | tuple[None, None]:
        ...

    @overload
    async def blpop(
        self, *keys: str, timeout: float = 0, decode: Literal[False]
    ) -> tuple[str, bytes] | tuple[None, None]:
        ...

    @overload
    async def blpop(
        self, *keys: str, timeout: float = 0, decode: bool
    ) -> tuple[str, str | bytes] | tuple[None, None]:
        ...

    async def blpop(
        self, *keys: str, timeout: float = 0, decode: bool = True
    ) -> tuple[str, str | bytes] | tuple[None, None]:
        """
        Remove and return the first element from one of the given lists.

        Unlike :meth:`lpop`, this method first waits for an element to appear on
        any of the lists if all of them are empty.

        :param keys: the lists to pop from
        :param timeout: seconds to wait for an element to appear on any of the lists; 0
            to wait indefinitely
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: a tuple of (list name, the removed element), or tuple of
            ``(None, None)`` if the timeout was reached

        .. seealso::
            `Official manual page for BLPOP <https://redis.io/commands/blpop/>`_

        """
        retval = await self._pool.execute_command(
            "BLPOP", *keys, timeout, decode=decode
        )
        assert isinstance(retval, list) and len(retval) == 2
        if retval[0] is None:
            return None, None

        assert isinstance(retval[1], (str, bytes))
        return as_string(retval[0]), retval[1]

    @overload
    async def brpop(
        self, *keys: str, timeout: float = 0, decode: Literal[True] = ...
    ) -> tuple[str, str] | tuple[None, None]:
        ...

    @overload
    async def brpop(
        self, *keys: str, timeout: float = 0, decode: Literal[False]
    ) -> tuple[str, bytes] | tuple[None, None]:
        ...

    @overload
    async def brpop(
        self, *keys: str, timeout: float = 0, decode: bool
    ) -> tuple[str, str | bytes] | tuple[None, None]:
        ...

    async def brpop(
        self, *keys: str, timeout: float = 0, decode: bool = True
    ) -> tuple[str, str | bytes] | tuple[None, None]:
        """
        Remove and return the last element from one of the given lists.

        Unlike :meth:`rpop`, this method first waits for an element to appear on
        any of the lists if all of them are empty.

        :param keys: the lists to pop from
        :param timeout: seconds to wait for an element to appear on any of the lists; 0
            to wait indefinitely
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: a tuple of (list name, the removed element), or tuple of
            ``(None, None)`` if the timeout was reached

        .. seealso::
            `Official manual page for BRPOP <https://redis.io/commands/brpop/>`_

        """
        retval = await self._pool.execute_command(
            "BRPOP", *keys, timeout, decode=decode
        )
        assert isinstance(retval, list) and len(retval) == 2
        if retval[0] is None:
            return None, None

        assert isinstance(retval[1], (str, bytes))
        return as_string(retval[0]), retval[1]

    @overload
    async def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        decode: Literal[True] = ...,
    ) -> str | None:
        ...

    @overload
    async def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        decode: Literal[False],
    ) -> bytes | None:
        ...

    @overload
    async def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        decode: bool,
    ) -> str | bytes | None:
        ...

    async def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal["left", "right"],
        whereto: Literal["left", "right"],
        *,
        decode: bool = True,
    ) -> str | bytes | None:
        """
        Atomically move an element from one array to another.

        :param source: the source key
        :param destination: the destination key
        :param wherefrom: either ``left`` or ``right``
        :param whereto: either ``left`` or ``right``
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the element being popped from ``source`` and moved to ``destination``,
            or ``None`` if ``source`` was empty

        .. seealso::
            `Official manual page for LMOVE <https://redis.io/commands/lmove/>`_

        """
        retval = await self._pool.execute_command(
            "LMOVE", source, destination, wherefrom, whereto, decode=decode
        )
        assert isinstance(retval, (str, bytes)) or retval is None
        return retval

    @overload
    async def lindex(self, key: str, index: int, *, decode: Literal[True] = ...) -> str:
        ...

    @overload
    async def lindex(self, key: str, index: int, *, decode: Literal[False]) -> bytes:
        ...

    @overload
    async def lindex(self, key: str, index: int, *, decode: bool) -> str | bytes:
        ...

    async def lindex(self, key: str, index: int, *, decode: bool = True) -> str | bytes:
        """
        Return the element at index ``index`` in key ``key``.

        :param key: the list to get the element from
        :param index: numeric index on the list
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the element at the specified index

        .. seealso::
            `Official manual page for LINDEX <https://redis.io/commands/lindex/>`_

        """
        retval = await self._pool.execute_command("LINDEX", key, index, decode=decode)
        assert isinstance(retval, (str, bytes))
        return retval

    async def linsert(
        self,
        key: str,
        where: Literal["before", "after"],
        pivot: object,
        element: object,
    ) -> int:
        """
        Insert ``element`` to the list ``key`` either before or after ``pivot``.

        :param key: the list to get the element from
        :param where: ``before`` to insert the element before the reference value,
            ``after`` to insert the element after the reference value
        :param pivot: the reference value to look for
        :param element: the element to be inserted
        :return: the length of the list after a successful operation; 0 if the key
            doesn't exist, and -1 when the pivot wasn't found

        .. seealso::
            `Official manual page for LINSERT <https://redis.io/commands/linsert/>`_

        """
        retval = await self._pool.execute_command(
            "LINSERT", key, where.upper(), pivot, element
        )
        assert isinstance(retval, int)
        return retval

    async def llen(self, key: str) -> int:
        """
        Remove and return the first element(s) value of a key.

        :param key: the array whose length to measure
        :return: the length of the array

        .. seealso:: `Official manual page for LLEN <https://redis.io/commands/llen/>`_

        """
        retval = await self._pool.execute_command("LLEN", key)
        assert isinstance(retval, int)
        return retval

    @overload
    async def lmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        decode: Literal[True] = ...,
    ) -> tuple[str, list[str]] | None:
        ...

    @overload
    async def lmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        decode: Literal[False],
    ) -> tuple[str, list[bytes]] | None:
        ...

    @overload
    async def lmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        decode: bool,
    ) -> tuple[str, list[str] | list[bytes]] | None:
        ...

    async def lmpop(
        self,
        wherefrom: Literal["left", "right"],
        *keys: str,
        count: int | None = None,
        decode: bool = True,
    ) -> tuple[str, list[str] | list[bytes]] | None:
        """
        Remove and return one or more elements from one of the given lists.

        :param wherefrom: ``left`` to remove an element from the beginning of the list,
            ``right`` to remove one from the end
        :param keys: the lists to remove elements from
        :param count: the maximum number of elements to remove (omit
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: a tuple of (key, list of elements), or ``None`` if no elements were
            removed

        .. seealso::
            `Official manual page for LMPOP <https://redis.io/commands/lmpop/>`_

        """
        args: list[object] = []
        if count is not None:
            args.extend(["COUNT", count])

        retval = await self._pool.execute_command(
            "LMPOP", len(keys), *keys, wherefrom.upper(), *args, decode=decode
        )
        if retval is None:
            return None

        assert isinstance(retval, list)
        assert isinstance(retval[1], list)
        return as_string(retval[0]), cast("list[str] | list[bytes]", retval[1])

    @overload
    async def lpop(
        self, key: str, count: int = 1, *, decode: Literal[True] = ...
    ) -> list[str] | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: int = 1, *, decode: Literal[False]
    ) -> list[bytes] | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: int = 1, *, decode: bool
    ) -> list[str] | list[bytes] | None:
        ...

    async def lpop(
        self, key: str, count: int = 1, *, decode: bool = True
    ) -> list[str] | list[bytes] | None:
        """
        Remove and return the first element(s) from a list.

        :param key: the list to remove elements from
        :param count: the number of elements to remove
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the list of removed elements, or ``None`` when no element could be
            popped.

        .. seealso:: `Official manual page for LPOP <https://redis.io/commands/lpop/>`_

        """
        retval = await self._pool.execute_command("LPOP", key, count, decode=decode)
        if retval is None:
            return None

        assert isinstance(retval, list)
        return cast("list[str] | list[bytes]", retval)

    @overload
    async def rpop(
        self, key: str, count: int = 1, *, decode: Literal[True] = ...
    ) -> list[str]:
        ...

    @overload
    async def rpop(
        self, key: str, count: int = 1, *, decode: Literal[False]
    ) -> list[bytes]:
        ...

    async def rpop(
        self, key: str, count: int = 1, *, decode: bool = True
    ) -> list[str] | list[bytes]:
        """
        Remove and return the last element(s) value of a key.

        :param key: the array to pop elements from
        :param count: the number of elements to pop
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the list of popped elements

        .. seealso:: `Official manual page for RPOP <https://redis.io/commands/rpop/>`_

        """
        retval = await self._pool.execute_command("RPOP", key, count, decode=decode)
        assert isinstance(retval, list)
        return cast("list[str] | list[bytes]", retval)

    async def rpush(self, key: str, *values: object) -> int:
        """
        Insert the given values to the tail of the list stored at ``key``.

        :param key: the array to insert elements to
        :param values: the values to insert
        :return: the length of the list after the operation

        .. seealso::
            `Official manual page for RPUSH <https://redis.io/commands/rpush/>`_

        """
        retval = await self._pool.execute_command("RPUSH", key, *values)
        assert isinstance(retval, int)
        return retval

    async def rpushx(self, key: str, *values: object) -> int:
        """
        Insert the given values to the tail of the list stored at ``key``.

        Unlike :meth:`rpush`, this variant only inserts the values if ``key`` already
        exists and is a list.

        :param key: the array to insert elements to
        :param values: the values to insert
        :return: the length of the list after the operation

        .. seealso::
            `Official manual page for RPUSHX <https://redis.io/commands/rpush/>`_

        """
        retval = await self._pool.execute_command("RPUSHX", key, *values)
        assert isinstance(retval, int)
        return retval

    #
    # String operations
    #

    async def getrange(
        self, key: str, start: int, end: int, decode: bool = True
    ) -> str | bytes:
        """
        Return a substring of the string value stored at ``key``.

        :param key: the key to retrieve
        :param start: index of the starting character (inclusive)
        :param end: index of the last character (inclusive)
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the substring

        .. seealso::
            `Official manual page for GETRANGE <https://redis.io/commands/getrange/>`_

        """
        return cast(
            str,
            await self._pool.execute_command(
                "GETRANGE", key, start, end, decode=decode
            ),
        )

    async def setrange(
        self, key: str, offset: int, value: str, decode: bool = True
    ) -> int:
        """
        Overwrite part of the specific string key.

        :param key: the key to modify
        :param offset: offset (in bytes) where to place the replacement string
        :param value: the string to place at the offset
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the length of the string after the modification

        .. warning:: Take care when modifying multibyte (outside of the ASCII range)
            strings, as each character may require more than one byte.

        .. seealso::
            `Official manual page for SETRANGE <https://redis.io/commands/setrange/>`_

        """
        retval = await self._pool.execute_command(
            "SETRANGE", key, offset, value, decode=decode
        )
        assert isinstance(retval, int)
        return retval

    #
    # Hash map operations
    #

    async def hdel(self, key: str, field: str) -> int:
        """
        Delete a field in the given hash map.

        :param key: the hash map
        :param field: the fields to delete
        :return: 1 if the hash map contained the given field, or 0 if either the field
            or the hash map did not exist

        .. seealso::
            `Official manual page for HDEL <https://redis.io/commands/hdel/>`_

        """
        retval = await self._pool.execute_command("HDEL", key, field)
        assert isinstance(retval, int)
        return retval

    @overload
    async def hget(
        self, key: str, field: str, decode: Literal[True] = ...
    ) -> str | None:
        ...

    @overload
    async def hget(self, key: str, field: str, decode: Literal[False]) -> bytes | None:
        ...

    async def hget(
        self, key: str, field: str, decode: bool = True
    ) -> str | bytes | None:
        """
        Retrieve the value of a field in a hash map stored at ``key``.

        :param key: the hash to retrieve fields from
        :param field: the field to retrieve
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the key values, with ``None`` as a placeholder for fields that didn't
            exist

        .. seealso:: `Official manual page for HGET <https://redis.io/commands/hget/>`_

        """
        retval = await self._pool.execute_command("HGET", key, field, decode=decode)
        assert isinstance(retval, (str, bytes)) or retval is None
        return retval

    @overload
    async def hgetall(self, key: str, decode: Literal[True] = ...) -> dict[str, str]:
        ...

    @overload
    async def hgetall(self, key: str, decode: Literal[False]) -> dict[bytes, bytes]:
        ...

    @overload
    async def hgetall(
        self, key: str, decode: bool
    ) -> dict[str, str] | dict[bytes, bytes]:
        ...

    async def hgetall(
        self, key: str, decode: bool = True
    ) -> dict[str, str] | dict[bytes, bytes]:
        """
        Retrieve all fields and their values from a hash map stored at ``key``.

        :param key: the hash map
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: a mapping of fields to their values, or an empty dict if the hash map
            did not exist

        .. seealso::
            `Official manual page for HGETALL <https://redis.io/commands/hgetall/>`_

        """
        retval = await self._pool.execute_command("HGETALL", key, decode=decode)
        assert isinstance(retval, dict)
        return cast("dict[str, str] | dict[bytes, bytes]", retval)

    @overload
    async def hmget(
        self, key: str, *fields: str, decode: Literal[True] = ...
    ) -> list[str | None] | list[bytes | None]:
        ...

    @overload
    async def hmget(
        self, key: str, *fields: str, decode: Literal[False]
    ) -> list[str | None] | list[bytes | None]:
        ...

    async def hmget(
        self, key: str, *fields: str, decode: bool = True
    ) -> list[str | None] | list[bytes | None]:
        """
        Retrieve the values of multiple fields in a hash stored at ``key``.

        :param key: the hash to retrieve fields from
        :param fields: the fields to retrieve
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the key values, with ``None`` as a placeholder for fields that didn't
            exist

        .. seealso::
            `Official manual page for HMGET <https://redis.io/commands/hmget/>`_

        """
        retval = await self._pool.execute_command("HMGET", key, *fields, decode=decode)
        assert isinstance(retval, list)
        return cast("list[str | None] | list[bytes | None]", retval)

    async def hset(self, key: str, values: Mapping[str | bytes, object]) -> int:
        """
        Set the specified field values in the given hash map.

        :param key: the hash map to set the values in
        :param values: a mapping of field name to value
        :return: the number of fields that were added

        Usage::

            await client.hset("somekey", {"key1": value1, "key2": value2})

        .. seealso:: `Official manual page for HSET <https://redis.io/commands/hset/>`_

        """
        retval = await self._pool.execute_command(
            "HSET", key, *chain.from_iterable(values.items())
        )
        assert isinstance(retval, int)
        return retval

    #
    # Miscellaneous operations
    #

    async def flushall(self, sync: bool = True) -> None:
        """
        Clears all keys in all databases.

        :param sync: if ``True``, flush the databases synchronously;
            if ``False``, flush them asynchronously.

        .. seealso::
            `Official manual page for FLUSHALL <https://redis.io/commands/flushall/>`_

        """
        mode = "SYNC" if sync else "ASYNC"
        await self._pool.execute_command("FLUSHALL", mode)

    async def flushdb(self, sync: bool = True) -> None:
        """
        Clears all keys in the currently selected database.

        :param sync: if ``True``, flush the database synchronously;
            if ``False``, flush it asynchronously.

        .. seealso::
            `Official manual page for FLUSHDB <https://redis.io/commands/flushdb/>`_

        """
        mode = "SYNC" if sync else "ASYNC"
        await self._pool.execute_command("FLUSHDB", mode)

    async def ping(self) -> None:
        nonce = str(random.randint(0, 100000))
        retval = await self._pool.execute_command("PING", nonce)
        if retval != nonce:
            raise RuntimeError(
                f"PING command returned an unexpected payload (got {retval!r}, "
                f"expected {nonce!r}"
            )

    #
    # Script operations
    #

    async def eval(
        self, script: str, keys: list[str], args: list[object]
    ) -> RESP3Value:
        """
        Run the given Lua script and return its result.

        :param script: the source code of the script
        :param keys: the list of keys used in the script
        :param args: the list of arguments passed to the script
        :return: the return value of the script

        .. seealso::
            `Official manual page for EVAL <https://redis.io/commands/eval/>`_

        """
        return await self.execute_command("EVAL", script, len(keys), *keys, *args)

    async def evalsha(
        self, sha1: str, keys: list[str], args: list[object]
    ) -> RESP3Value:
        """
        Run a previously stored Lua script and return its result.

        :param sha1: hex digest of a SHA-1 hash made from the script
        :param keys: the list of keys used in the script
        :param args: the list of arguments passed to the script
        :return: the return value of the script

        .. seealso::
            `Official manual page for EVALSHA <https://redis.io/commands/evalsha/>`_

        """
        return await self.execute_command("EVALSHA", sha1, len(keys), *keys, *args)

    async def script_load(self, script: str) -> str:
        """
        Load the given Lua script into the scripts cache (without executing it).

        :param script: the source code of the script
        :return: the SHA-1 hex digest of the script

        .. seealso::
            `Official manual page for 'SCRIPT LOAD'
            <https://redis.io/commands/script-load/>`_

        """
        retval = await self.execute_command("SCRIPT", "LOAD", script)
        assert isinstance(retval, str)
        return retval

    async def script_flush(self, sync: bool = True) -> None:
        """
        Flush the Lua script cache.

        :param sync: if ``True``, flush the scripts synchronously;
            if ``False``, flush them asynchronously.

        :return: the SHA-1 hex digest of the script

        .. seealso::
            `Official manual page for 'SCRIPT LOAD'
            <https://redis.io/commands/script-load/>`_

        """
        mode = "SYNC" if sync else "ASYNC"
        await self.execute_command("SCRIPT", "FLUSH", mode)

    #
    # Publish/Subscribe operations
    #

    async def publish(self, channel: str, message: str | bytes) -> int:
        """
        Publish a message to the given channel.

        :param channel: name of the channel to publish to
        :param message: the message to publish
        :return: the number of clients that received the message

        .. seealso::
            `Official manual page for PUBLISH <https://redis.io/commands/publish/>`_

        """
        retval = await self.execute_command("PUBLISH", channel, message)
        assert isinstance(retval, int)
        return retval

    async def spublish(self, shardchannel: str, message: str | bytes) -> int:
        """
        Publish a message to the given shard channel.

        :param shardchannel: name of the shard channel to publish to
        :param message: the message to publish
        :return: the number of clients that received the message

        .. seealso::
            `Official manual page for SPUBLISH <https://redis.io/commands/spublish/>`_

        """
        retval = await self.execute_command("SPUBLISH", shardchannel, message)
        assert isinstance(retval, int)
        return retval

    @overload
    def subscribe(
        self, *channels: str, decode: Literal[True] = ...
    ) -> Subscription[str]:
        ...

    @overload
    def subscribe(self, *channels: str, decode: Literal[False]) -> Subscription[bytes]:
        ...

    @overload
    def subscribe(
        self, *channels: str, decode: bool
    ) -> Subscription[str] | Subscription[bytes]:
        ...

    def subscribe(
        self, *channels: str, decode: bool = True
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more channels.

        :param decode: if ``True``, decode the messages into strings using the
            UTF-8 encoding. If ``False``, yield raw bytes instead.

        Usage::

            async with client.subscribe("channel1", "channel2") as subscription:
                async for channel, data in subscription:
                    ...  # Received data on <channel>

        .. seealso::
            `Official manual page for SUBSCRIBE <https://redis.io/commands/subscribe/>`_

        """
        args = (
            self._pool,
            channels,
            "subscribe",
            "message",
            decode,
            "SUBSCRIBE",
            "UNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)

    @overload
    def ssubscribe(
        self, *shardchannels: str, decode: Literal[True] = ...
    ) -> Subscription[str]:
        ...

    @overload
    def ssubscribe(
        self, *shardchannels: str, decode: Literal[False]
    ) -> Subscription[bytes]:
        ...

    @overload
    def ssubscribe(
        self, *shardchannels: str, decode: bool
    ) -> Subscription[str] | Subscription[bytes]:
        ...

    def ssubscribe(
        self, *shardchannels: str, decode: bool = True
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more shard channels.

        :param decode: if ``True``, decode the messages into strings using the
            UTF-8 encoding. If ``False``, yield raw bytes instead.

        Usage::

            async with client.ssubscribe("channel1", "channel2") as subscription:
                async for channel, data in subscription:
                    ...  # Received data on <channel>

        .. seealso::
            `Official manual page for SSUBSCRIBE
            <https://redis.io/commands/ssubscribe/>`_

        """
        args = (
            self._pool,
            shardchannels,
            "ssubscribe",
            "smessage",
            decode,
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)

    def psubscribe(
        self, *patterns: str, decode: bool = True
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more topic patterns.

        :param decode: if ``True``, decode the messages into strings using the
            UTF-8 encoding. If ``False``, yield raw bytes instead.

        Usage::

            async with client.psubscribe("chann*", "ch[aie]n?el") as subscription:
                async for topic, data in subscription:
                    ...  # Received data on <topic>

        .. seealso::
            `Official manual page for PSUBSCRIBE
            <https://redis.io/commands/psubscribe/>`_

        """
        args = (
            self._pool,
            patterns,
            "psubscribe",
            "pmessage",
            decode,
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)
