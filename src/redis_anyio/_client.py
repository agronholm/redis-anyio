from __future__ import annotations

import random
import sys
from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timedelta
from itertools import chain
from ssl import Purpose, SSLContext, create_default_context
from types import TracebackType
from typing import Any, Literal, cast, overload

from tenacity import (
    stop_never,
    wait_exponential,
)
from tenacity.stop import stop_base
from tenacity.wait import wait_base

from ._connection import (
    RedisConnectionPool,
    RedisConnectionPoolStatistics,
)
from ._lock import RedisLock
from ._pipeline import RedisPipeline, RedisTransaction
from ._subscription import Subscription
from ._types import ResponseValue
from ._utils import (
    as_milliseconds,
    as_seconds,
    as_string,
    as_unix_timestamp,
    as_unix_timestamp_ms,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RedisClient:
    """ """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        *,
        db: int = 0,
        ssl: bool | SSLContext = False,
        username: str | None = None,
        password: str | None = None,
        pool_size: int = 64,
        pool_overflow: int = 2048,
        timeout: float = 30,
        connect_timeout: float = 10,
        retry_wait: wait_base = wait_exponential(max=5),
        retry_stop: stop_base = stop_never,
    ):
        """
        :param host: the host name of the Redis server
        :param port: the port number of the Redis server
        :param db: the database number to select
        :param ssl: ``True`` to use a default SSL context to connect to the server,
            or a custom SSL context to use to make an SSL connection; ``False`` to not
            use SSL at all
        :param username: user name to authenticate with
        :param password: password to authenticate with
        :param pool_size: the maximum allowed number of server connections to keep in
            the connection pool on standby
        :param pool_overflow: the maximum number of disposable server connections to
            allow this client to form with the server (this is _in addition to_
            ``pool_size``; these connections will be dropped when not used any more)
        :param timeout: timeout (in seconds) for read/write operations
        :param connect_timeout: time (in seconds) to wait for a connect operation to
            succeed
        :param retry_wait: specifies how to wait before the next retry on a connection
            failure
        :param retry_stop: specifies when to stop retrying

        """
        ssl_context: SSLContext | None = None
        if ssl is True:
            ssl_context = create_default_context(Purpose.SERVER_AUTH)

        self._pool = RedisConnectionPool(
            host=host,
            port=port,
            db=db,
            ssl_context=ssl_context,
            username=username,
            password=password,
            size=pool_size,
            overflow=pool_overflow,
            timeout=timeout,
            connect_timeout=connect_timeout,
            retry_wait=retry_wait,
            retry_stop=retry_stop,
        )

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
    ) -> ResponseValue:
        """
        Execute a command on the Redis server.

        Argument values that are not already bytes will be first converted to strings,
        and then encoded to bytes using the UTF-8 encoding.

        :param command: the command
        :param args: arguments to be sent
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the return value of the command
        :raises ConnectivityError: if there's a connectivity problem (can't connect to
            the server, connection prematurely closed, etc.)
        :raises ResponseError: if the server returns an error response

        """
        async with self._pool.acquire() as conn:
            return await conn.execute_command(command, *args, decode=decode)

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
        """
        Create a new command pipeline bound to this client.

        .. seealso:: `Redis pipelining <https://redis.io/docs/manual/pipelining/>`

        """
        return RedisPipeline(self._pool)

    def transaction(self) -> RedisTransaction:
        """
        Create a new transaction bound to this client.

        A transaction is a pipeline with slightly different semantics. See the
        documentation of the :class:`RedisTransaction` class for further details.

        .. seealso::
            `How transactions work in Redis
            <https://redis.io/docs/manual/transactions/>`

        """
        return RedisTransaction(self._pool)

    #
    # Basic key operations
    #

    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys in the database.

        :return: the number of keys that was removed.

        .. seealso:: `Official manual page for DEL <https://redis.io/commands/del/>`_

        """
        return cast(int, await self.execute_command("DEL", *keys))

    @overload
    async def get(self, key: str, *, decode: Literal[False]) -> bytes | None:
        ...

    @overload
    async def get(self, key: str, *, decode: Literal[True] = ...) -> str | None:
        ...

    @overload
    async def get(self, key: str, *, decode: bool) -> str | bytes | None:
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
        retval = await self.execute_command("GET", key, decode=decode)
        assert isinstance(retval, (str, bytes)) or retval is None
        return retval

    async def keys(self, pattern: str) -> list[str]:
        """
        Return the keys in the database matching the given pattern.

        :param pattern: the pattern to match against
        :return: the list of keys in the database that match the pattern

        .. seealso::
            `Official manual page for KEYS <https://redis.io/commands/keys/>`_

        """
        retval = await self.execute_command("KEYS", pattern)
        assert isinstance(retval, list)
        return cast("list[str]", retval)

    @overload
    async def set(
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
    ) -> str | None:
        ...

    @overload
    async def set(
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
    ) -> bytes | None:
        ...

    @overload
    async def set(
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
    ) -> str | bytes | None:
        ...

    async def set(
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
            extra_args.extend(["EX", as_seconds(ex)])
        elif px is not None:
            extra_args.extend(["PX", as_milliseconds(px)])
        elif exat is not None:
            extra_args.extend(["EXAT", as_unix_timestamp(exat)])
        elif pxat is not None:
            extra_args.extend(["PXAT", as_unix_timestamp_ms(pxat)])

        if keepttl:
            extra_args.append("KEEPTTL")

        retval = await self.execute_command(
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

    @overload
    async def mget(self, *keys: str, decode: bool) -> list[str] | list[bytes]:
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
        retval = await self.execute_command("MGET", *keys, decode=decode)
        assert isinstance(retval, list)
        return cast("list[str] | list[bytes]", retval)

    async def mset(self, values: Mapping[str | bytes, object]) -> None:
        """
        Set the values of multiple keys.

        :param values: a mapping of keys to their values

        .. seealso:: `Official manual page for MSET <https://redis.io/commands/mset/>`_

        """
        await self.execute_command("MSET", *chain.from_iterable(values.items()))

    async def expiretime(self, key: str) -> int:
        """
        Return the expiration time of the given key as a UNIX timestamp (in seconds).

        :return: the expiration time as an UNIX timestamp (in seconds), or -1 if
            the key exists but has no associated expiration time, or -2 if the key
            doesn't exist

        .. seealso::
            `Official manual page for EXPIRETIME
            <https://redis.io/commands/expiretime/>`_

        """
        retval = await self.execute_command("EXPIRETIME", key)
        assert isinstance(retval, int)
        return retval

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

        retval = await self.execute_command("PEXPIRE", key, milliseconds, *args)
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

        retval = await self.execute_command("PEXPIREAT", key, *args)
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
            `Official manual page for PEXPIRETIME
            <https://redis.io/commands/pexpiretime/>`_

        """
        retval = await self.execute_command("PEXPIRETIME", key)
        assert isinstance(retval, int)
        return retval

    async def pttl(self, key: str) -> int:
        """
        Return the remaining time to live of a key, in milliseconds.

        :return: the time to live, or -1 if the key exists but has no associated
            expiration time, or -2 if the key doesn't exist

        .. seealso:: `Official manual page for PTTL <https://redis.io/commands/pttl/>`_

        """
        retval = await self.execute_command("PTTL", key)
        assert isinstance(retval, int)
        return retval

    async def scan_iter(
        self,
        *,
        match: str | None = None,
        count: int | None = None,
        type_: str | None = None,
    ) -> AsyncIterator[str]:
        """
        Iterate over the set of keys in the current database.

        :param match: glob-style pattern to use for matching against keys
        :param count: maximum number of items to fetch on each iteration
        :param type_: type of keys to match
        :return: an async context manager yielding an async iterator yielding keys

        Usage::

            async for key in client.scan(match="patter*"):
                print(f"Found key: {key}")

        .. seealso:: `Official manual page for SCAN <https://redis.io/commands/scan/>`_

        """
        args: list[object] = []
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", count])
        if type_ is not None:
            args.extend(["TYPE", type_.upper()])

        cursor: str | None = None
        while cursor != "0":
            retval = await self.execute_command("SCAN", cursor or "0", *args)
            assert isinstance(retval, list) and len(retval) == 2
            cursor = cast(str, retval[0])
            items = retval[1]
            assert isinstance(items, list)
            for item in items:
                yield cast(str, item)

    async def time(self) -> tuple[int, int]:
        """
        Return the current server time.

        :return: a tuple of (UNIX timestamp in seconds, microseconds elapsed in the
            current second)

        .. seealso:: `Official manual page for TIME <https://redis.io/commands/time/>`_

        """
        retval = await self.execute_command("TIME")
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
        retval = await self.execute_command(
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

        retval = await self.execute_command(
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
        retval = await self.execute_command("BLPOP", *keys, timeout, decode=decode)
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
        retval = await self.execute_command("BRPOP", *keys, timeout, decode=decode)
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
        retval = await self.execute_command(
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
        retval = await self.execute_command("LINDEX", key, index, decode=decode)
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
        retval = await self.execute_command(
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
        retval = await self.execute_command("LLEN", key)
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

        retval = await self.execute_command(
            "LMPOP", len(keys), *keys, wherefrom.upper(), *args, decode=decode
        )
        if retval is None:
            return None

        assert isinstance(retval, list)
        assert isinstance(retval[1], list)
        return as_string(retval[0]), cast("list[str] | list[bytes]", retval[1])

    @overload
    async def lpop(
        self, key: str, count: None = ..., *, decode: Literal[True] = ...
    ) -> str | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: None = ..., *, decode: Literal[False]
    ) -> bytes | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: None = ..., *, decode: bool
    ) -> str | bytes | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: int, *, decode: Literal[True] = ...
    ) -> list[str] | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: int, *, decode: Literal[False]
    ) -> list[bytes] | None:
        ...

    @overload
    async def lpop(
        self, key: str, count: int, *, decode: bool
    ) -> list[str] | list[bytes] | None:
        ...

    async def lpop(
        self, key: str, count: int | None = None, *, decode: bool = True
    ) -> str | bytes | list[str] | list[bytes] | None:
        """
        Remove and return the first element(s) from a list.

        :param key: the list to remove elements from
        :param count: the number of elements to remove (omit to return the first
            element)
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return:
            * a list of removed elements (if ``count`` was specified)
            * the removed element (if ``count`` was omitted)
            * ``None`` when no element could be popped.

        .. seealso:: `Official manual page for LPOP <https://redis.io/commands/lpop/>`_

        """
        args: list[object] = []
        if count is not None:
            args.append(count)

        retval = await self.execute_command("LPOP", key, *args, decode=decode)
        if retval is None:
            return None

        if isinstance(retval, list):
            return cast("list[str] | list[bytes]", retval)

        return cast("str | bytes", retval)

    @overload
    async def rpop(
        self, key: str, count: None = ..., *, decode: Literal[True] = ...
    ) -> str | None:
        ...

    @overload
    async def rpop(
        self, key: str, count: None = ..., *, decode: Literal[False]
    ) -> bytes | None:
        ...

    @overload
    async def rpop(
        self, key: str, count: None = ..., *, decode: bool
    ) -> str | bytes | None:
        ...

    @overload
    async def rpop(
        self, key: str, count: int, *, decode: Literal[True] = ...
    ) -> list[str] | None:
        ...

    @overload
    async def rpop(
        self, key: str, count: int, *, decode: Literal[False]
    ) -> list[bytes] | None:
        ...

    @overload
    async def rpop(
        self, key: str, count: int, *, decode: bool
    ) -> list[str] | list[bytes] | None:
        ...

    async def rpop(
        self, key: str, count: int | None = None, *, decode: bool = True
    ) -> str | bytes | list[str] | list[bytes] | None:
        """
        Remove and return the last element(s) from a list.

        :param key: the list to remove elements from
        :param count: the number of elements to remove (omit to return the last
            element)
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return:
            * a list of removed elements (if ``count`` was specified)
            * the removed element (if ``count`` was omitted)
            * ``None`` when no element could be popped.

        .. seealso:: `Official manual page for RPOP <https://redis.io/commands/rpop/>`_

        """
        args: list[object] = []
        if count is not None:
            args.append(count)

        retval = await self.execute_command("RPOP", key, *args, decode=decode)
        if retval is None:
            return None

        if isinstance(retval, list):
            return cast("list[str] | list[bytes]", retval)

        return cast("str | bytes", retval)

    async def rpush(self, key: str, *values: object) -> int:
        """
        Insert the given values to the tail of the list stored at ``key``.

        :param key: the array to insert elements to
        :param values: the values to insert
        :return: the length of the list after the operation

        .. seealso::
            `Official manual page for RPUSH <https://redis.io/commands/rpush/>`_

        """
        retval = await self.execute_command("RPUSH", key, *values)
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
        retval = await self.execute_command("RPUSHX", key, *values)
        assert isinstance(retval, int)
        return retval

    #
    # String operations
    #

    @overload
    async def getrange(
        self, key: str, start: int, end: int, decode: Literal[True] = ...
    ) -> str:
        ...

    @overload
    async def getrange(
        self, key: str, start: int, end: int, decode: Literal[False]
    ) -> bytes:
        ...

    @overload
    async def getrange(
        self, key: str, start: int, end: int, decode: bool
    ) -> str | bytes:
        ...

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
            await self.execute_command("GETRANGE", key, start, end, decode=decode),
        )

    async def setrange(self, key: str, offset: int, value: str) -> int:
        """
        Overwrite part of the specific string key.

        :param key: the key to modify
        :param offset: offset (in bytes) where to place the replacement string
        :param value: the string to place at the offset
        :return: the length of the string after the modification

        .. warning:: Take care when modifying multibyte (outside of the ASCII range)
            strings, as each character may require more than one byte.

        .. seealso::
            `Official manual page for SETRANGE <https://redis.io/commands/setrange/>`_

        """
        retval = await self.execute_command("SETRANGE", key, offset, value)
        assert isinstance(retval, int)
        return retval

    #
    # Hash map operations
    #

    async def hdel(self, key: str, field: str) -> int:
        """
        Delete a field in the given hash map.

        :param key: the hash map
        :param field: the field to delete
        :return: 1 if the hash map contained the given field, or 0 if either the field
            or the hash map did not exist

        .. seealso::
            `Official manual page for HDEL <https://redis.io/commands/hdel/>`_

        """
        retval = await self.execute_command("HDEL", key, field)
        assert isinstance(retval, int)
        return retval

    async def hexists(self, key: str, field: str) -> bool:
        """
        Check if the given field exists in the given hash map.

        :param key: the hash map
        :param field: the field to check
        :return: ``True`` if the hash map contains the given field, ``False`` if not

        .. seealso::
            `Official manual page for HEXISTS <https://redis.io/commands/hexists/>`_

        """
        retval = await self.execute_command("HEXISTS", key, field)
        return bool(retval)

    @overload
    async def hget(
        self, key: str, field: str, decode: Literal[True] = ...
    ) -> str | None:
        ...

    @overload
    async def hget(self, key: str, field: str, decode: Literal[False]) -> bytes | None:
        ...

    @overload
    async def hget(self, key: str, field: str, decode: bool) -> str | bytes | None:
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
        retval = await self.execute_command("HGET", key, field, decode=decode)
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
        retval = await self.execute_command("HGETALL", key, decode=decode)
        assert isinstance(retval, dict)
        return cast("dict[str, str] | dict[bytes, bytes]", retval)

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        """
        Increment the value of the specified field in the given hash map.

        If the field does not exist already, it will be created, with ``increment`` as
        the value.

        :param key: the hash map
        :param field: the name of the field to increment
        :param increment: the value to increment the field by
        :return: the value of the field after the increment operation

        .. seealso::
            `Official manual page for HINCRBY <https://redis.io/commands/hincrby/>`_

        """
        retval = await self.execute_command("HINCRBY", key, field, increment)
        assert isinstance(retval, int)
        return retval

    async def hincrbyfloat(self, key: str, field: str, increment: float) -> str:
        """
        Increment the value of the specified field in the given hash map by a floating
        point value.

        If the field does not exist already, it will be created, with ``increment`` as
        the value.

        :param key: the hash map
        :param field: the name of the field to increment
        :param increment: the (float) value to increment the field by
        :return: the value of the field after the increment operation, as a string

        .. seealso::
            `Official manual page for HINCRBYFLOAT
            <https://redis.io/commands/hincrbyfloat/>`_

        """
        retval = await self.execute_command("HINCRBYFLOAT", key, field, increment)
        assert isinstance(retval, str)
        return retval

    async def hkeys(self, key: str) -> list[str]:
        """
        Return the keys present in the given hash map.

        :param key: the hash map
        :return: the list of keys in the hash map, or an empty list if the hash map
            doesn't exist

        .. seealso::
            `Official manual page for HKEYS <https://redis.io/commands/hkeys/>`_

        """
        retval = await self.execute_command("HKEYS", key)
        assert isinstance(retval, list)
        return cast("list[str]", retval)

    async def hlen(self, key: str) -> int:
        """
        Return the number of keys present in the given hash map.

        :param key: the hash map
        :return: the number of keys in the hash map, or 0 if the hash map doesn't exist

        .. seealso::
            `Official manual page for HLEN <https://redis.io/commands/hlen/>`_

        """
        retval = await self.execute_command("HLEN", key)
        assert isinstance(retval, int)
        return retval

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

    @overload
    async def hmget(
        self, key: str, *fields: str, decode: bool
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
        retval = await self.execute_command("HMGET", key, *fields, decode=decode)
        assert isinstance(retval, list)
        return cast("list[str | None] | list[bytes | None]", retval)

    @overload
    async def hrandfield(
        self,
        key: str,
        count: int | None = ...,
        *,
        withvalues: bool = ...,
        decode: Literal[True] = ...,
    ) -> str | list[str] | Mapping[str, str] | None:
        ...

    @overload
    async def hrandfield(
        self,
        key: str,
        count: int | None = ...,
        *,
        withvalues: bool = ...,
        decode: Literal[False],
    ) -> bytes | list[str] | Mapping[str, bytes] | None:
        ...

    @overload
    async def hrandfield(
        self, key: str, count: int | None = ..., *, withvalues: bool = ..., decode: bool
    ) -> str | bytes | list[str] | Mapping[str, str] | Mapping[str, bytes] | None:
        ...

    async def hrandfield(
        self,
        key: str,
        count: int | None = None,
        *,
        withvalues: bool = False,
        decode: bool = True,
    ) -> str | bytes | list[str] | list[bytes] | Mapping[str, str] | Mapping[
        str, bytes
    ] | None:
        """
        Retrieve the value(s) of one or more random fields in the given hash map.

        :param key: the hash map
        :param count: if given, the number of fields to fetch (can be negative; see the
            official documentation for details)
        :param withvalues: if ``True``, return a mapping of random fields to their
            values
        :param decode: ``True`` to decode byte strings in field values to strings,
            ``False`` to leave them as is (applied only when ``count`` is specified and
            ``withvalues`` is ``True``)
        :return:
            * ``None`` if the hash map didn't exist
            * If no ``count`` was specified: the value of a random field
            * If no ``count`` was specified, if ``count`` was not given; otherwise, a
              list of the fetched values

        .. seealso::
            `Official manual page for HRANDFIELD
            <https://redis.io/commands/hrandfield/>`_

        """
        args: list[object] = []
        if count is not None:
            args.append(count)

        if withvalues:
            args.append("WITHVALUES")

        retval = await self.execute_command("HRANDFIELD", key, *args, decode=False)
        if retval is None:
            return None

        if isinstance(retval, list):
            if withvalues:
                keyvalues = cast("list[list[bytes]]", retval)
                if decode:
                    return {
                        as_string(item[0]): as_string(item[1]) for item in keyvalues
                    }
                else:
                    return {as_string(item[0]): item[1] for item in keyvalues}
            else:
                keylist = cast("list[bytes]", retval)
                return [
                    key.decode("utf-8", errors="backslashreplace") for key in keylist
                ]

        assert isinstance(retval, bytes)
        if decode:
            return retval.decode("utf-8", errors="backslashreplace")
        else:
            return retval

    @overload
    def hscan_iter(
        self,
        key: str,
        *,
        match: str | None = None,
        count: int | None,
        decode: Literal[True] = ...,
    ) -> AsyncIterator[tuple[str, str]]:
        ...

    @overload
    def hscan_iter(
        self,
        key: str,
        *,
        match: str | None = None,
        count: int | None,
        decode: Literal[False],
    ) -> AsyncIterator[tuple[str, bytes]]:
        ...

    @overload
    def hscan_iter(
        self,
        key: str,
        *,
        match: str | None = None,
        count: int | None,
        decode: bool,
    ) -> AsyncIterator[tuple[str, str]] | AsyncIterator[tuple[str, bytes]]:
        ...

    async def hscan_iter(
        self,
        key: str,
        *,
        match: str | None = None,
        count: int | None,
        decode: bool = True,
    ) -> (AsyncIterator[tuple[str, str]] | AsyncIterator[tuple[str, bytes]]):
        """
        Iterate over the fields in the given hash map.

        :param key: the hash map
        :param match: glob-style pattern to use for matching field names
        :param count: maximum number of items to fetch on each iteration
        :param decode: ``True`` to decode byte strings in field values to strings,
            ``False`` to leave them as is
        :return: an async context manager yielding an async iterator yielding tuples of
            (field name, field value)

        Usage::

            async for field, value in client.hscan("hashmapname", match="patter*"):
                print(f"Found field: {field} = {value}")

        .. seealso::
            `Official manual page for HSCAN <https://redis.io/commands/hscan/>`_

        """
        args: list[object] = []
        if match is not None:
            args.extend(["MATCH", match])
        if count is not None:
            args.extend(["COUNT", count])

        cursor: bytes | None = None
        while cursor != b"0":
            retval = await self.execute_command(
                "HSCAN", key, cursor or "0", *args, decode=False
            )
            assert isinstance(retval, list) and len(retval) == 2
            cursor = cast(bytes, retval[0])
            items = retval[1]
            assert isinstance(items, list)
            fields = [as_string(f) for f in items[::2]]
            values: list[Any] = (
                [as_string(v) for v in items[1::2]] if decode else items[1::2]
            )
            for field_, value in zip(fields, values):
                yield field_, value

    async def hset(self, key: str, values: Mapping[str, object]) -> int:
        """
        Set the specified field values in the given hash map.

        :param key: the hash map to set the values in
        :param values: a mapping of field name to value
        :return: the number of fields that were added

        Usage::

            await client.hset("somekey", {"key1": value1, "key2": value2})

        .. seealso:: `Official manual page for HSET <https://redis.io/commands/hset/>`_

        """
        retval = await self.execute_command(
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
        await self.execute_command("FLUSHALL", mode)

    async def flushdb(self, sync: bool = True) -> None:
        """
        Clears all keys in the currently selected database.

        :param sync: if ``True``, flush the database synchronously;
            if ``False``, flush it asynchronously.

        .. seealso::
            `Official manual page for FLUSHDB <https://redis.io/commands/flushdb/>`_

        """
        mode = "SYNC" if sync else "ASYNC"
        await self.execute_command("FLUSHDB", mode)

    async def ping(self) -> None:
        nonce = str(random.randint(0, 100000))
        retval = await self.execute_command("PING", nonce)
        if retval != nonce:
            raise RuntimeError(
                f"PING command returned an unexpected payload (got {retval!r}, "
                f"expected {nonce!r}"
            )

    #
    # Script operations
    #

    async def eval(
        self, script: str, keys: list[str], args: list[object], *, decode: bool = True
    ) -> ResponseValue:
        """
        Run the given Lua script and return its result.

        :param script: the source code of the script
        :param keys: the list of keys used in the script
        :param args: the list of arguments passed to the script
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the return value of the script

        .. seealso::
            `Official manual page for EVAL <https://redis.io/commands/eval/>`_

        """
        return await self.execute_command(
            "EVAL", script, len(keys), *keys, *args, decode=decode
        )

    async def evalsha(
        self, sha1: str, keys: list[str], args: list[object], *, decode: bool = True
    ) -> ResponseValue:
        """
        Run a previously stored Lua script and return its result.

        :param sha1: hex digest of a SHA-1 hash made from the script
        :param keys: the list of keys used in the script
        :param args: the list of arguments passed to the script
        :param decode: ``True`` to decode byte strings in the response to strings,
            ``False`` to leave them as is
        :return: the return value of the script

        .. seealso::
            `Official manual page for EVALSHA <https://redis.io/commands/evalsha/>`_

        """
        return await self.execute_command(
            "EVALSHA", sha1, len(keys), *keys, *args, decode=decode
        )

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
        self,
        *channels: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[True] = ...,
    ) -> Subscription[str]:
        ...

    @overload
    def subscribe(
        self,
        *channels: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[False],
    ) -> Subscription[bytes]:
        ...

    @overload
    def subscribe(
        self, *channels: str, send_connection_state_changes: bool = ..., decode: bool
    ) -> Subscription[str] | Subscription[bytes]:
        ...

    def subscribe(
        self,
        *channels: str,
        send_connection_state_changes: bool = False,
        decode: bool = True,
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more channels.

        :param send_connection_state_changes: if ``True``, send messages about
            disconnects and reconnections in a specially named topic within the stream
            (see the publish/subscribe section of the documentation for further info)
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
            send_connection_state_changes,
            decode,
            "SUBSCRIBE",
            "UNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)

    @overload
    def ssubscribe(
        self,
        *shardchannels: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[True] = ...,
    ) -> Subscription[str]:
        ...

    @overload
    def ssubscribe(
        self,
        *shardchannels: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[False],
    ) -> Subscription[bytes]:
        ...

    @overload
    def ssubscribe(
        self,
        *shardchannels: str,
        send_connection_state_changes: bool = ...,
        decode: bool,
    ) -> Subscription[str] | Subscription[bytes]:
        ...

    def ssubscribe(
        self,
        *shardchannels: str,
        send_connection_state_changes: bool = False,
        decode: bool = True,
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more shard channels.

        :param send_connection_state_changes: if ``True``, send messages about
            disconnects and reconnections in a specially named topic within the stream
            (see the publish/subscribe section of the documentation for further info)
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
            send_connection_state_changes,
            decode,
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)

    @overload
    def psubscribe(
        self,
        *patterns: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[True] = ...,
    ) -> Subscription[str]:
        ...

    @overload
    def psubscribe(
        self,
        *patterns: str,
        send_connection_state_changes: bool = ...,
        decode: Literal[False],
    ) -> Subscription[bytes]:
        ...

    @overload
    def psubscribe(
        self, *patterns: str, send_connection_state_changes: bool = ..., decode: bool
    ) -> Subscription[str] | Subscription[bytes]:
        ...

    def psubscribe(
        self,
        *patterns: str,
        send_connection_state_changes: bool = False,
        decode: bool = True,
    ) -> Subscription[str] | Subscription[bytes]:
        """
        Subscribe to one or more topic patterns.

        :param send_connection_state_changes: if ``True``, send messages about
            disconnects and reconnections in a specially named topic within the stream
            (see the publish/subscribe section of the documentation for further info)
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
            send_connection_state_changes,
            decode,
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
        )
        return Subscription[str](*args) if decode else Subscription[bytes](*args)
