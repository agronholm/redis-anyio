from __future__ import annotations

import pytest

from redis_anyio import RedisClient

pytestmark = pytest.mark.anyio


async def test_get_set_delete(redis_port: int) -> None:
    async with RedisClient(port=redis_port) as client:
        await client.delete("foo")
        await client.set("foo", "bar")
        assert await client.get("foo") == b"bar"
        await client.delete("foo")
        assert await client.get("foo") is None
        assert client.statistics().total_connections == 1


async def test_rpush_llen(redis_port: int) -> None:
    async with RedisClient(port=redis_port) as client:
        await client.delete("dummy")
        await client.rpush("dummy", 6, "foo")
        assert await client.llen("dummy") == 2


async def test_flushdb(redis_port: int) -> None:
    async with RedisClient(port=redis_port) as client:
        await client.set("foo", "bar")
        await client.flushdb()
        assert await client.get("foo") is None


async def test_flushall(redis_port: int) -> None:
    async with RedisClient(port=redis_port) as client1, RedisClient(
        port=redis_port, db=1
    ) as client2:
        await client1.set("foo", "bar")
        await client2.set("bar", "baz")
        await client1.flushall()
        assert await client1.get("foo") is None
        assert await client2.get("bar") is None
