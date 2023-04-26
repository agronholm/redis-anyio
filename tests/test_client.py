from __future__ import annotations

from typing import cast

import pytest
from _pytest.fixtures import SubRequest
from anyio import create_task_group, fail_after

from redis_anyio import RedisClient

pytestmark = pytest.mark.anyio


@pytest.fixture(
    params=[pytest.param(True, id="string"), pytest.param(False, id="bytes")]
)
def decode(request: SubRequest) -> bool:
    return cast(bool, request.param)


async def test_statistics(redis_port: int) -> None:
    async with RedisClient(port=redis_port) as client:
        assert client.statistics().max_connections == 65535
        assert client.statistics().total_connections == 0
        assert client.statistics().idle_connections == 0
        assert client.statistics().busy_connections == 0

        for _ in range(3):
            await client.ping()

        assert client.statistics().max_connections == 65535
        assert client.statistics().total_connections == 1
        assert client.statistics().idle_connections == 1
        assert client.statistics().busy_connections == 0

        async with client.subscribe("foo"), client.subscribe("bar"):
            assert client.statistics().max_connections == 65535
            assert client.statistics().total_connections == 2
            assert client.statistics().idle_connections == 0
            assert client.statistics().busy_connections == 2

    assert client.statistics().max_connections == 65535
    assert client.statistics().total_connections == 0
    assert client.statistics().idle_connections == 0
    assert client.statistics().busy_connections == 0


class TestBasicKeyOperations:
    async def test_get_set_delete(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar")
            assert await client.get("foo") == "bar" if decode else b"bar"
            await client.delete("foo")
            assert await client.get("foo") is None

    async def test_mget_mset(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo", "bar")
            await client.mset({"foo": 1, "bar": 2})
            assert (
                await client.mget("foo", "bar") == ["1", "2"]
                if decode
                else [b"1", b"2"]
            )


class TestListOperations:
    async def test_rpush_llen(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.rpush("dummy", 6, "foo")
            assert await client.llen("dummy") == 2


class TestHashMapOperations:
    async def test_hset_hget_hmget_hdelete(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"key1": 8, "key2": "foo"})
            assert await client.hget("dummy", "key1") == "8"
            assert await client.hget("dummy", "key2") == "foo"
            assert await client.hget("dummy", "key1", decode=False) == b"8"
            assert await client.hget("dummy", "key2", decode=False) == b"foo"
            assert (
                await client.hmget("dummy", "key1", "key2") == ["8", "foo"]
                if decode
                else [b"8", b"foo"]
            )
            assert await client.hdel("dummy", "key1") == 1
            assert await client.hget("dummy", "key1") is None


class TestMiscellaneousOperations:
    async def test_flushdb(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.set("foo", "bar")
            await client.flushdb()
            assert await client.get("foo") is None

    async def test_flushall(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client1, RedisClient(
            port=redis_port, db=1
        ) as client2:
            await client1.set("foo", "bar")
            await client2.set("bar", "baz")
            await client1.flushall()
            assert await client1.get("foo") is None
            assert await client2.get("bar") is None

    async def test_ping(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.ping()


class TestPublishSubscribe:
    async def test_subscribe(self, redis_port: int) -> None:
        async def publish_messages() -> None:
            await client.publish("channel1", "Hello")
            await client.publish("channel2", "World!")
            await client.publish("channel1", "åäö")

        async with RedisClient(port=redis_port) as client:
            async with client.subscribe(
                "channel1", "channel2"
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    messages = [await subscription.__anext__() for _ in range(3)]

        assert messages == [
            ("channel1", b"Hello"),
            ("channel2", b"World!"),
            ("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
        ]

    async def test_ssubscribe(self) -> None:
        async def publish_messages() -> None:
            await client.spublish("channel1", "Hello")
            await client.spublish("channel2", "World!")
            await client.spublish("channel1", "åäö")

        async with RedisClient(port=6380) as client:
            async with client.ssubscribe(
                "channel1", "channel2"
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    messages = [await subscription.__anext__() for _ in range(3)]

        assert messages == [
            ("channel1", b"Hello"),
            ("channel2", b"World!"),
            ("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
        ]

    async def test_psubscribe(self, redis_port: int) -> None:
        async def publish_messages() -> None:
            await client.publish("channel1", "Hello")
            await client.publish("channel2", "World!")
            await client.publish("channel1", "åäö")

        messages = []
        async with RedisClient(port=redis_port) as client:
            async with client.psubscribe(
                "channel?"
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    messages = [await subscription.__anext__() for _ in range(3)]

        assert messages == [
            ("channel1", b"Hello"),
            ("channel2", b"World!"),
            ("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
        ]
