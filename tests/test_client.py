from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import cast

import pytest
from _pytest.fixtures import SubRequest
from anyio import create_task_group, fail_after, sleep
from anyio.abc import TaskStatus

from redis_anyio import Message, RedisClient, ResponseError

pytestmark = pytest.mark.anyio

FAR_FUTURE_DATE = datetime(2200, 9, 5, 12, 40, 8, tzinfo=timezone.utc)
FAR_FUTURE_DATE_UNIXTS = 7279504808
FAR_FUTURE_DATE_UNIXTS_MS = 7279504808000


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

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(5, id="int"),
            pytest.param(timedelta(seconds=5), id="timedelta"),
        ],
    )
    async def test_set_ex(
        self,
        redis_port: int,
        decode: bool,
        value: int | timedelta,
    ) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar", ex=value)
            ttl = await client.pttl("foo")
            assert 4500 < ttl <= 5000

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(5000, id="int"),
            pytest.param(timedelta(seconds=5), id="timedelta"),
        ],
    )
    async def test_set_px(
        self,
        redis_port: int,
        decode: bool,
        value: int | timedelta,
    ) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar", px=value)
            ttl = await client.pttl("foo")
            assert 4500 < ttl <= 5000

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(FAR_FUTURE_DATE_UNIXTS, id="int"),
            pytest.param(FAR_FUTURE_DATE, id="datetime"),
        ],
    )
    async def test_set_exat(
        self,
        redis7_port: int,
        decode: bool,
        value: int | datetime,
    ) -> None:
        async with RedisClient(port=redis7_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar", exat=value)
            expire_time = await client.expiretime("foo")
            assert expire_time == FAR_FUTURE_DATE_UNIXTS

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(FAR_FUTURE_DATE_UNIXTS_MS, id="int"),
            pytest.param(FAR_FUTURE_DATE, id="datetime"),
        ],
    )
    async def test_set_pxat(
        self,
        redis7_port: int,
        decode: bool,
        value: int | datetime,
    ) -> None:
        async with RedisClient(port=redis7_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar", pxat=value)
            expire_time = await client.pexpiretime("foo")
            assert expire_time == FAR_FUTURE_DATE_UNIXTS_MS

    async def test_mget_mset(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo", "bar")
            await client.mset({"foo": 1, "bar": 2})
            assert (
                await client.mget("foo", "bar") == ["1", "2"]
                if decode
                else [b"1", b"2"]
            )

    async def test_pexpire(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar")
            assert await client.pexpire("foo", 10000) == 1
            assert 8000 < await client.pttl("foo") <= 10000

    async def test_pexpireat(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            await client.set("foo", "bar")
            server_time, _ = await client.time()
            expire_time = server_time * 1000 + 10000
            assert await client.pexpireat("foo", expire_time) == 1
            assert 8000 < await client.pttl("foo") <= 10000

    @pytest.mark.parametrize(
        "kwargs, expected_keys",
        [
            pytest.param({}, ["listkey1", "strkey1", "strkey2"], id="all"),
            pytest.param({"type_": "string"}, ["strkey1", "strkey2"], id="strings"),
            pytest.param({"type_": "list"}, ["listkey1"], id="lists"),
            pytest.param({"match": "*key1"}, ["listkey1", "strkey1"], id="key1"),
        ],
    )
    async def test_scan(
        self, redis_port: int, kwargs: dict[str, str], expected_keys: list[str]
    ) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.flushdb()
            await client.set("strkey1", "value")
            await client.set("strkey2", "value")
            await client.rpush("listkey1", "value")
            async with client.scan(count=1, **kwargs) as iterator:
                keys = [key async for key in iterator]

            assert sorted(keys) == expected_keys


class TestListOperations:
    async def test_blmpop(self, redis7_port: int, decode: bool) -> None:
        async with RedisClient(port=redis7_port) as client, create_task_group() as tg:
            await client.delete("dummy")
            tg.start_soon(client.rpush, "dummy", "value1", "value2", "value3")
            result = await client.blmpop("left", "dummy", count=2, decode=decode)
            if decode:
                assert result == ("dummy", ["value1", "value2"])
            else:
                assert result == ("dummy", [b"value1", b"value2"])

    async def test_lmpop(self, redis7_port: int, decode: bool) -> None:
        async with RedisClient(port=redis7_port) as client:
            await client.delete("dummy")
            assert await client.lmpop("left", "dummy") is None
            assert await client.rpush("dummy", "value1", "value2", "value3")
            result = await client.lmpop("left", "dummy", count=2, decode=decode)
            if decode:
                assert result == ("dummy", ["value1", "value2"])
            else:
                assert result == ("dummy", [b"value1", b"value2"])

    async def test_lpop_single(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            assert await client.lpop("dummy") is None
            assert await client.rpush("dummy", "value1", "value2")
            result = await client.lpop("dummy", decode=decode)
            if decode:
                assert result == "value1"
            else:
                assert result == b"value1"

            assert await client.lpop("dummy") == "value2"
            assert await client.lpop("dummy") is None

    async def test_lpop_multi(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            assert await client.lpop("dummy") is None
            assert await client.rpush("dummy", "value1", "value2", "value3")
            result = await client.lpop("dummy", count=2, decode=decode)
            if decode:
                assert result == ["value1", "value2"]
            else:
                assert result == [b"value1", b"value2"]

            assert await client.rpop("dummy", count=1) == ["value3"]
            assert await client.lpop("dummy", count=1, decode=decode) is None

    async def test_rpop_single(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            assert await client.rpop("dummy") is None
            assert await client.rpush("dummy", "value1", "value2")
            result = await client.rpop("dummy", decode=decode)
            if decode:
                assert result == "value2"
            else:
                assert result == b"value2"

            assert await client.lpop("dummy") == "value1"
            assert await client.lpop("dummy") is None

    async def test_rpop_multi(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            assert await client.rpop("dummy") is None
            assert await client.rpush("dummy", "value1", "value2", "value3")
            result = await client.rpop("dummy", count=2, decode=decode)
            if decode:
                assert result == ["value3", "value2"]
            else:
                assert result == [b"value3", b"value2"]

            assert await client.rpop("dummy", count=1) == ["value1"]
            assert await client.rpop("dummy", count=1, decode=decode) is None

    async def test_rpush_llen(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.rpush("dummy", 6, "foo")
            assert await client.llen("dummy") == 2


class TestHashMapOperations:
    async def test_hset_hget_hmget_hdel(self, redis_port: int, decode: bool) -> None:
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

    async def test_hexists(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field": 2})
            assert await client.hexists("dummy", "field") is True
            assert await client.hexists("dummy", "field2") is False

    async def test_hgetall(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            assert await client.hgetall("dummy") == {}
            await client.hset("dummy", {"key1": 8, "key2": "foo"})
            result = await client.hgetall("dummy", decode=decode)
            if decode:
                assert result == {"key1": "8", "key2": "foo"}
            else:
                assert result == {b"key1": b"8", b"key2": b"foo"}

    async def test_hincrby(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hincrby("dummy", "field", 2)
            assert await client.hget("dummy", "field") == "2"
            await client.hincrby("dummy", "field", 3)
            assert await client.hget("dummy", "field") == "5"

    async def test_hincrbyfloat(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hincrbyfloat("dummy", "field", 2.5)
            assert await client.hget("dummy", "field") == "2.5"
            await client.hincrbyfloat("dummy", "field", 3.5)
            assert await client.hget("dummy", "field") == "6"

    async def test_hkeys(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field": 2, "field2": 1})
            assert await client.hkeys("dummy") == ["field", "field2"]

    async def test_hlen(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field": 2, "field2": 1})
            assert await client.hlen("dummy") == 2

    async def test_hscan_all(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"key1": 8, "key2": "foo"})
            async with client.hscan("dummy", count=1, decode=decode) as iterator:
                result = {field: value async for field, value in iterator}

        if decode:
            assert result == {"key1": "8", "key2": "foo"}
        else:
            assert result == {"key1": b"8", "key2": b"foo"}

    async def test_hrandfield_single(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field1": 1, "field2": 2, "field3": 3})
            assert await client.hrandfield("dummy") in ("field1", "field2", "field3")

    async def test_hrandfield_count(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field1": 1, "field2": 2, "field3": 3})
            fields = await client.hrandfield("dummy", 2)
            assert isinstance(fields, list)
            assert len(fields) == 2
            assert all(field in ("field1", "field2", "field3") for field in fields)

    async def test_hrandfield_withvalues(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"field1": 1, "field2": 2, "field3": 3})
            fields = await client.hrandfield("dummy", 2, withvalues=True, decode=decode)
            assert isinstance(fields, dict)
            assert len(fields) == 2
            for field, value in fields.items():
                assert value == field[-1] if decode else field[-1].encode("ascii")

    async def test_hscan_match(self, redis_port: int, decode: bool) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("dummy")
            await client.hset("dummy", {"key1": 8, "key2": "foo"})
            async with client.hscan(
                "dummy", count=1, match="*2", decode=decode
            ) as iterator:
                result = {field: value async for field, value in iterator}

        if decode:
            assert result == {"key2": "foo"}
        else:
            assert result == {"key2": b"foo"}


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
    async def test_subscribe(self, redis_port: int, decode: bool) -> None:
        async def publish_messages() -> None:
            await client.publish("channel1", "Hello")
            await client.publish("channel2", "World!")
            await client.publish("channel1", "åäö")

        async with RedisClient(port=redis_port) as client:
            async with client.subscribe(
                "channel1", "channel2", decode=decode
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    iterator = subscription.__aiter__()
                    messages = [await iterator.__anext__() for _ in range(3)]

        if decode:
            assert messages == [
                Message("channel1", "Hello"),
                Message("channel2", "World!"),
                Message("channel1", "åäö"),
            ]
        else:
            assert messages == [
                Message("channel1", b"Hello"),
                Message("channel2", b"World!"),
                Message("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
            ]

    async def test_ssubscribe(self, redis7_port: int, decode: bool) -> None:
        async def publish_messages() -> None:
            await client.spublish("channel1", "Hello")
            await client.spublish("channel2", "World!")
            await client.spublish("channel1", "åäö")

        async with RedisClient(port=redis7_port) as client:
            async with client.ssubscribe(
                "channel1", "channel2", decode=decode
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    messages = [await subscription.__anext__() for _ in range(3)]

        if decode:
            assert messages == [
                Message("channel1", "Hello"),
                Message("channel2", "World!"),
                Message("channel1", "åäö"),
            ]
        else:
            assert messages == [
                Message("channel1", b"Hello"),
                Message("channel2", b"World!"),
                Message("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
            ]

    async def test_psubscribe(self, redis_port: int, decode: bool) -> None:
        async def publish_messages() -> None:
            await client.publish("channel1", "Hello")
            await client.publish("channel2", "World!")
            await client.publish("channel1", "åäö")

        async with RedisClient(port=redis_port) as client:
            async with client.psubscribe(
                "channel?", decode=decode
            ) as subscription, create_task_group() as tg:
                tg.start_soon(publish_messages)
                with fail_after(2):
                    messages = [await subscription.__anext__() for _ in range(3)]

        if decode:
            assert messages == [
                Message("channel1", "Hello"),
                Message("channel2", "World!"),
                Message("channel1", "åäö"),
            ]
        else:
            assert messages == [
                Message("channel1", b"Hello"),
                Message("channel2", b"World!"),
                Message("channel1", b"\xc3\xa5\xc3\xa4\xc3\xb6"),
            ]


class TestPipeline:
    async def test_pipeline(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            pipeline = client.pipeline()
            pipeline.hset("foo", {"key": "value"})
            pipeline.pexpire("foo", 1000)
            pipeline.pttl("foo")
            pipeline.get("foo")
            results = await pipeline.execute()
            assert results[:2] == [1, 1]
            assert isinstance(results[2], int)
            assert 990 < results[2] <= 1000
            assert isinstance(results[3], ResponseError)
            assert results[3].code == "WRONGTYPE"


class TestTransaction:
    async def test_transaction(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            transaction = client.transaction()
            transaction.hset("foo", {"key": "value"})
            transaction.pexpire("foo", 1000)
            transaction.pttl("foo")
            results = await transaction.execute()
            assert isinstance(results[2], int)
            assert results[:2] == [1, 1]
            assert 990 < results[2] <= 1000

    async def test_transaction_aborted(self, redis_port: int) -> None:
        async with RedisClient(port=redis_port) as client:
            await client.delete("foo")
            transaction = client.transaction()
            transaction.set("foo", "value")
            transaction.queue_command("FOOBAR")

            with pytest.raises(ResponseError, match="ERR unknown command"):
                await transaction.execute()

            assert await client.get("foo") is None


class TestLock:
    @pytest.mark.parametrize(
        "separate_locks",
        [pytest.param(False, id="same"), pytest.param(True, id="separate")],
    )
    async def test_locking(self, redis_port: int, separate_locks: bool) -> None:
        events: list[str] = []

        async def acquire_lock(*, task_status: TaskStatus) -> None:
            async with lock1:
                task_status.started()
                events.append("subtask acquired the lock")
                await sleep(0.1)
                events.append("subtask sleep done, releasing the lock")

            events.append("subtask released the lock")

        async with RedisClient(port=redis_port) as client, create_task_group() as tg:
            lock1 = client.lock("dummylock")
            lock2 = client.lock("dummylock") if separate_locks else lock1

            await client.delete(lock2.name)
            await client.script_flush()
            await tg.start(acquire_lock)
            async with lock2:
                events.append("main task acquired the lock")

            events.append("main task released the lock")

        assert events == [
            "subtask acquired the lock",
            "subtask sleep done, releasing the lock",
            "subtask released the lock",
            "main task acquired the lock",
            "main task released the lock",
        ]

    async def test_lifetime_as_timedelta(self, redis7_port: int) -> None:
        async with RedisClient(port=redis7_port) as client:
            lock = client.lock("dummy", lifetime=timedelta(seconds=15))
            assert lock.lifetime == 15000

    @pytest.mark.parametrize(
        "separate_locks",
        [pytest.param(False, id="same"), pytest.param(True, id="separate")],
    )
    async def test_task_holding_lock_cancelled(
        self, redis_port: int, separate_locks: bool
    ) -> None:
        """
        Test that when a task that holds a lock gets cancelled, it won't stop the next
        one from getting the lock.
        """

        async def acquire_lock(*, task_status: TaskStatus) -> None:
            async with lock:
                task_status.started()
                await sleep(5)

            pytest.fail("Execution should never reach this point")

        async with RedisClient(port=redis_port) as client:
            lock = client.lock("dummylock")
            async with create_task_group() as tg:
                await tg.start(acquire_lock)
                tg.cancel_scope.cancel()

            with fail_after(1):
                async with lock:
                    pass


async def test_cancel_operation(redis7_port: int) -> None:
    """
    Test that cancelling a task in the middle of a blocking operation will caused the
    connection to be dropped from the pool.
    """
    async with RedisClient(port=redis7_port) as client:
        async with create_task_group() as tg:
            tg.start_soon(client.blpop, "dummy")
            await sleep(0.1)
            assert client.statistics().busy_connections == 1
            assert client.statistics().idle_connections == 0

            tg.cancel_scope.cancel()

        assert client.statistics().busy_connections == 0
        assert client.statistics().idle_connections == 0
