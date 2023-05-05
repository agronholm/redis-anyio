User guide
==========

.. py:currentmodule:: redis_anyio

Quickstart
----------

Install:

.. code:: bash

    $ pip install redis-anyio

A simple script that sets and gets the value of a key::

    import asyncio

    from redis_anyio import RedisClient


    async def main() -> None:
        async with RedisClient() as redis:
            await redis.set("somekey", "somevalue")
            print(await redis.get("somekey"))


    asyncio.run(main())

Another simple script which subscribes to a channel and prints out all the received
messages::

    import asyncio

    from redis_anyio import RedisClient


    async def main() -> None:
        async with RedisClient() as redis, redis.subscribe("sometopic") as messages:
            async for message in messages:
                print(message)


    asyncio.run(main())
