Differences with the official Redis Python client
=================================================

.. py:currentmodule:: redis_anyio

* Asynchronous operation:

  * The official client uses :mod:`asyncio` directly, whereas this implementation
    leverages AnyIO_ for compatibility with both Trio as well as :mod:`asyncio`.
  * This implementation leverages structured concurrency, making it more robust against
    cancellation and unexpected problems.

* Supported features:

  * Currently, this implementation only supports a narrow subset of features provided
    by the official client. In particular, Redis Stack features are absent.

* String handling: Byte string (:class:`bytes`) to unicode string (:class:`str`)
  decoding is controlled on the level of individual method (via the ``decode``
  argument), rather than the client object, as in the official client. This has three
  advantages:

  * It allows the Redis client object to be shared between multiple unrelated code bases
    working together in the same application.
  * It allows developers to opt out of unicode decoding on a case-by-case basis when
    receiving binary data (e.g. when using Redis to cache serialized objects).
  * It allows static type checkers to determine the exact return values of
    string-returning commands, whereas in the official client, all such methods are
    typed as returning either :class:`str` or :class:`bytes`.

* Publish/subscribe:

  * In the official client, you first create a "pubsub" object and then use that to
    make subscriptions. In this implementation, you call :meth:`RedisClient.subscribe`,
    :meth:`RedisClient.ssubscribe` or :meth:`RedisClient.psubscribe` and use them with
    ``async with`` and then iterate through them to receive the push events.

* Pipelines and transactions:

  * Pipelines and transactions are clearly separated from each other, created via either
    :meth:`RedisClient.pipeline` or :meth:`RedisClient.transaction`, whereas in the
    official client they are lumped into the same pipeline class, differentiated via a
    flag.
  * There is currently no way to leverage the ``WATCH`` command in this implementation.
  * Commands are queued with synchronous calls, unlike with the official async client.
  * Queuing a command in this implementation gives you a handle that lets you check the
    result after execution, while in the official client, you get all the results
    `back as a list <https://github.com/redis/redis-py#pipelines>`_ from the
    ``execute()`` call.

.. _AnyIO: https://pypi.org/project/anyio/
.. _Trio: https://trio.readthedocs.io/en/stable/
