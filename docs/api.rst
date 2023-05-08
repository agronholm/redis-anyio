API reference
=============

.. module:: redis_anyio

Client
======

.. autoclass:: RedisClient

Types
=====

.. autoclass:: VerbatimString

.. autoclass:: Subscription

.. autoclass:: Message

.. autoclass:: RedisPipeline

.. autoclass:: RedisTransaction

.. autoclass:: RedisConnectionPoolStatistics

.. autoclass:: RedisLock

Constants
=========

.. autodata:: CONNECTION_STATE_CHANNEL

.. autodata:: DISCONNECTED

.. autodata:: RECONNECTED

Exceptions
==========

.. autoexception:: RedisError

.. autoexception:: ResponseError

.. autoexception:: ConnectivityError

.. autoexception:: ProtocolError
