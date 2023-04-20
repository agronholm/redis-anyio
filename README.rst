.. image:: https://github.com/agronholm/redis-anyio/actions/workflows/test.yml/badge.svg
  :target: https://github.com/agronholm/redis-anyio/actions/workflows/test.yml
  :alt: Build Status
.. image:: https://coveralls.io/repos/agronholm/redis-anyio/badge.svg?branch=main&service=github
  :target: https://coveralls.io/github/agronholm/redis-anyio?branch=main
  :alt: Code Coverage
.. image:: https://readthedocs.org/projects/redis-anyio/badge/?version=latest
  :target: https://redis-anyio.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation

This is an unofficial Redis_ client, implemented using AnyIO_.

Features:

* Full RESP3_ protocol support (Redis 6 and above)
* Support for Redis core commands (but not Redis Stack like JSON, full text,
  time series, etc.)
* Publish/Subscribe support
* Pipeline and transaction support
* Automatic retrying on connection failures with configurable back-off strategies
* Fully type annotated, passes mypy_ checks in strict mode

.. _Redis: https://redis.io/
.. _AnyIO: https://pypi.org/project/anyio/
.. _documentation: https://redis-anyio.readthedocs.io/en/latest/
.. _RESP3: https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md
.. _mypy: https://www.mypy-lang.org/
