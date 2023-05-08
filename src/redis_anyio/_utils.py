from __future__ import annotations

from datetime import datetime, timedelta

from ._types import ResponseValue


def as_string(value: ResponseValue) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")

    assert isinstance(value, str)
    return value


def as_seconds(value: int | timedelta) -> int:
    if isinstance(value, timedelta):
        return int(value.total_seconds())

    return value


def as_milliseconds(value: int | timedelta) -> int:
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1000)

    return value


def as_unix_timestamp(value: int | datetime) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())

    return value


def as_unix_timestamp_ms(value: int | datetime) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)

    return value


def decode_response_value(value: ResponseValue) -> ResponseValue:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    elif isinstance(value, list):
        return [decode_response_value(x) for x in value]
    elif isinstance(value, set):
        return {decode_response_value(x) for x in value}
    elif isinstance(value, dict):
        return {
            decode_response_value(k): decode_response_value(v) for k, v in value.items()
        }

    return value
