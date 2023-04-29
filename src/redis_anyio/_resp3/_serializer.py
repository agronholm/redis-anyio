"""Contains functions for serializing objects for sending to a Redis server."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._types import RESP3Value

SERIALIZED_NULL = b"_\r\n"


def serialize_bytestring(value: bytes) -> bytes:
    return b"$%d\r\n%s\r\n" % (len(value), value)


def serialize_boolean(value: bool) -> bytes:
    return b"#t\r\n" if value else b"#f\r\n"


def serialize_int(value: int) -> bytes:
    return f":{value}\r\n".encode()


def serialize_double(value: float) -> bytes:
    return f",{value}\r\n".encode()


def serialize_bignumber(value: Decimal) -> bytes:
    return f"({value}\r\n".encode()


def serialize_array(value: Sequence[RESP3Value]) -> bytes:
    items_as_bytes = b"".join([serialize_value(item) for item in value])
    return b"*%d\r\n%s" % (len(value), items_as_bytes)


def serialize_set(value: set[RESP3Value]) -> bytes:
    items_as_bytes = b"".join([serialize_value(item) for item in value])
    return b"~%d\r\n%s" % (len(value), items_as_bytes)


def serialize_map(value: Mapping[RESP3Value, RESP3Value]) -> bytes:
    encoded_items: list[bytes] = []
    for key, val in value.items():
        encoded_items.append(serialize_value(key))
        encoded_items.append(serialize_value(val))

    items_as_bytes = b"".join(encoded_items)
    return b"%%%d\r\n%s" % (len(value), items_as_bytes)


def serialize_value(value: RESP3Value) -> bytes:
    if value is None:
        return SERIALIZED_NULL
    elif isinstance(value, bytes):
        return serialize_bytestring(value)
    elif isinstance(value, str):
        return serialize_bytestring(value.encode("utf-8"))
    elif isinstance(value, bool):
        return serialize_boolean(value)
    elif isinstance(value, int):
        return serialize_int(value)
    elif isinstance(value, float):
        return serialize_double(value)
    elif isinstance(value, Decimal):
        return serialize_bignumber(value)
    elif isinstance(value, list):
        return serialize_array(value)
    elif isinstance(value, tuple):
        return serialize_array(list(value))
    elif isinstance(value, set):
        return serialize_set(value)
    elif isinstance(value, frozenset):
        return serialize_set(set(value))
    elif isinstance(value, Mapping):
        return serialize_map(value)

    typename = type(value).__qualname__
    raise TypeError(
        f"Cannot serialize a value of type {typename} for the RESP3 protocol"
    )


def serialize_command(command: str, *args: object) -> bytes:
    """
    Serialize the command and its arguments.

    :param command: name of the command to send
    :param args: arguments for the command
    :return: the bytes to be sent to the Redis server

    """
    return serialize_array(
        [command.encode("utf-8")]
        + [arg if isinstance(arg, bytes) else str(arg).encode("utf-8") for arg in args]
    )
