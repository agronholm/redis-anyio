from ._resp3 import RESP3Value


def as_string(value: RESP3Value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")

    assert isinstance(value, str)
    return value
