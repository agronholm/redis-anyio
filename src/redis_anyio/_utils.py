from ._types import ResponseValue


def as_string(value: ResponseValue) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")

    assert isinstance(value, str)
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
