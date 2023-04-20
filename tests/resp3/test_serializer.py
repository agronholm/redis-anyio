from __future__ import annotations

from decimal import Decimal

import pytest

from redis_anyio import RESP3Value
from redis_anyio._resp3 import serialize_command, serialize_value


@pytest.mark.parametrize(
    "payload, expected",
    [
        pytest.param(None, b"_\r\n", id="none"),
        pytest.param(b"Foo", b"$3\r\nFoo\r\n", id="bytes"),
        pytest.param("åäö", b"$6\r\n\xc3\xa5\xc3\xa4\xc3\xb6\r\n", id="string"),
        pytest.param(-12355, b":-12355\r\n", id="integer"),
        pytest.param(-76.3401, b",-76.3401\r\n", id="double"),
        pytest.param(
            Decimal("-654604562.245345436"), b"(-654604562.245345436\r\n", id="decimal"
        ),
        pytest.param(True, b"#t\r\n", id="bool_true"),
        pytest.param(False, b"#f\r\n", id="bool_false"),
        pytest.param(
            ["Foo", [17, True]],
            b"*2\r\n$3\r\nFoo\r\n*2\r\n:17\r\n#t\r\n",
            id="list_nested",
        ),
        pytest.param(
            ("Foo", (17, True)),
            b"*2\r\n$3\r\nFoo\r\n*2\r\n:17\r\n#t\r\n",
            id="tuple_nested",
        ),
        pytest.param(
            {"Foo": [17, True]},
            b"%1\r\n$3\r\nFoo\r\n*2\r\n:17\r\n#t\r\n",
            id="dict_nested",
        ),
    ],
)
def test_serialize(payload: bytes, expected: RESP3Value) -> None:
    assert serialize_value(payload) == expected


def test_serialize_set() -> None:
    serialized = serialize_value({"Foo", 17})
    assert serialized in (b"~2\r\n$3\r\nFoo\r\n:17\r\n", b"~2\r\n:17\r\n$3\r\nFoo\r\n")


def test_serialize_unsupported_type() -> None:
    with pytest.raises(
        TypeError,
        match="Cannot serialize a value of type complex for the RESP3 protocol",
    ):
        serialize_value(4 + 9j)  # type: ignore[arg-type]


def test_serialize_command() -> None:
    assert serialize_command("HELLO", 3) == b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
