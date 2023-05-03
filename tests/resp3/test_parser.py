from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

import pytest

from redis_anyio._resp3 import (
    RESP3Attributes,
    RESP3BlobError,
    RESP3ParseError,
    RESP3Parser,
    RESP3PushData,
    RESP3SimpleError,
    VerbatimString,
)


@pytest.fixture
def parser() -> RESP3Parser:
    return RESP3Parser()


@pytest.mark.parametrize(
    "payload, expected",
    [
        pytest.param(b"_\r\n", None, id="null"),
        pytest.param(b"+Test string\r\n", b"Test string", id="string_simple"),
        pytest.param(b"$11\r\nTest string\r\n", b"Test string", id="string_blob"),
        pytest.param(
            b"$?\r\n;5\r\nTest \r\n;6\r\nstring\r\n;0\r\n",
            b"Test string",
            id="string_streamed",
        ),
        pytest.param(b":-764363\r\n", -764363, id="integer"),
        pytest.param(b",-5.071\r\n", -5.071, id="double"),
        pytest.param(
            b"(-654333333333333333366546456546123135646\r\n",
            Decimal("-654333333333333333366546456546123135646"),
            id="bignumber",
        ),
        pytest.param(b"#t\r\n", True, id="bool_true"),
        pytest.param(b"#f\r\n", False, id="bool_false"),
        pytest.param(b"*2\r\n+Test\r\n:7\r\n", [b"Test", 7], id="array_2_items"),
        pytest.param(b"*0\r\n", [], id="array_empty"),
        pytest.param(
            b"*2\r\n*1\r\n+Test\r\n*1\r\n:7\r\n", [[b"Test"], [7]], id="array_nested"
        ),
        pytest.param(b"*?\r\n+Test\r\n:7\r\n.\r\n", [b"Test", 7], id="array_streamed"),
        pytest.param(
            b"*?\r\n$4\r\nTest\r\n:7\r\n.\r\n", [b"Test", 7], id="array_streamed_nested"
        ),
        pytest.param(b"~2\r\n+Test\r\n:7\r\n", {b"Test", 7}, id="set_2_items"),
        pytest.param(b"~0\r\n", set(), id="set_empty"),
        pytest.param(b"~2\r\n$4\r\nTest\r\n:7\r\n", {b"Test", 7}, id="set_nested"),
        pytest.param(b"~?\r\n+Test\r\n:7\r\n.\r\n", {b"Test", 7}, id="set_streamed"),
        pytest.param(
            b"~?\r\n$4\r\nTest\r\n:7\r\n.\r\n", {b"Test", 7}, id="set_streamed_nested"
        ),
        pytest.param(
            b"%2\r\n+Test\r\n:7\r\n+Foo\r\n+Bar\r\n",
            {b"Test": 7, b"Foo": b"Bar"},
            id="map_2_items",
        ),
        pytest.param(b"%0\r\n", {}, id="map_empty"),
        pytest.param(
            b"%2\r\n+Test\r\n:7\r\n+Foo\r\n%1\r\n+Bar\r\n+Baz\r\n",
            {b"Test": 7, b"Foo": {b"Bar": b"Baz"}},
            id="map_nested",
        ),
        pytest.param(
            b"%?\r\n+Test\r\n:7\r\n+Foo\r\n+Bar\r\n.\r\n",
            {b"Test": 7, b"Foo": b"Bar"},
            id="map_streamed",
        ),
        pytest.param(
            b"%?\r\n$4\r\nTest\r\n:7\r\n+Foo\r\n+Bar\r\n.\r\n",
            {b"Test": 7, b"Foo": b"Bar"},
            id="map_streamed_nested",
        ),
        pytest.param(
            b">3\r\n$9\r\nsubscribe\r\n$5\r\ndummy\r\n:1\r\n",
            RESP3PushData("subscribe", [b"dummy", 1]),
            id="pushdata_subscribe",
        ),
        pytest.param(
            b">3\r\n$7\r\nmessage\r\n$5\r\ntopic\r\n$4\r\nTest\r\n",
            RESP3PushData("message", [b"topic", b"Test"]),
            id="pushdata_message",
        ),
        pytest.param(
            b"|1\r\n+Test\r\n:7\r\n", RESP3Attributes({b"Test": 7}), id="attribute"
        ),
    ],
)
def test_parser(payload: bytes, expected: Any, parser: RESP3Parser) -> None:
    parser.feed_bytes(payload)
    responses = list(parser)
    assert len(responses) == 1
    assert responses[0] == expected


def test_verbatim_string(parser: RESP3Parser) -> None:
    parser.feed_bytes(b"=txt:Test string\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], VerbatimString)
    assert responses[0].type == b"txt"
    assert responses[0] == b"Test string"


def test_simple_error(parser: RESP3Parser) -> None:
    parser.feed_bytes(b"-ERR Failure\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], RESP3SimpleError)
    assert responses[0].code == "ERR"
    assert responses[0].message == "Failure"
    assert str(responses[0]) == "Failure"


def test_blob_error(parser: RESP3Parser) -> None:
    parser.feed_bytes(b"!11\r\nERR Failure\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], RESP3BlobError)
    assert responses[0].code == b"ERR"
    assert responses[0].message == b"Failure"
    assert str(responses[0]) == "Failure"


def test_double_nan(parser: RESP3Parser) -> None:
    parser.feed_bytes(b",nan\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], float)
    assert math.isnan(responses[0])


def test_double_inf(parser: RESP3Parser) -> None:
    parser.feed_bytes(b",inf\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], float)
    assert math.isinf(responses[0])
    assert responses[0] > 0


def test_double_negative_inf(parser: RESP3Parser) -> None:
    parser.feed_bytes(b",-inf\r\n")
    responses = list(parser)
    assert len(responses) == 1
    assert isinstance(responses[0], float)
    assert math.isinf(responses[0])
    assert responses[0] < 0


@pytest.mark.parametrize(
    "payload, error_regex",
    [
        pytest.param(b"\r\n", "Got an empty line in the data stream", id="empty_line"),
        pytest.param(
            b"N\r\n", "Unrecognized type indicator: b'N'", id="unrecognized_type"
        ),
        pytest.param(b"#h\r\n", "Invalid boolean value: b'h'", id="bool"),
        pytest.param(b",-nan\r\n", "Invalid double value: b'-nan'", id="negative_nan"),
        pytest.param(
            b"$4\r\nFoo\r\n",
            "Invalid blob string: length mismatch: expected 4 bytes, got 3",
            id="blob_string_length_mismatch",
        ),
        pytest.param(
            b"=Foo\r\n",
            "Invalid verbatim string: missing ':' delimiter in b'Foo'",
            id="verbatim_string_no_delimiter_found",
        ),
        pytest.param(
            b"$?\r\n;4\r\nFoo\r\n",
            "Invalid streamed string: length mismatch: expected 4 bytes in next "
            "segment, got 3",
            id="streamed_string_length_mismatch",
        ),
        pytest.param(
            b"-Foo\r\n",
            "Invalid simple error: missing space delimiter in b'Foo'",
            id="simple_error_no_delimiter_found",
        ),
        pytest.param(
            b"!4\r\nFoo\r\n",
            "Invalid blob error: length mismatch: expected 4 bytes, got 3",
            id="blob_error_length_mismatch",
        ),
        pytest.param(
            b"!3\r\nFoo\r\n",
            "Invalid blob error: missing space delimiter in b'Foo'",
            id="blob_error_no_delimiter_found",
        ),
        pytest.param(
            b">2\r\n:1\r\n+Foo\r\n",
            "Invalid push data: first element must be a bytestring; got: 1",
            id="push_data_invalid_first_element",
        ),
    ],
)
def test_parse_errors(payload: bytes, error_regex: str, parser: RESP3Parser) -> None:
    parser.feed_bytes(payload)
    with pytest.raises(RESP3ParseError, match=error_regex):
        next(parser)
