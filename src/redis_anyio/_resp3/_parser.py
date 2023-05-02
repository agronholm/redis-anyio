from __future__ import annotations

import sys
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from ._types import (
    RESP3Attributes,
    RESP3BlobError,
    RESP3ParseError,
    RESP3PushData,
    RESP3SimpleError,
    VerbatimString,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from ._types import RESP3Value


class _NotEnoughData:
    __slots__ = ()


# sentinel value indicating that there is not enough data for parsing
not_enough_data = _NotEnoughData()


class SubParser:
    def feed_token(self, data: bytes) -> RESP3Value | _NotEnoughData:
        raise NotImplementedError  # pragma: no cover


@dataclass
class BlobStringParser(SubParser):
    length: int

    def feed_token(self, data: bytes) -> bytes:
        if len(data) != self.length:
            raise RESP3ParseError(
                f"Invalid blob string: length mismatch: expected {self.length} bytes, "
                f"got {len(data)}"
            )

        return data


@dataclass
class BlobErrorParser(SubParser):
    length: int

    def feed_token(self, data: bytes) -> RESP3BlobError:
        if len(data) != self.length:
            raise RESP3ParseError(
                f"Invalid blob error: length mismatch: expected {self.length} "
                f"bytes, got {len(data)} bytes"
            )

        try:
            code, message = data.split(b" ", 1)
        except ValueError:
            raise RESP3ParseError(
                f"Invalid blob error: missing space delimiter in {data!r}"
            ) from None

        return RESP3BlobError(code, message)


@dataclass
class StreamedStringParser(SubParser):
    _next_length: int | None = field(init=False, default=None)
    _string: bytes = field(default=b"")

    def feed_token(self, data: bytes) -> bytes | _NotEnoughData:
        if self._next_length is None:
            if data.startswith(b";"):
                self._next_length = int(data[1:])
                if self._next_length == 0:
                    return self._string
        else:
            if len(data) != self._next_length:
                raise RESP3ParseError(
                    f"Invalid streamed string: length mismatch: expected "
                    f"{self._next_length} bytes in next segment, got {len(data)}"
                )

            self._string += data
            self._next_length = None

        return not_enough_data


@dataclass
class ArrayParser(SubParser):
    length: int
    _subparser: SubParser | None = field(init=False, default=None)
    _items: list[RESP3Value] = field(init=False, default_factory=list)

    def feed_token(self, data: bytes) -> list[RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | _NotEnoughData | SubParser = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            self._items.append(retval)
            if len(self._items) == self.length:
                return self._items

        return not_enough_data


@dataclass
class StreamedArrayParser(SubParser):
    _subparser: SubParser | None = field(init=False, default=None)
    _items: list[RESP3Value] = field(init=False, default_factory=list)

    def feed_token(self, data: bytes) -> list[RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | SubParser | _NotEnoughData = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            if data == b".":
                return self._items

            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            self._items.append(retval)

        return not_enough_data


@dataclass
class SetParser(SubParser):
    length: int | None
    _subparser: SubParser | None = field(init=False, default=None)
    _items: set[RESP3Value] = field(init=False, default_factory=set)

    def feed_token(self, data: bytes) -> set[RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | SubParser | _NotEnoughData = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            self._items.add(retval)
            if len(self._items) == self.length:
                return self._items

        return not_enough_data


@dataclass
class StreamedSetParser(SubParser):
    _subparser: SubParser | None = field(init=False, default=None)
    _items: set[RESP3Value] = field(init=False, default_factory=set)

    def feed_token(self, data: bytes) -> set[RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | SubParser | _NotEnoughData = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            if data == b".":
                return self._items

            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval
                return not_enough_data

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            self._items.add(retval)

        return not_enough_data


@dataclass
class MapParser(SubParser):
    length: int | None
    _subparser: SubParser | None = field(init=False, default=None)
    _items: dict[RESP3Value, RESP3Value] = field(init=False, default_factory=dict)
    _key: RESP3Value = field(init=False)

    def feed_token(self, data: bytes) -> dict[RESP3Value, RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | SubParser | _NotEnoughData = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            if hasattr(self, "_key"):
                self._items[self._key] = retval
                del self._key
                if len(self._items) == self.length:
                    return self._items
            else:
                self._key = retval

        return not_enough_data


@dataclass
class StreamedMapParser(SubParser):
    _subparser: SubParser | None = field(init=False, default=None)
    _items: dict[RESP3Value, RESP3Value] = field(init=False, default_factory=dict)
    _key: RESP3Value = field(init=False)

    def feed_token(self, data: bytes) -> dict[RESP3Value, RESP3Value] | _NotEnoughData:
        if self._subparser:
            retval: RESP3Value | SubParser | _NotEnoughData = (
                self._subparser.feed_token(data)
            )
            if retval is not not_enough_data:
                self._subparser = None
        else:
            if data == b".":
                return self._items

            retval = parse_token(data)
            if isinstance(retval, SubParser):
                self._subparser = retval
                return not_enough_data

        if not isinstance(retval, (SubParser, _NotEnoughData)):
            if hasattr(self, "_key"):
                self._items[self._key] = retval
                del self._key
            else:
                self._key = retval

        return not_enough_data


@dataclass
class PushDataParser(SubParser):
    array_parser: ArrayParser

    def feed_token(self, data: bytes) -> RESP3PushData | _NotEnoughData:
        retval = self.array_parser.feed_token(data)
        if isinstance(retval, list):
            if not isinstance(retval[0], bytes):
                raise RESP3ParseError(
                    f"Invalid push data: first element must be a bytestring; got: "
                    f"{retval[0]}"
                )

            return RESP3PushData(retval[0].decode("utf-8"), retval[1:])

        return not_enough_data


@dataclass
class AttributeParser(SubParser):
    map_parser: MapParser

    def feed_token(self, data: bytes) -> RESP3Attributes | _NotEnoughData:
        retval = self.map_parser.feed_token(data)
        if isinstance(retval, dict):
            return RESP3Attributes(retval)

        return not_enough_data


def parse_token(token: bytes) -> RESP3Value | SubParser:
    type_indicator = token[:1]
    value_buffer = token[1:]
    if type_indicator == b"_":  # Null
        return None
    elif type_indicator == b"+":  # Simple string
        return value_buffer
    elif type_indicator == b"$":  # Blob string
        if value_buffer == b"?":
            return StreamedStringParser()
        else:
            string_length = int(value_buffer)
            return BlobStringParser(string_length)
    elif type_indicator == b"=":  # Verbatim string
        if len(value_buffer) < 4 or value_buffer[3:4] != b":":
            raise RESP3ParseError(
                f"Invalid verbatim string: missing ':' delimiter in {value_buffer!r}"
            )

        type_, content = value_buffer.partition(b":")[::2]
        return VerbatimString(type_, content)
    elif type_indicator == b":":  # Integer
        return int(value_buffer)
    elif type_indicator == b",":  # Double
        # Negative NaN is forbidden as of RESP3 v1.4
        if value_buffer == b"-nan":
            raise RESP3ParseError(f"Invalid double value: {value_buffer!r}")

        return float(value_buffer)
    elif type_indicator == b"(":  # Big number
        return Decimal(value_buffer.decode("utf-8"))
    elif type_indicator == b"#":  # Boolean
        if value_buffer == b"t":
            return True
        elif value_buffer == b"f":
            return False
        else:
            raise RESP3ParseError(f"Invalid boolean value: {value_buffer!r}")
    elif type_indicator == b"-":  # Simple error
        try:
            code, message = value_buffer.decode("utf-8").split(" ", 1)
        except ValueError:
            raise RESP3ParseError(
                f"Invalid simple error: missing space delimiter in {value_buffer!r}"
            ) from None

        return RESP3SimpleError(code, message)
    elif type_indicator == b"!":  # Blob error
        blob_length = int(value_buffer)
        return BlobErrorParser(blob_length)
    elif type_indicator == b"*":  # Array
        if value_buffer == b"?":
            return StreamedArrayParser()
        else:
            array_length = int(value_buffer)
            return ArrayParser(array_length) if array_length else []
    elif type_indicator == b"~":  # Set
        if value_buffer == b"?":
            return StreamedSetParser()
        else:
            set_length = int(value_buffer)
            return SetParser(set_length) if set_length else set()
    elif type_indicator == b"%":  # Map
        if value_buffer == b"?":
            return StreamedMapParser()
        else:
            map_length = int(value_buffer)
            return MapParser(map_length) if map_length else {}
    elif type_indicator == b">":  # Push data
        data_length = int(value_buffer)
        return PushDataParser(ArrayParser(data_length))
    elif type_indicator == b"|":  # Attribute
        attribute_length = int(value_buffer)
        return AttributeParser(MapParser(attribute_length))

    raise RESP3ParseError(f"Unrecognized type indicator: {type_indicator!r}")


@dataclass
class RESP3Parser:
    _buffer: bytes = field(init=False, default=b"")
    _subparser: SubParser | None = field(init=False, default=None)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> RESP3Value:
        while True:
            # Get the next token; raise StopIteration if we can't find the terminator
            end = self._buffer.find(b"\r\n")
            if end < 0:
                raise StopIteration
            elif end == 0:
                raise RESP3ParseError("Got an empty line in the data stream")

            token = self._buffer[:end]
            self._buffer = self._buffer[end + 2 :]

            if self._subparser:
                # If we are parsing a more complex item, send the data to the sub-parser
                # instead
                retval = self._subparser.feed_token(token)
                if not isinstance(retval, _NotEnoughData):
                    self._subparser = None
                    return retval
            else:
                parsed = parse_token(token)
                if isinstance(parsed, SubParser):
                    self._subparser = parsed
                else:
                    return parsed

    def feed_bytes(self, data: bytes) -> None:
        self._buffer += data


def decode_bytestrings(value: RESP3Value) -> RESP3Value:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    elif isinstance(value, list):
        return [decode_bytestrings(x) for x in value]
    elif isinstance(value, set):
        return {decode_bytestrings(x) for x in value}
    elif isinstance(value, dict):
        return {decode_bytestrings(k): decode_bytestrings(v) for k, v in value.items()}
    elif isinstance(value, RESP3PushData):
        value.data = [decode_bytestrings(x) for x in value.data]
    elif isinstance(value, RESP3Attributes):
        return RESP3Attributes(value)

    return value
