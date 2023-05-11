from __future__ import annotations

import sys
from dataclasses import dataclass, field
from decimal import Decimal

from ._types import (
    RESP3Attributes,
    RESP3BlobError,
    RESP3End,
    RESP3ParseError,
    RESP3PushData,
    RESP3SimpleError,
    RESP3Value,
    VerbatimString,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class RESP3Parser:
    _buffer: bytes = field(init=False, default=b"")
    _subparser: RESP3Parser | None = field(init=False, default=None)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> RESP3Value:
        if self._subparser:
            return self._get_next_subparser_item()

        type_indicator, value_buffer = self._parse_next_token()
        if type_indicator == b"_":  # Null
            return None
        elif type_indicator == b"+":  # Simple string
            return value_buffer
        elif type_indicator == b"$":  # Blob string
            if value_buffer == b"?":
                return self._start_subparser(StreamedStringParser())
            else:
                string_length = int(value_buffer)
                return self._start_subparser(BlobStringParser(string_length + 2))
        elif type_indicator == b"=":  # Verbatim string
            if len(value_buffer) < 4 or value_buffer[3:4] != b":":
                raise RESP3ParseError(
                    f"Invalid verbatim string: missing ':' delimiter in "
                    f"{value_buffer!r}"
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
            return self._start_subparser(BlobErrorParser(blob_length + 2))
        elif type_indicator == b"*":  # Array
            if value_buffer == b"?":
                return self._start_subparser(StreamedArrayParser())
            else:
                array_length = int(value_buffer)
                if array_length:
                    return self._start_subparser(ArrayParser(array_length))
                else:
                    return []
        elif type_indicator == b"~":  # Set
            if value_buffer == b"?":
                return self._start_subparser(StreamedSetParser())
            else:
                set_length = int(value_buffer)
                if set_length:
                    return self._start_subparser(SetParser(set_length))
                else:
                    return set()
        elif type_indicator == b"%":  # Map
            if value_buffer == b"?":
                return self._start_subparser(StreamedMapParser())
            else:
                map_length = int(value_buffer)
                if map_length:
                    return self._start_subparser(MapParser(map_length * 2))
                else:
                    return {}
        elif type_indicator == b".":  # End of stream
            return RESP3End()
        elif type_indicator == b">":  # Push data
            data_length = int(value_buffer)
            array_parser = ArrayParser(data_length)
            return self._start_subparser(PushDataParser(array_parser))
        elif type_indicator == b"|":  # Attribute
            attribute_length = int(value_buffer)
            map_parser = MapParser(attribute_length * 2)
            return self._start_subparser(AttributeParser(map_parser))

        raise RESP3ParseError(f"Unrecognized type indicator: {type_indicator!r}")

    def _parse_next_token(self) -> tuple[bytes, bytes]:
        end = self._buffer.find(b"\r\n")
        if end < 0:
            raise StopIteration
        elif end == 0:
            raise RESP3ParseError("Got an empty line in the data stream")

        token = self._buffer[:end]
        self._buffer = self._buffer[end + 2 :]
        return token[:1], token[1:]

    def _start_subparser(self, subparser: RESP3Parser) -> RESP3Value:
        self._subparser = subparser
        self._subparser.feed_bytes(self._buffer)
        self._buffer = b""
        return self._get_next_subparser_item()

    def _get_next_subparser_item(self) -> RESP3Value:
        assert self._subparser is not None
        item = next(self._subparser)
        self._buffer = self._subparser._buffer
        self._subparser = None
        return item

    def feed_bytes(self, data: bytes) -> None:
        if self._subparser:
            self._subparser.feed_bytes(data)
        else:
            self._buffer += data


@dataclass
class BlobStringParser(RESP3Parser):
    bytes_needed: int  # len(payload + \r\n)

    def __next__(self) -> bytes:
        if len(self._buffer) < self.bytes_needed:
            raise StopIteration

        if self._buffer[self.bytes_needed - 2 : self.bytes_needed] != b"\r\n":
            raise RESP3ParseError(
                f"Invalid blob string: CRLF not found after reading "
                f"{self.bytes_needed} bytes"
            )

        payload = self._buffer[: self.bytes_needed - 2]
        self._buffer = self._buffer[self.bytes_needed :]
        return payload


@dataclass
class BlobErrorParser(RESP3Parser):
    bytes_needed: int

    def __next__(self) -> RESP3BlobError:
        if len(self._buffer) < self.bytes_needed:
            raise StopIteration

        if self._buffer[self.bytes_needed - 2 : self.bytes_needed] != b"\r\n":
            raise RESP3ParseError(
                f"Invalid blob error: CRLF not found after reading "
                f"{self.bytes_needed} bytes"
            )

        payload = self._buffer[: self.bytes_needed - 2]
        self._buffer = self._buffer[self.bytes_needed :]
        try:
            code, message = payload.split(b" ", 1)
        except ValueError:
            raise RESP3ParseError(
                f"Invalid blob error: missing space delimiter in {payload!r}"
            ) from None

        return RESP3BlobError(code, message)


@dataclass
class StreamedStringParser(RESP3Parser):
    _next_length: int | None = field(init=False, default=None)
    _string: bytes = field(default=b"")

    def __next__(self) -> bytes:
        while self._next_length != 2:  # actual indicated length + 2
            if self._next_length is None:
                type_indicator, value_buffer = self._parse_next_token()
                if type_indicator != b";":
                    raise RESP3ParseError(
                        f"Unexpected type indicator when parsing a streamed string: "
                        f"{type_indicator!r}"
                    )

                self._next_length = int(value_buffer) + 2
            elif len(self._buffer) >= self._next_length:
                if self._buffer[self._next_length - 2 : self._next_length] != b"\r\n":
                    raise RESP3ParseError(
                        f"Invalid chunk in streamed string: CRLF not found after "
                        f"reading {self._next_length} bytes"
                    )

                self._string += self._buffer[: self._next_length - 2]
                self._buffer = self._buffer[self._next_length :]
                self._next_length = None
            else:
                raise StopIteration

        return self._string


@dataclass
class ArrayParser(RESP3Parser):
    remaining_items: int
    _items: list[RESP3Value] = field(init=False, default_factory=list)

    def __next__(self) -> list[RESP3Value]:
        for _ in range(self.remaining_items):
            self._items.append(super().__next__())
            self.remaining_items -= 1

        return self._items


@dataclass
class StreamedArrayParser(RESP3Parser):
    _items: list[RESP3Value] = field(init=False, default_factory=list)

    def __next__(self) -> list[RESP3Value]:
        while True:
            item = super().__next__()
            if isinstance(item, RESP3End):
                return self._items

            self._items.append(item)


@dataclass
class SetParser(RESP3Parser):
    remaining_items: int
    _items: set[RESP3Value] = field(init=False, default_factory=set)

    def __next__(self) -> set[RESP3Value]:
        for _ in range(self.remaining_items):
            self._items.add(super().__next__())
            self.remaining_items -= 1

        return self._items


@dataclass
class StreamedSetParser(RESP3Parser):
    _items: set[RESP3Value] = field(init=False, default_factory=set)

    def __next__(self) -> set[RESP3Value]:
        while True:
            item = super().__next__()
            if isinstance(item, RESP3End):
                return self._items

            self._items.add(item)


@dataclass
class MapParser(RESP3Parser):
    remaining_items: int
    _items: dict[RESP3Value, RESP3Value] = field(init=False, default_factory=dict)
    _key: RESP3Value = field(init=False)

    def __next__(self) -> dict[RESP3Value, RESP3Value]:
        for _ in range(self.remaining_items):
            item = super().__next__()
            self.remaining_items -= 1
            if hasattr(self, "_key"):
                self._items[self._key] = item
                del self._key
            else:
                self._key = item

        return self._items


@dataclass
class StreamedMapParser(RESP3Parser):
    _items: dict[RESP3Value, RESP3Value] = field(init=False, default_factory=dict)
    _key: RESP3Value = field(init=False)

    def __next__(self) -> dict[RESP3Value, RESP3Value]:
        while True:
            item = super().__next__()
            if isinstance(item, RESP3End):
                return self._items

            if hasattr(self, "_key"):
                self._items[self._key] = item
                del self._key
            else:
                self._key = item


class PushDataParser(RESP3Parser):
    def __init__(self, array_parser: ArrayParser):
        super().__init__()
        self._subparser = array_parser
        self._array_parser = array_parser

    def __next__(self) -> RESP3PushData:
        item = next(self._array_parser)
        if not isinstance(item[0], bytes):
            raise RESP3ParseError(
                f"Invalid push data: first element must be a bytestring; got: "
                f"{item[0]!r}"
            )

        return RESP3PushData(item[0].decode("utf-8"), item[1:])


@dataclass
class AttributeParser(RESP3Parser):
    def __init__(self, map_parser: MapParser):
        super().__init__()
        self._subparser = map_parser
        self._map_parser = map_parser

    def __next__(self) -> RESP3Attributes:
        item = next(self._map_parser)
        return RESP3Attributes(item)
