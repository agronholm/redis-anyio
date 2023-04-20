from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

RESP3Value: TypeAlias = (
    "None | str | bytes | float | bool | VerbatimString | Decimal | RESP3PushData "
    "| RESP3Attribute | RESP3SimpleError | RESP3BlobError | list[RESP3Value] "
    "| set[RESP3Value] | dict[RESP3Value, RESP3Value]"
)


@dataclass(frozen=True)
class RESP3PushData:
    """
    Encapsulates push data (either pub-sub or ``MONITOR``).

    .. attribute:: type
        :type: str
        Type of the push data being sent. Typically ``monitor`` or ``pubsub``.

    .. attribute:: data
        :type: Sequence[RESP3Value]
        Type-dependent subtype of the push data being sent.
    """

    type: str
    data: Sequence[RESP3Value]


class RESP3Attribute(Dict[RESP3Value, RESP3Value]):
    """A map of auxiliary response info, transmitted as out-of-band data."""


class VerbatimString(bytes):
    """
    A string with embedded formatting information.

    .. attribute:: type
        :type: str

        Specifies the formatting of the string; either ``txt`` for plain text, or
        ``mkd`` for markdown.
    """

    type: bytes

    def __new__(cls, type_: bytes, value: bytes) -> Self:
        instance = super().__new__(cls, value)
        instance.type = type_
        return instance


class RESP3ParseError(Exception):
    """
    Raised when the protocol state machine detects a protocol discrepancy.

    If this exception is raised by the protocol, the connection should be invalidated.
    """


class RESP3SimpleError(Exception):
    """
    Represents a (unicode) error returned from the server.

    .. attribute:: code
        :type: str | None

        The "code" part (e.g. ``ERR`` or ``NOPROTO``) in a Redis error.

    .. attribute:: message
        :type: str

        The description part of the error message.
    """

    code: str
    message: str

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return self.message


class RESP3BlobError(Exception):
    """
    Represents a binary safe error returned from the server.

    .. attribute:: code
        :type: str | None

        The "code" part (e.g. ``ERR`` or ``NOPROTO``) in a Redis error.

    .. attribute:: message
        :type: str

        The description part of the error message.
    """

    code: bytes
    message: bytes

    def __init__(self, code: bytes, message: bytes) -> None:
        super().__init__(code, message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return self.message.decode("utf-8", errors="replace")
