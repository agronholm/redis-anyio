import sys
from decimal import Decimal
from typing import Dict, List, Set, Union

from ._resp3 import VerbatimString

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

ResponseValue: TypeAlias = Union[
    None,
    str,
    bytes,
    float,
    bool,
    VerbatimString,
    Decimal,
    List["ResponseValue"],
    Set["ResponseValue"],
    Dict["ResponseValue", "ResponseValue"],
]
