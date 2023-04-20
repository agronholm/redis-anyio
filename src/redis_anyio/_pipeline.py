from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field


@dataclass
class RedisPipeline:
    _queued_commands: list[tuple[str, ...]] = field(init=False)

    def __len__(self) -> int:
        return len(self._queued_commands)

    def __iter__(self) -> Iterator[tuple[str, ...]]:
        return iter(self._queued_commands)
