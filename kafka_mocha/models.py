from dataclasses import dataclass, field, astuple, replace
from datetime import datetime
from heapq import heappush
from typing import Any, Optional, Callable


def current_timestamp_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


@dataclass(eq=False)
class KHeader:
    """Dataclass mimicking Kafka record header"""

    key: str
    value: bytes

    def __post_init__(self):
        if not isinstance(self.value, bytes):
            raise TypeError("Value must be bytes")

    @classmethod
    def from_tuple(cls, header: tuple) -> "KHeader":
        return cls(header[0], header[1])


@dataclass(eq=False)
class PMessage:
    """Dataclass mimicking producer message"""

    partition: int
    topic: str
    key: bytes
    value: bytes
    headers: Optional[tuple[KHeader, ...] | list[KHeader]] = field(default=None, compare=False)
    on_delivery: Callable[[Any, Any], None] = lambda *_: None
    # timestamp: float = field(default_factory=lambda: datetime.now().timestamp(), repr=False)
    timestamp: int = field(default=0)

    def __post_init__(self):
        if not (isinstance(self.partition, int) and self.partition >= 0):
            raise TypeError("Record's partition must be unsigned integer")
        if not isinstance(self.key, bytes):
            raise TypeError("Record's key must be bytes")
        if not isinstance(self.value, bytes):
            raise TypeError("Record's value must be bytes")
        if self.headers:
            if not isinstance(self.headers, tuple) and not isinstance(self.headers, list):
                raise TypeError("Record's headers must be a tuple or a list")

    @classmethod
    def from_producer_data(
        cls, *, headers: tuple | list, timestamp: int, **kwargs
    ) -> "PMessage":
        k_headers = [KHeader.from_tuple(header) for header in headers] if headers else []
        if timestamp != 0:
            _timestamp = timestamp / 1000
            return cls(headers=k_headers, timestamp=_timestamp, **kwargs)
        return cls(headers=k_headers, **kwargs)


@dataclass(frozen=True, eq=False)
class KRecord:
    """Dataclass mimicking Kafka record"""

    partition: int
    offset: int
    key: bytes
    value: bytes
    headers: Optional[tuple[KHeader, ...] | list[KHeader]] = field(default=None, compare=False)
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp(), repr=False)

    def __post_init__(self):
        if not (isinstance(self.partition, int) and self.partition >= 0):
            raise TypeError("Record's partition must be unsigned integer")
        if not (isinstance(self.offset, int) and self.partition >= 0):
            raise TypeError("Record's offset must be unsigned integer")
        if not isinstance(self.key, bytes):
            raise TypeError("Record's key must be bytes")
        if not isinstance(self.value, bytes):
            raise TypeError("Record's value must be bytes")
        if self.headers:
            if not isinstance(self.headers, tuple) and not isinstance(self.headers, list):
                raise TypeError("Record's headers must be a tuple or a list")

    def as_tuple(self) -> tuple:
        if isinstance(self.headers, list):
            copy = replace(self, headers=tuple(self.headers))
            return astuple(copy)
        return astuple(self)

    def __eq__(self, other) -> bool:
        """Record is unambiguously determined by partition and offset."""
        return self.offset == other.offset and self.partition == other.partition

    def __str__(self) -> str:
        return (
            f"("
            f"partition={self.partition}, "
            f"offset={self.offset}, "
            f"key={self.key}, "
            f"value={self.value}, "
            f"timestamp={datetime.fromtimestamp(self.timestamp).isoformat()}, "
            f"headers={self.headers})"
            f")"
        )


class KPartition:
    """Dataclass mimicking Kafka partition"""

    def __init__(self):
        self._heap = []

    def append(self, record: KRecord) -> None:
        heappush(self._heap, record.as_tuple())

    def get(self, offset: int = 0, batch_size: int = 1) -> list[KRecord]:
        return self._heap[offset:batch_size]

    def __len__(self) -> int:
        return len(self._heap)


@dataclass(frozen=True)
class KTopic:
    """Dataclass mimicking Kafka topic"""

    name: str
    partition_no: int = field(default=1)
    partitions: list[KPartition] = field(init=False, default_factory=list)
    config: Optional[dict[str, Any]] = None

    def __post_init__(self):
        if not (isinstance(self.partition_no, int) and self.partition_no > 0):
            raise TypeError("Topic's partition number must be integer greater than 0")
        [self.partitions.append(KPartition()) for _ in range(self.partition_no)]
