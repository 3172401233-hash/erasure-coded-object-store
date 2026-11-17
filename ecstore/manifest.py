"""Versioned object manifests: the atomic commit point of a put()."""

import hashlib
from dataclasses import dataclass, field


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ShardRecord:
    """Where one shard lives and what it must look like."""

    index: int
    node: str
    length: int
    sha256: str


@dataclass(frozen=True)
class Manifest:
    """Immutable description of one committed object version."""

    object_id: str
    version: int
    k: int
    m: int
    object_length: int
    object_sha256: str
    shard_length: int
    shards: tuple = field(default_factory=tuple)

    def record_for(self, index):
        for record in self.shards:
            if record.index == index:
                return record
        return None
