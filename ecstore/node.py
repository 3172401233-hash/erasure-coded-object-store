"""A storage node with two-phase shard visibility and fault injection.

Shards are staged into a pending area and only become visible to readers
when the coordinator commits the (object, version) pair.  A node that
misses the commit keeps its shards pending forever, so readers never
observe a half-written version.
"""

from collections import namedtuple

from .errors import NodeTimeout

ShardView = namedtuple("ShardView", ["version", "index", "payload"])


class StorageNode:
    def __init__(self, name):
        self.name = name
        self._pending = {}
        self._committed = {}
        # Fault-injection switches used by the tests.
        self.fail_stage = False
        self.fail_commit = False
        self.fail_fetch = False

    # -- coordinator-facing write path ---------------------------------

    def stage(self, object_id, version, index, payload):
        if self.fail_stage:
            raise NodeTimeout("node %s timed out on stage" % self.name)
        self._pending[(object_id, version, index)] = bytes(payload)

    def commit(self, object_id, version):
        """Publish every staged shard of (object_id, version) atomically."""
        if self.fail_commit:
            raise NodeTimeout("node %s timed out on commit" % self.name)
        moved = 0
        for key in list(self._pending):
            obj, ver, _idx = key
            if obj == object_id and ver == version:
                self._committed[key] = self._pending.pop(key)
                moved += 1
        return moved

    def abort(self, object_id, version):
        for key in list(self._pending):
            obj, ver, _idx = key
            if obj == object_id and ver == version:
                del self._pending[key]

    # -- reader-facing path ---------------------------------------------

    def fetch(self, object_id, version, index):
        if self.fail_fetch:
            raise NodeTimeout("node %s timed out on fetch" % self.name)
        payload = self._committed.get((object_id, version, index))
        if payload is None:
            return None
        return ShardView(version=version, index=index, payload=payload)

    def committed_indices(self, object_id, version):
        return sorted(
            idx for (obj, ver, idx) in self._committed
            if obj == object_id and ver == version
        )

    # -- test helpers ----------------------------------------------------

    def corrupt_shard(self, object_id, version, index):
        key = (object_id, version, index)
        payload = bytearray(self._committed[key])
        payload[0] ^= 0xFF
        self._committed[key] = bytes(payload)

    def drop_shard(self, object_id, version, index):
        self._committed.pop((object_id, version, index), None)
