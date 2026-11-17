"""Coordinator: two-phase writes, version-pinned reads and repair plans.

Write protocol (put):
  1. encode the object into k + m shards,
  2. stage every shard on its assigned node (pending, invisible),
  3. publish the manifest and commit every reachable node; only now do
     the shards of this version become visible, at a single commit point.

Read protocol (get):
  1. walk committed manifests from newest to oldest,
  2. fetch shards of exactly that version, reject stale/foreign indices,
  3. verify each shard against the digest recorded in the manifest,
  4. decode from the first k valid shards and verify the object digest.
"""

from collections import namedtuple

from . import codec
from .errors import (
    NodeTimeout,
    ObjectNotFound,
    RepairError,
    UnrecoverableError,
    WriteError,
)
from .manifest import Manifest, ShardRecord, digest

RepairPlan = namedtuple(
    "RepairPlan",
    ["object_id", "version", "healthy", "missing", "corrupt", "feasible"],
)


class Cluster:
    def __init__(self, nodes, k, m):
        if len(nodes) < k + m:
            raise ValueError("need at least k + m nodes")
        self.nodes = {node.name: node for node in nodes}
        self._placement = [node.name for node in nodes[: k + m]]
        self.k = k
        self.m = m
        self._manifests = {}
        self._versions = {}

    # ------------------------------------------------------------------
    # write path
    # ------------------------------------------------------------------

    def put(self, object_id, data):
        data = bytes(data)
        version = self._versions.get(object_id, 0) + 1
        shard_payloads, shard_length = codec.encode(data, self.k, self.m)

        staged = []
        for index, payload in enumerate(shard_payloads):
            node = self.nodes[self._placement[index]]
            try:
                node.stage(object_id, version, index, payload)
                staged.append((index, node, payload))
            except NodeTimeout:
                continue
        if len(staged) < self.k:
            for _idx, node, _payload in staged:
                node.abort(object_id, version)
            raise WriteError(
                "staged %d of %d shards, need at least k=%d"
                % (len(staged), self.k + self.m, self.k)
            )

        records = tuple(
            ShardRecord(
                index=index,
                node=node.name,
                length=len(payload),
                sha256=digest(payload),
            )
            for index, node, payload in staged
        )
        manifest = Manifest(
            object_id=object_id,
            version=version,
            k=self.k,
            m=self.m,
            object_length=len(data),
            object_sha256=digest(data),
            shard_length=shard_length,
            shards=records,
        )

        # Commit point: the manifest is published first, then every
        # reachable node flips its staged shards to visible.  Nodes that
        # miss the commit keep their shards pending and invisible.
        self._manifests.setdefault(object_id, {})[version] = manifest
        self._versions[object_id] = version
        for _idx, node, _payload in staged:
            try:
                node.commit(object_id, version)
            except NodeTimeout:
                continue
        return manifest

    # ------------------------------------------------------------------
    # read path
    # ------------------------------------------------------------------

    def manifests(self, object_id):
        versions = self._manifests.get(object_id)
        if not versions:
            raise ObjectNotFound(object_id)
        return [versions[v] for v in sorted(versions, reverse=True)]

    def get(self, object_id):
        errors = []
        for manifest in self.manifests(object_id):
            try:
                return self._read_version(manifest)
            except UnrecoverableError as exc:
                errors.append(exc)
        raise UnrecoverableError(
            "no committed version of %r is recoverable: %s"
            % (object_id, "; ".join(str(e) for e in errors))
        )

    def _read_version(self, manifest):
        good = {}
        for record in manifest.shards:
            if record.index in good:
                continue  # duplicate shard: first valid copy wins
            node = self.nodes[record.node]
            try:
                view = node.fetch(manifest.object_id, manifest.version, record.index)
            except NodeTimeout:
                continue
            if view is None:
                continue
            # Never mix versions or indices inside one reconstruction.
            if view.version != manifest.version or view.index != record.index:
                continue
            if len(view.payload) != record.length or digest(view.payload) != record.sha256:
                continue
            good[record.index] = view.payload
        if len(good) < self.k:
            raise UnrecoverableError(
                "version %d of %r has %d valid shards, need k=%d"
                % (manifest.version, manifest.object_id, len(good), self.k)
            )
        data_shards = codec.decode(good, self.k, self.m, manifest.shard_length)
        data = b"".join(data_shards)[: manifest.object_length]
        if digest(data) != manifest.object_sha256:
            raise UnrecoverableError(
                "object digest mismatch for %r version %d"
                % (manifest.object_id, manifest.version)
            )
        return data

    # ------------------------------------------------------------------
    # repair
    # ------------------------------------------------------------------

    def repair_plan(self, object_id):
        """Classify every shard of the newest committed version."""
        manifest = self.manifests(object_id)[0]
        healthy, missing, corrupt = [], [], []
        for record in manifest.shards:
            node = self.nodes[record.node]
            try:
                view = node.fetch(object_id, manifest.version, record.index)
            except NodeTimeout:
                missing.append(record)
                continue
            if (
                view is None
                or view.version != manifest.version
                or view.index != record.index
            ):
                missing.append(record)
            elif len(view.payload) != record.length or digest(view.payload) != record.sha256:
                corrupt.append(record)
            else:
                healthy.append(record)
        return RepairPlan(
            object_id=object_id,
            version=manifest.version,
            healthy=[r.index for r in healthy],
            missing=missing,
            corrupt=corrupt,
            feasible=len(healthy) >= self.k,
        )

    def repair(self, object_id):
        plan = self.repair_plan(object_id)
        if not plan.feasible:
            raise RepairError(
                "only %d healthy shards of %r version %d, need k=%d"
                % (len(plan.healthy), object_id, plan.version, self.k)
            )
        manifest = self._manifests[object_id][plan.version]
        data = self.get(object_id)
        if digest(data) != manifest.object_sha256:
            raise RepairError("recovered object failed digest verification")
        for record in plan.missing + plan.corrupt:
            node = self.nodes[record.node]
            payload = self._rebuild_shard(record.index, data, manifest)
            try:
                node.stage(object_id, plan.version, record.index, payload)
                node.commit(object_id, plan.version)
            except NodeTimeout as exc:
                raise RepairError(
                    "node %s unreachable during repair" % record.node
                ) from exc
        return plan

    def _rebuild_shard(self, index, data, manifest):
        data_shards, _ = codec.split_object(data, self.k)
        return codec.reencode_shard(index, data_shards, self.k, self.m)
