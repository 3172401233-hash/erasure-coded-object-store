import os
import unittest

from ecstore import (
    Cluster,
    ObjectNotFound,
    RepairError,
    ShardRecord,
    ShardView,
    StorageNode,
    UnrecoverableError,
    WriteError,
)
from ecstore.manifest import digest

K, M = 4, 2


def make_cluster(k=K, m=M, node_count=None):
    count = node_count or (k + m)
    nodes = [StorageNode("node-%d" % i) for i in range(count)]
    return Cluster(nodes, k, m), nodes


class RoundTripTest(unittest.TestCase):
    def test_roundtrip_byte_identical(self):
        cluster, _nodes = make_cluster()
        sizes = [0, 1, K - 1, K, K + 1, 3 * K + 1, 4096, 10007]
        for size in sizes:
            payload = os.urandom(size)
            cluster.put("obj-%d" % size, payload)
            self.assertEqual(cluster.get("obj-%d" % size), payload)

    def test_versions_accumulate(self):
        cluster, _nodes = make_cluster()
        cluster.put("obj", b"first")
        cluster.put("obj", b"second")
        self.assertEqual(cluster.get("obj"), b"second")
        self.assertEqual([m.version for m in cluster.manifests("obj")], [2, 1])

    def test_get_unknown_object(self):
        cluster, _nodes = make_cluster()
        with self.assertRaises(ObjectNotFound):
            cluster.get("nope")


class NodeTimeoutTest(unittest.TestCase):
    def test_stage_timeout_still_commits(self):
        cluster, nodes = make_cluster()
        nodes[5].fail_stage = True
        payload = os.urandom(500)
        manifest = cluster.put("obj", payload)
        self.assertEqual(len(manifest.shards), K + M - 1)
        self.assertEqual(cluster.get("obj"), payload)

    def test_stage_timeout_below_k_aborts(self):
        cluster, nodes = make_cluster()
        for node in nodes[: M + 1]:
            node.fail_stage = True
        with self.assertRaises(WriteError):
            cluster.put("obj", b"data")
        with self.assertRaises(ObjectNotFound):
            cluster.get("obj")
        for node in nodes:
            self.assertEqual(node._pending, {})

    def test_fetch_timeout_tolerated(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        cluster.put("obj", payload)
        for node in nodes[:M]:
            node.fail_fetch = True
        self.assertEqual(cluster.get("obj"), payload)


class PartialCommitTest(unittest.TestCase):
    def test_uncommitted_shard_stays_invisible(self):
        cluster, nodes = make_cluster()
        nodes[0].fail_commit = True
        payload = os.urandom(500)
        manifest = cluster.put("obj", payload)
        self.assertEqual(nodes[0].committed_indices("obj", 1), [])
        self.assertEqual(cluster.get("obj"), payload)
        self.assertEqual(len(manifest.shards), K + M)

    def test_commit_quorum_loss_falls_back_to_old_version(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", b"version-one")
        for node in nodes[: M + 1]:
            node.fail_commit = True
        cluster.put("obj", b"version-two")
        # v2 has only k-1 committed shards; the reader must not mix in
        # v1 shards to fill the gap, it falls back to v1 wholesale.
        self.assertEqual(cluster.get("obj"), b"version-one")


class DuplicateShardTest(unittest.TestCase):
    def test_duplicate_record_first_valid_wins(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        manifest = cluster.put("obj", payload)
        # Inject a duplicate record for index 0 pointing at a node that
        # serves garbage; the reader must keep the digest-valid copy.
        garbage_node = nodes[1]
        garbage_node.stage("obj", 1, 0, b"\xde\xad" * manifest.shard_length)
        garbage_node.commit("obj", 1)
        duplicate = ShardRecord(
            index=0,
            node=garbage_node.name,
            length=manifest.shard_length,
            sha256=digest(b"\xde\xad" * manifest.shard_length),
        )
        tampered = cluster._manifests["obj"][1]
        object.__setattr__(tampered, "shards", (duplicate,) + tampered.shards)
        # Corrupt the duplicate so only the original copy is valid.
        garbage_node.corrupt_shard("obj", 1, 0)
        self.assertEqual(cluster.get("obj"), payload)

    def test_wrong_index_shard_rejected(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        cluster.put("obj", payload)

        class ConfusedNode(StorageNode):
            def fetch(self, object_id, version, index):
                view = super().fetch(object_id, version, 0)
                if view is None:
                    return None
                return ShardView(version=version, index=index, payload=view.payload)

        confused = ConfusedNode("node-0")
        confused._committed = nodes[0]._committed
        cluster.nodes["node-0"] = confused
        self.assertEqual(cluster.get("obj"), payload)


class CrossVersionTest(unittest.TestCase):
    def test_stale_node_serving_old_version_is_rejected(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", b"version-one")
        nodes[0].fail_commit = True  # node-0 only ever commits v1
        cluster.put("obj", b"version-two")

        class StaleNode(StorageNode):
            def fetch(self, object_id, version, index):
                for candidate in (version, version - 1):
                    view = super().fetch(object_id, candidate, index)
                    if view is not None:
                        return view
                return None

        stale = StaleNode("node-0")
        stale._committed = nodes[0]._committed
        cluster.nodes["node-0"] = stale
        self.assertEqual(cluster.get("obj"), b"version-two")

    def test_never_mixes_versions_in_one_read(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", b"A" * 100)
        cluster.put("obj", b"B" * 100)
        # Destroy enough v2 shards that v2 alone cannot decode; v1 shards
        # are still fully present and must not be used to top up v2.
        for idx in range(M + 1):
            nodes[idx].drop_shard("obj", 2, idx)
        self.assertEqual(cluster.get("obj"), b"A" * 100)


class CorruptionTest(unittest.TestCase):
    def test_up_to_m_corruptions_recover(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        cluster.put("obj", payload)
        for idx in range(M):
            nodes[idx].corrupt_shard("obj", 1, idx)
        self.assertEqual(cluster.get("obj"), payload)

    def test_more_than_m_corruptions_unrecoverable(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", os.urandom(500))
        for idx in range(M + 1):
            nodes[idx].corrupt_shard("obj", 1, idx)
        with self.assertRaises(UnrecoverableError):
            cluster.get("obj")

    def test_digest_detects_tampering(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        manifest = cluster.put("obj", payload)
        nodes[0].corrupt_shard("obj", 1, 0)
        plan = cluster.repair_plan("obj")
        self.assertEqual([r.index for r in plan.corrupt], [0])
        self.assertEqual(cluster.get("obj"), payload)
        self.assertEqual(manifest.object_sha256, digest(payload))


class RepairTest(unittest.TestCase):
    def test_repair_plan_and_repair(self):
        cluster, nodes = make_cluster()
        payload = os.urandom(500)
        cluster.put("obj", payload)
        nodes[0].corrupt_shard("obj", 1, 0)
        nodes[1].drop_shard("obj", 1, 1)

        plan = cluster.repair_plan("obj")
        self.assertTrue(plan.feasible)
        self.assertEqual([r.index for r in plan.corrupt], [0])
        self.assertEqual([r.index for r in plan.missing], [1])

        cluster.repair("obj")
        followup = cluster.repair_plan("obj")
        self.assertEqual(followup.corrupt, [])
        self.assertEqual(followup.missing, [])
        self.assertEqual(cluster.get("obj"), payload)

    def test_repair_fails_beyond_m_losses(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", os.urandom(500))
        for idx in range(M + 1):
            nodes[idx].corrupt_shard("obj", 1, idx)
        plan = cluster.repair_plan("obj")
        self.assertFalse(plan.feasible)
        with self.assertRaises(RepairError):
            cluster.repair("obj")

    def test_repair_fails_when_target_node_unreachable(self):
        cluster, nodes = make_cluster()
        cluster.put("obj", os.urandom(500))
        nodes[0].corrupt_shard("obj", 1, 0)
        nodes[0].fail_stage = True
        with self.assertRaises(RepairError):
            cluster.repair("obj")


if __name__ == "__main__":
    unittest.main()
