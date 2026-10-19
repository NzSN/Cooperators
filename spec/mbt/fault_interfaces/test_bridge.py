"""Fail-closed provenance tests for the generated-interface projection bridge."""
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "generate_fault_interfaces.py"
IMPORT = importlib.util.spec_from_file_location("fault_interface_bridge", SCRIPT)
BRIDGE = importlib.util.module_from_spec(IMPORT)
IMPORT.loader.exec_module(BRIDGE)


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory(prefix="coop-bridge-test-")
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        for relative in ("spec/MBFaults.tla", "spec/AgentInstances.tla",
                         "spec/mbt/check_fault_detection.py", "spec/mbt/generate_fault_interfaces.py"):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(BRIDGE.ROOT / relative, target)
        shutil.copytree(BRIDGE.ROOT / "spec/mbt/faults", self.root / "spec/mbt/faults")
        self.base = self.root / "spec/mbt/fault_interfaces"
        self.compiler = self.root / "compiler"
        self.compiler.write_bytes(b"identified compiler fixture")
        self.addCleanup(patch.stopall)
        patch.object(BRIDGE, "ROOT", self.root).start()
        patch.object(BRIDGE, "BASE", self.base).start()
        BRIDGE.prepare(False, self.compiler)

    def test_projection_retains_actual_outcomes_records_and_receipts(self):
        groups = BRIDGE.prepare(True, self.compiler)
        self.assertEqual(set(groups), set(BRIDGE.SCHEMAS))
        corpus = json.loads((self.root / "spec/mbt/faults/manifest.json").read_bytes())
        self.assertEqual(sum(map(len, groups.values())), len(corpus["traces"]))
        manifest = json.loads((self.base / "provenance.json").read_bytes())
        renamings = {Path(row["projected"]).name: row.get("identityRenaming") for row in manifest["members"]}
        for profile, paths in groups.items():
            for path in paths:
                actual = json.loads(path.read_bytes())
                original = json.loads((self.root / "spec/mbt/faults/traces" / path.name).read_bytes())
                for row, expected in zip(actual["states"], original["states"]):
                    self.assertEqual(row["parameters"], expected["parameters"])
                    self.assertEqual(row["result"], expected["result"])
                    self.assertEqual(row["action_taken"], expected["action_taken"])
                    wanted = dict(expected["observation"])
                    if profile == "storage":
                        del wanted["phase"]
                        del wanted["recoveryRequired"]
                    observed = json.loads(json.dumps(row["observation"]))
                    if profile == "identity":
                        names = renamings[path.name]
                        for field, domain in (("ids", "ids"), ("replies", "ids"), ("currentTokens", "tokens")):
                            inverse = {alias: opaque for opaque, alias in names[domain].items()}
                            self.assertEqual(len(inverse), len(names[domain]))
                            for pair in observed[field]["#map"]:
                                pair[1] = inverse.get(pair[1], pair[1])
                    self.assertEqual(observed, wanted)

    def test_identity_allocation_names_follow_first_reply_and_preserve_collisions(self):
        trace = {"states": [
            {"parameters": {"actor": "launch2"}, "observation": {
                "replies": {"#map": [["launch1", "-id"], ["launch2", "id3"]]},
                "ids": {"#map": [["launch1", "-id"], ["launch2", "id3"]]},
                "currentTokens": {"#map": [["launch1", "-token"], ["launch2", "token2"]]}}},
            {"parameters": {"actor": "launch1"}, "observation": {
                "replies": {"#map": [["launch1", "id3"], ["launch2", "id3"]]},
                "ids": {"#map": [["launch1", "id3"], ["launch2", "id3"]]},
                "currentTokens": {"#map": [["launch1", "token1"], ["launch2", "token2"]]}}}]}
        names = BRIDGE.canonicalize_identity(trace)
        self.assertEqual(names, {"ids": {"id3": "id1"}, "tokens": {"token2": "token1", "token1": "token2"}})
        self.assertEqual(trace["states"][0]["observation"]["ids"]["#map"], [["launch1", "-id"], ["launch2", "id1"]])
        self.assertEqual(trace["states"][1]["observation"]["ids"]["#map"], [["launch1", "id1"], ["launch2", "id1"]])

    def test_model_change_rejects_stale_canonical_certificate(self):
        path = self.root / "spec/MBFaults.tla"
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(RuntimeError, "canonical fault corpus is stale"):
            BRIDGE.prepare(True, self.compiler)

    def test_missing_canonical_trace_fails(self):
        next((self.root / "spec/mbt/faults/traces").glob("*.itf.json")).unlink()
        with self.assertRaisesRegex(RuntimeError, "missing or unexpected canonical fault trace"):
            BRIDGE.prepare(True, self.compiler)

    def test_changed_canonical_trace_fails(self):
        path = next((self.root / "spec/mbt/faults/traces").glob("*.itf.json"))
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(RuntimeError, "canonical trace differs"):
            BRIDGE.prepare(True, self.compiler)

    def test_missing_projected_trace_fails(self):
        next((self.base / "storage/traces").glob("*.itf.json")).unlink()
        with self.assertRaisesRegex(RuntimeError, "stale or missing interface artifact"):
            BRIDGE.prepare(True, self.compiler)

    def test_changed_projected_observation_fails(self):
        path = next((self.base / "storage/traces").glob("*.itf.json"))
        trace = json.loads(path.read_bytes())
        trace["states"][0]["observation"]["records"] = ["fabricated"]
        path.write_text(json.dumps(trace))
        with self.assertRaisesRegex(RuntimeError, "stale or missing interface artifact"):
            BRIDGE.prepare(True, self.compiler)

    def test_changed_interface_view_fails(self):
        path = self.root / "spec/MBFaultStorage.tla"
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(RuntimeError, "stale or missing interface artifact"):
            BRIDGE.prepare(True, self.compiler)

    def test_different_compiler_identity_fails(self):
        self.compiler.write_bytes(b"different compiler")
        with self.assertRaisesRegex(RuntimeError, "stale or missing interface artifact"):
            BRIDGE.prepare(True, self.compiler)


if __name__ == "__main__":
    unittest.main()
