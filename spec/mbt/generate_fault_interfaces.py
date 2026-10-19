#!/usr/bin/env python3
"""Generate/check typed Rust ports for the existing MBFaults TLC corpus.

The current model-interface frontend cannot parse MBFaults's LAMBDA expressions
and requires source variables to exactly match trace variables. Its projection
facility cannot drop the hidden state. Explicit declaration-only interface views
therefore describe the public trace projection. They are not alternate models.
The checked provenance manifest binds them back to the real model and corpus.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "spec/mbt/fault_interfaces"
GEN = Path("/home/nzsn/Repos/Mirrors/.lake/build/bin/model_interface_gen")
PARAM_TYPE = "{actor: Str, mode: Str, scenario: Str, value: Int}"
SCHEMAS = {
    "storage": "{records: Seq({id: Int, seq: Int}), receipts: Seq({id: Int, seq: Int})}",
    "writer": "{owners: Set(Str), records: Seq(Int), rejected: Set(Str), torn: Bool}",
    "inbox": "{head: Int, ack: Int, cursor: Int, messages: Seq(Int), pending: Int}",
    "deadletter": "{terminal: Bool, deadRecords: Seq(Int), up: Bool}",
    "document": "{file: Bool, version: Int, up: Bool}",
    "status": "{records: Seq({id: Int, bytes: Int}), reported: Int}",
    "rotation": "{records: Seq({id: Int, bytes: Int}), files: Seq(Seq(Int)), cursor: {file: Int, seq: Int}, readBack: Seq(Int), up: Bool}",
    "identity": "{ids: (Str -> Str), replies: (Str -> Str), currentTokens: (Str -> Str), kinds: (Str -> Str), labels: (Str -> Str), active: Set(Str), connected: Set(Str)}",
}
SUFFIXES = {"deadletter": "DeadLetter", **{p: p.title() for p in SCHEMAS if p != "deadletter"}}
# Review removes unused scaffold parameters, so a SUT never receives an expected
# result, observation, or next state. Only the actual operation's inputs remain.
INPUTS = {
    "append-bytes": ("Mode", "Value"), "recover-storage": ("Value",),
    "start": ("Actor", "Mode"), "stop": ("Actor",), "writer-append": ("Actor",),
    "inbox-ack": ("Value",), "append-status": ("Value",),
    "begin-rotating-append": ("Value",), "read-cursor": ("Value",),
    "enroll": ("Actor",), "disconnect": ("Actor",), "reconnect": ("Actor",),
    "rotate-token": ("Actor",), "retire": ("Actor",),
    "authenticate": ("Actor", "Mode"),
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def write_or_check(path, content, check):
    if check:
        if not path.is_file() or path.read_bytes() != content:
            raise RuntimeError(f"stale or missing interface artifact: {path.relative_to(ROOT)}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def module_source(name, profile, schema, source_hash):
    return f"""------------------------- MODULE {name} -------------------------
(* Generated interface view of MBFaults, scenario {profile}. *)
(* Declaration-only public projection; MBFaults remains the transition oracle. *)
(* No replacement transition algorithm is defined or verified here. *)
(* Canonical MBFaults SHA256: {source_hash} *)
EXTENDS Integers, Sequences, FiniteSets
VARIABLES
  \\* @type: {schema};
  observation,
  \\* @type: Str;
  result,
  \\* @type: {PARAM_TYPE};
  parameters,
  \\* @type: Str;
  action_taken
=============================================================================
""".encode()


def canonicalize_identity(trace):
    """Rename opaque model choices by first allocation, preserving equality.

    The SUT independently names actual replies by the same first-seen rule. It
    receives no mapping or expected IDs. Repeated/colliding source values retain
    one alias; normalization cannot manufacture distinct identities or tokens.
    """
    names = {"ids": {}, "tokens": {}}
    for state in trace["states"]:
        observation = state["observation"]
        actor = state["parameters"]["actor"]
        for field, domain, sentinel, prefix in (
                ("replies", "ids", "-id", "id"),
                ("ids", "ids", "-id", "id"),
                ("currentTokens", "tokens", "-token", "token")):
            value = observation[field]
            if set(value) != {"#map"}:
                raise ValueError(f"identity {field} must be an ITF map")
            pairs = value["#map"]
            # An enrollment/rotation reply introduces one allocation. Give its
            # actor precedence instead of relying on JSON object/map ordering.
            for key, opaque in sorted(pairs, key=lambda pair: (pair[0] != actor, pair[0])):
                if not isinstance(opaque, str):
                    raise ValueError("identity allocation must be a string")
                if opaque != sentinel and opaque not in names[domain]:
                    names[domain][opaque] = f"{prefix}{len(names[domain]) + 1}"
            for pair in pairs:
                if pair[1] != sentinel:
                    pair[1] = names[domain][pair[1]]
    for mapping in names.values():
        if len(set(mapping.values())) != len(mapping):
            raise ValueError("identity renaming must remain injective")
    return names


def prepare(check, compiler=GEN):
    source_paths = ("spec/MBFaults.tla", "spec/AgentInstances.tla",
                    "spec/mbt/faults/manifest.json", "spec/mbt/check_fault_detection.py",
                    "spec/mbt/generate_fault_interfaces.py")
    sources = {p: digest((ROOT / p).read_bytes()) for p in source_paths}
    original = json.loads((ROOT / "spec/mbt/faults/manifest.json").read_bytes())
    if original["sourceSha256"] != sources["spec/MBFaults.tla"] or original["registrySha256"] != sources["spec/AgentInstances.tla"]:
        raise RuntimeError("canonical fault corpus is stale against its model sources")
    admitted = {str(Path("spec/mbt/faults") / row["file"]): row for row in original["traces"]}
    raw_paths = sorted((ROOT / "spec/mbt/faults/traces").glob("*.itf.json"))
    if set(admitted) != {str(p.relative_to(ROOT)) for p in raw_paths}:
        raise RuntimeError("missing or unexpected canonical fault trace")
    members = []
    groups = {p: [] for p in SCHEMAS}
    for raw in raw_paths:
        trace = json.loads(raw.read_bytes())
        profile = trace["#meta"]["scenario"]
        expected = admitted[str(raw.relative_to(ROOT))]
        if digest(raw.read_bytes()) != expected["sha256"] or profile != expected["scenario"]:
            raise RuntimeError(f"canonical trace differs from checked corpus manifest: {raw}")
        module = "MBFault" + SUFFIXES[profile]
        projected = json.loads(raw.read_bytes())
        renaming = canonicalize_identity(projected) if profile == "identity" else None
        if profile == "storage":
            for state in projected["states"]:
                # These fields assert internal policy, not an independent runtime
                # observation. Outcomes/raw bytes/receipt history expose defects.
                del state["observation"]["phase"]
                del state["observation"]["recoveryRequired"]
        projected["#meta"]["interfaceView"] = module
        projected["#meta"]["varTypes"] = {
            "observation": SCHEMAS[profile], "result": "Str",
            "parameters": PARAM_TYPE, "action_taken": "Str"}
        projected["vars"] = ["observation", "result", "parameters", "action_taken"]
        projected["param_vars"] = ["parameters"]
        target = BASE / profile / "traces" / raw.name
        data = encoded(projected)
        write_or_check(target, data, check)
        member = {"profile": profile, "source": str(raw.relative_to(ROOT)),
                        "sourceSha256": digest(raw.read_bytes()),
                        "projected": str(target.relative_to(ROOT)),
                        "projectedSha256": digest(data)}
        if renaming is not None:
            member["identityRenaming"] = renaming
        members.append(member)
        groups[profile].append(target)
    for profile, schema in SCHEMAS.items():
        if not groups[profile]:
            raise RuntimeError(f"missing required profile: {profile}")
        if set((BASE / profile / "traces").glob("*.itf.json")) != set(groups[profile]):
            raise RuntimeError(f"unexpected projected trace in {profile}; preserve and review it explicitly")
        name = "MBFault" + SUFFIXES[profile]
        write_or_check(ROOT / f"spec/{name}.tla",
                       module_source(name, profile, schema, sources["spec/MBFaults.tla"]), check)
    manifest = {
        "schema": "cooperators.generated-fault-interface-bridge/v1",
        "proofBoundary": "Declaration-only interface views; canonical MBFaults and its fixed-input checks remain the transition oracle. Compiler check proves structural binding, not semantic refinement.",
        "projection": {
            "storage": "Remove phase and recoveryRequired; retain actual records and factual successful receipts.",
            "identity": "Bijectively rename opaque IDs and tokens by first allocation occurrence; preserve equality, sentinels, launch keys and all other fields. The SUT independently names actual replies; no expected values or mapping are passed to it.",
            "otherProfiles": "Preserve observation/result/inputs exactly."},
        "compiler": {"binarySha256": digest(compiler.read_bytes()), "target": "mirrorrust-v2"},
        "sources": sources, "members": members,
    }
    write_or_check(BASE / "provenance.json", encoded(manifest), check)
    return groups


def invoke(compiler, args):
    result = subprocess.run([str(compiler), *map(str, args)], cwd=ROOT,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"compiler {' '.join(map(str, args[:3]))}:\n{result.stdout}")
    print(result.stdout.strip(), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on stale model/corpus/view/contract/binding; do not rewrite")
    parser.add_argument("--compiler", type=Path, default=GEN)
    parser.add_argument("--profile", choices=SCHEMAS, action="append")
    args = parser.parse_args()
    groups = prepare(args.check, args.compiler)
    for profile, traces in groups.items():
        if args.profile and profile not in args.profile:
            continue
        name = "MBFault" + SUFFIXES[profile]
        directory = BASE / profile
        spec = Path(f"spec/{name}.tla")
        contract = directory / f"{name}.mirror-interface.json"
        lock = directory / f"{name}.mirror-interface.lock.json"
        scaffold = directory / f"{name}.mirror-interface.scaffold.json"
        generated = directory / "generated"
        with tempfile.TemporaryDirectory(prefix="coop-fault-interface-") as temporary:
            draft = Path(temporary) / scaffold.name if args.check else scaffold
            invoke(args.compiler, ["scaffold", "--replace", "--spec", spec,
                   "--param-var", "parameters", "--proposal", draft,
                   *[part for trace in traces for part in ("--evidence", trace)]])
            proposal = json.loads(draft.read_bytes())
            reviewed = proposal.get("scaffold", proposal)["contract"]
            for action in reviewed["actions"]:
                keep = INPUTS.get(action["wireAction"], ())
                action["inputs"] = [item for item in action["inputs"] if item["id"] in keep]
            if args.check:
                if draft.read_bytes() != scaffold.read_bytes():
                    raise RuntimeError(f"stale scaffold {scaffold}")
            write_or_check(contract, encoded(reviewed), args.check)
        common = ["--spec", spec, "--contract", contract, "--evidence", traces[0],
                  "--param-var", "parameters", "--lock", lock]
        if not args.check:
            invoke(args.compiler, ["resolve", *common])
            invoke(args.compiler, ["generate", "--lock", lock,
                                  "--target", "mirrorrust-v2", "--out", generated])
        invoke(args.compiler, ["check", *common, "--target", "mirrorrust-v2", "--out", generated])
        invoke(args.compiler, ["preflight", "--lock", lock, "--trace", directory / "traces", "--require-all-actions"])
    print("Generated fault interface bridge and selected compiler checks clean", flush=True)


if __name__ == "__main__":
    main()
