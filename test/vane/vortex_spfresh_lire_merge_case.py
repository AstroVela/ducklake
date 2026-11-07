"""Native-only merge regression: identical posting bytes, one source correction.

The synthetic component VID references below are deliberately separate from the
coordinator's committed DuckLake RowRefs and transaction/ReadView acceptance.
"""

from copy import deepcopy
import json
import shutil
import struct


def exercise(args, report, files, oracle, trace):
    root = args.output_dir / "merge-prefix-case"
    root.mkdir(mode=0o700)
    vectors = [[float(i), *([0.0] * 6), -0.0] for i in range(256)]
    vectors += [[84 + (-0.25 if i % 2 == 0 else 0.25) + (i % 64) / 8192, *([0.0] * 7)] for i in range(1600)]
    rows = [{"row_ref": {"component_vid": i}, "row": {"embedding": v}} for i, v in enumerate(vectors)]
    body = {"mapping": rows, "capture": {"rows": deepcopy(rows)}}
    input_path = root / "seed.bin"
    input_path.write_bytes(b"".join(struct.pack("<8f", *v) for v in vectors[:256]))

    def drain(worker, phase):
        state = worker.call("state")
        for _ in range(2000):
            if state["queued"] == 0:
                return state
            state = worker.call("drain", 16, files.LIMIT)
            trace(report, phase, state)
            files.require(state["steps"] > 0, "COMPONENT_NO_PROGRESS")
        raise AssertionError("Component LIRE did not converge")

    native = root / "source"
    worker = files.Worker(args.probe, "seed-lire", native, root / "source.log", input_path)
    try:
        for offset in (256, 1056):
            expected = list(range(offset, offset + 800))
            files.require(
                worker.call("add", 800, *[v for row in vectors[offset : offset + 800] for v in row])
                == {"ids": expected},
                "COMPONENT_VID_CHANGED",
            )
        split = drain(worker, "component-split")
        files.require(split["splits"] > 0 and split["version_changes"] > 0, "COMPONENT_NO_SPLIT")
        files.require(worker.call("checkpoint") == {"saved": True}, "COMPONENT_CHECKPOINT_FAILED")
        worker.close()
    finally:
        worker.stop()
    image = oracle.observe(args.probe, native, body)
    head = min(
        (int(h) for h in image["current_postings"] if int(h) >= 64),
        key=lambda h: len(image["current_postings"][str(h)]),
    )
    removed = image["current_postings"][str(head)][5:]
    files.require(0 < len(removed) <= 1000, "COMPONENT_MUTATION_BUDGET")
    body["capture"]["rows"] = [r for i, r in enumerate(rows) if i not in set(removed)]
    worker = files.Worker(args.probe, "update-lire", native, root / "delete.log")
    try:
        for vid in removed:
            files.require(worker.call("delete", vid) == {"deleted": True}, "COMPONENT_DELETE_CHANGED")
        files.require(worker.call("checkpoint") == {"saved": True}, "COMPONENT_CHECKPOINT_FAILED")
        worker.close()
    finally:
        worker.stop()
    source_files = files.inventory(native)
    oracle.observe(args.probe, native, body)
    (root / "input.json").write_text(json.dumps({"body": body, "head": head, "removed": removed}, indent=2) + "\n")
    (root / "source-files.json").write_text(json.dumps(source_files, indent=2) + "\n")
    outcomes = {}
    for name, probe in (("control", args.merge_control_probe), ("fixed", args.probe)):
        target = root / name
        shutil.copytree(native, target)
        files.require(files.inventory(target) == source_files, "COMPONENT_INPUT_CHANGED")
        worker = files.Worker(probe, "update-lire", target, root / (name + ".log"))
        try:
            worker.call("merge", head)
            state = drain(worker, "component-merge-" + name)
            files.require(state["merges"] > 0, "COMPONENT_NO_MERGE")
            files.require(worker.call("checkpoint") == {"saved": True}, "COMPONENT_CHECKPOINT_FAILED")
            worker.close()
        finally:
            worker.stop()
        try:
            after = oracle.observe(args.probe, target, body)
        except AssertionError as error:
            files.require(name == "control" and "Nearest-partition assignment violated" in str(error), str(error))
            outcomes[name] = {"status": "rejected", "reason": str(error)}
        else:
            files.require(name == "fixed", "MERGE_PREFIX_CONTROL_UNEXPECTED_PASS")
            outcomes[name] = {"status": "passed", "active_heads": after["active_heads"]}
            (root / "fixed-physical.json").write_text(json.dumps(after, indent=2) + "\n")
        outcomes[name]["state"] = {k: v for k, v in state.items() if k != "heads"}
    files.require(files.inventory(native) == source_files, "COMPONENT_SOURCE_CHANGED")
    report["merge_prefix_case"] = {
        "scope": "Synthetic native-only VIDs, separate from committed DuckLake captures",
        "source_files": source_files,
        "input_sha256": files.sha((root / "input.json").read_bytes()),
        "head": head,
        "deleted_vids": len(removed),
        "outcomes": outcomes,
    }
