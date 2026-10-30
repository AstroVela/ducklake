#!/usr/bin/env python3
"""Measure installed Vane mixed load in a process separate from its exact oracle."""
from __future__ import annotations

import argparse
import bisect
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import threading
import time

SPEC = importlib.util.spec_from_file_location(
    "automatic_proof", Path(__file__).with_name("test_vane_vortex_automatic_flow.py")
)
assert SPEC is not None and SPEC.loader is not None
AUTO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUTO)
BASE, KEYED = AUTO.BASE, AUTO.KEYED
query, require, quote = AUTO.query, AUTO.require, AUTO.quote
REQUESTS = AUTO.WRITE.REQUESTS
PHASES = [("paced", 1.0), ("burst", 0.0), ("recovery", 1.0)]


def row(key, epoch, dimension, fixture):
    result = AUTO.row(key, epoch)
    if fixture == "cost":
        result["embedding"] = [float(key) + (epoch % 3) / 64] + [
            float((key * (i + 3)) % (i + 11)) for i in range(1, dimension)
        ]
    else:
        result["embedding"].extend(((key * (i + 1)) % 17) / 16 for i in range(8, dimension))
    return result


def plan(keys, epoch, dimension, fixture):
    slots = [((epoch - 1) * 128 + i) % len(keys) for i in range(128)]
    added = [len(keys) + (epoch - 1) * 32 + i for i in range(32)]
    rows = [row(keys[slot], epoch, dimension, fixture) for slot in slots[:96]]
    rows.extend(row(key, epoch, dimension, fixture) for key in added)
    return rows, [keys[slot] for slot in slots[96:]], list(zip(slots[96:], added))


def canonical_bytes(rows, deletes, dimension):
    length = len(b"ducklake-vortex-mutate-v1") + 4 + 8 + 8 + 8 * len(deletes)
    for entry in rows:
        length += 1 + 8 + 4 * dimension
        for field in ["category", "payload"]:
            length += 1
            if entry[field] is not None:
                length += 8 + len(entry[field].encode())
    return length


def watermark(connection):
    return query(
        connection,
        "SELECT coalesce(max(snapshot_id),0) AS n FROM " + REQUESTS + " WHERE starts_with(request_id,'steady-')",
    )[0]["n"]


def counters():
    values = {}
    for filename, keys in [
        ("status", {"VmRSS", "VmHWM"}),
        ("io", {"rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes"}),
    ]:
        for line in Path("/proc/self/" + filename).read_text().splitlines():
            key, value = line.split(":", 1)
            if key in keys:
                values[key] = int(value.split()[0])
    usage = resource.getrusage(resource.RUSAGE_SELF)
    values.update(user_seconds=usage.ru_utime, system_seconds=usage.ru_stime)
    return values


def bootstrap(root, count, dimension, fixture):
    connection = BASE.connect(root)
    (root / "indexes").mkdir()
    for sql in [
        "CREATE TABLE lake.items(id BIGINT,embedding FLOAT[],category VARCHAR,payload VARCHAR)",
        "CALL ducklake_set_option('lake','experimental_data_file_format','vortex',table_name=>'items')",
        "CALL ducklake_set_option('lake','data_inlining_row_limit',0,table_name=>'items')",
        "SELECT * FROM ducklake_initialize_vortex_indexes('lake')",
        "SELECT * FROM ducklake_initialize_vortex_optimizer('lake')",
        "SELECT * FROM ducklake_initialize_vortex_writes('lake')",
        "SELECT * FROM ducklake_create_vortex_index('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension})",
    ]:
        connection.execute(sql)
    KEYED.manage(connection)
    sequences = [range(0, count, 2), range(1, count, 2)] if fixture == "cost" else [range(count)]
    for part, sequence in enumerate(sequences):
        for start in range(0, len(sequence), 1000):
            KEYED.mutate(
                connection,
                f"seed-{part}-{start}",
                [row(key, 0, dimension, fixture) for key in sequence[start : start + 1000]],
            )
    connection.execute(
        "SELECT * FROM ducklake_build_vortex_index('lake','main','items','knn',"
        + quote(root / "indexes" / "initial.json")
        + f",{dimension},1)"
    )
    connection.execute("CALL ducklake_set_option('lake','data_inlining_row_limit',1000,table_name=>'items')")
    for sql in [
        "SELECT * FROM ducklake_enable_vortex_optimizer('lake','main','items','knn',"
        + quote(root / "indexes")
        + f",{dimension},128,4194304,128,67108864)",
        "SELECT * FROM ducklake_set_vortex_write_budget('lake','main','items','knn',256,4194304,1024,67108864)",
        "SELECT * FROM ducklake_set_vortex_data_compaction_policy('lake','main','items','knn',true,32,1)",
        "SELECT * FROM ducklake_set_vortex_source_retention('lake',true,2,0)",
        "SELECT * FROM ducklake_enable_vortex_maintenance('lake',2,67108864)",
    ]:
        connection.execute(sql)
    AUTO.settle(connection, root, 2, min(100000, 3 * count + 2048))
    return connection


def worker(args):
    import vane

    root, count, dimension, fixture = args.worker_case, args.rows[0], args.dimensions[0], args.fixtures[0]
    lock, stopped = threading.Lock(), threading.Event()
    pipe = os.fdopen(args.events_fd, "w", encoding="utf-8", buffering=1)
    emitted = 0
    phase = "setup"

    def emit(kind, **values):
        nonlocal emitted
        text = json.dumps(dict(kind=kind, **values), ensure_ascii=False, separators=(",", ":")) + "\n"
        with lock:
            pipe.write(text)
            pipe.flush()
            emitted += len(text.encode())

    def usage():
        with lock:
            return dict(counters(), event_pipe_bytes=emitted, time_ns=time.monotonic_ns())

    connection = bootstrap(root, count, dimension, fixture)
    emit(
        "ready",
        version=vane.__version__,
        engine=query(connection, "PRAGMA version")[0],
        count=count,
        dimension=dimension,
        fixture=fixture,
        providers={
            dist.metadata["Name"]: dist.version
            for dist in importlib.metadata.distributions()
            if dist.metadata["Name"].startswith("vane-extension-")
        },
    )
    keys, epoch = list(range(count)), 1
    pending_started = None
    errors = []
    cursors = [connection.cursor() for _ in range(args.readers + 1)]
    baseline = usage()

    def guarded(call):
        try:
            call()
        except Exception as error:
            errors.append(str(error))
            stopped.set()
            raise

    def reader(index):
        cursor = cursors[index]
        serial = index
        while not stopped.is_set():
            serial += args.readers
            canary = index == 0
            vector = (
                (
                    row(133, 0, dimension, fixture)["embedding"]
                    if fixture == "cost"
                    else BASE.VECTOR + [0.0] * (dimension - 8)
                )
                if canary
                else row((serial * 257) % count, 0, dimension, fixture)["embedding"]
            )
            category = "B" if serial % 2 else None
            query_phase = phase
            before_time = time.monotonic_ns()
            before = watermark(cursor)
            started = time.monotonic_ns()
            hits = query(cursor, BASE.SEARCH.search_sql(vector, category=category))
            finished = time.monotonic_ns()
            after = watermark(cursor)
            emit(
                "query",
                phase=query_phase,
                canary=canary,
                before=before,
                after=after,
                before_time_ns=before_time,
                elapsed_ms=(finished - started) / 1e6,
                finished_ns=finished,
                vector=vector,
                category=category,
                rows=hits,
            )
            stopped.wait(args.recovery_query_pause if phase in ["recovery", "drain"] else args.query_pause)

    def monitor():
        cursor = cursors[-1]
        while not stopped.is_set():
            optimizer, maintenance = AUTO.healthy(cursor)
            source = query(cursor, "SELECT * FROM ducklake_vortex_source_retention_status('lake')")[0]
            emit("sample", phase=phase, usage=usage(), optimizer=optimizer, maintenance=maintenance, source=source)
            stopped.wait(0.25)

    def producer(name, interval, deadline):
        nonlocal epoch, pending_started
        cursor = connection.cursor()
        try:
            next_due = time.monotonic()
            while time.monotonic() < deadline and not stopped.is_set():
                if stopped.wait(max(0, next_due - time.monotonic())):
                    break
                if time.monotonic() >= deadline:
                    break
                rows, deletes, changes = plan(keys, epoch, dimension, fixture)
                started = time.monotonic_ns()
                if pending_started is None:
                    pending_started = started
                try:
                    receipt = KEYED.mutate(cursor, f"steady-{epoch}", rows, deletes)
                except Exception as error:
                    message = str(error)
                    snapshot_conflict = (
                        'duplicate key "snapshot_id:' in message.lower() and "primary key constraint" in message.lower()
                    )
                    backpressure = "RESOURCE_EXHAUSTED: Vortex write " in message and " budget exceeded" in message
                    require(backpressure or "transaction conflict" in message.lower() or snapshot_conflict, message)
                    AUTO.WRITE.check_no_request(cursor, f"steady-{epoch}")
                    emit(
                        "rejected",
                        phase=name,
                        epoch=epoch,
                        error=message,
                        reason="backpressure" if backpressure else "native_conflict",
                        elapsed_ms=(time.monotonic_ns() - started) / 1e6,
                        started_ns=started,
                    )
                    stopped.wait(0.01)
                else:
                    finished = time.monotonic_ns()
                    emit(
                        "ack",
                        phase=name,
                        epoch=epoch,
                        receipt=receipt,
                        finished_ns=finished,
                        end_to_end_ms=(finished - pending_started) / 1e6,
                        started_ns=started,
                        elapsed_ms=(finished - started) / 1e6,
                        input_bytes=canonical_bytes(rows, deletes, dimension),
                    )
                    for slot, key in changes:
                        keys[slot] = key
                    epoch += 1
                    pending_started = None
                next_due = time.monotonic() + interval
        finally:
            cursor.close()

    with ThreadPoolExecutor(max_workers=args.readers + 2) as pool:
        tasks = [pool.submit(guarded, lambda index=i: reader(index)) for i in range(args.readers)]
        tasks.append(pool.submit(guarded, monitor))
        try:
            for name, interval in PHASES:
                phase = name
                begin = usage()
                task = pool.submit(guarded, lambda: producer(name, interval, time.monotonic() + args.seconds))
                task.result(timeout=args.seconds + 90)
                emit("phase", phase=name, begin=begin, end=usage(), interval_seconds=interval)
                require(not errors, str(errors))
            phase = "drain"
            begin = usage()
            _, source, footprint = AUTO.settle(connection, root, 2, min(100000, 3 * count + 2048))
            emit("drained", begin=begin, end=usage(), source=source, footprint=footprint)
        finally:
            stopped.set()
            for task in tasks:
                task.result(timeout=90)
            for cursor in cursors:
                cursor.close()
    emit("measured", begin=baseline, end=usage())
    try:
        require(not errors, str(errors))
        emit("final_rows", rows=BASE.raw(connection))
        recorded = query(
            connection,
            "SELECT request_id,snapshot_id FROM "
            + REQUESTS
            + " WHERE starts_with(request_id,'steady-') ORDER BY snapshot_id",
        )
        require(len(recorded) == epoch - 1, "Rejected attempts left receipts or successful writes lost receipts")
        for entry in recorded[:1] + recorded[-1:]:
            # Retry checks run outside the measured workload.
            number = int(entry["request_id"].split("-")[-1])
            old_keys = list(range(count))
            for n in range(1, number + 1):
                rows, deletes, changes = plan(old_keys, n, dimension, fixture)
                for slot, key in changes:
                    old_keys[slot] = key
            require(
                KEYED.mutate(connection, entry["request_id"], rows, deletes)["snapshot_id"] == entry["snapshot_id"],
                "Retry changed its receipt",
            )
        emit("retry_rows", rows=BASE.raw(connection))
    finally:
        connection.execute("SELECT * FROM ducklake_cancel_vortex_optimizer('lake','main','items','knn')")
        connection.execute("SELECT * FROM ducklake_cancel_vortex_maintenance('lake')")
        deadline = time.monotonic() + 30
        while (
            BASE.status(connection)["running"]
            or query(connection, "SELECT * FROM ducklake_vortex_maintenance_status('lake')")[0]["running"]
        ):
            require(time.monotonic() < deadline, "Workers did not stop at load teardown")
            time.sleep(0.01)
        connection.close()
        pipe.close()


def percentiles(values):
    require(bool(values), "Measurement has no samples")
    ordered = sorted(values)
    return {f"p{n}": ordered[math.ceil(len(ordered) * n / 100) - 1] for n in [50, 95, 99]}


def delta(begin, end):
    keys = ["rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes", "user_seconds", "system_seconds"]
    values = {key: end[key] - begin[key] for key in keys}
    values["event_pipe_bytes"] = end["event_pipe_bytes"] - begin["event_pipe_bytes"]
    values["wchar_excluding_event_pipe"] = values["wchar"] - values["event_pipe_bytes"]
    values["seconds"] = (end["time_ns"] - begin["time_ns"]) / 1e9
    return values


def valid_hits(sample, model):
    import numpy as np

    rows = sample["rows"]
    if len(rows) > 10 or len({entry["id"] for entry in rows}) != len(rows):
        return None
    previous = -1
    point = sample["vector"]
    for entry in rows:
        expected = model.get(entry["id"])
        if expected is None or any(entry[key] != expected[key] for key in ["embedding", "category", "payload"]):
            return None
        if sample["category"] is not None and entry["category"] != sample["category"]:
            return None
        exact = sum((a - b) ** 2 for a, b in zip(entry["embedding"], point))
        if entry["distance"] < previous or not math.isclose(entry["distance"], exact, rel_tol=1e-5, abs_tol=1e-4):
            return None
        previous = entry["distance"]
    eligible = [
        entry for entry in model.values() if sample["category"] is None or entry["category"] == sample["category"]
    ]
    if not eligible:
        return 1.0 if not rows else None
    vectors = np.asarray([entry["embedding"] for entry in eligible], dtype=np.float64)
    distances = np.sum((vectors - np.asarray(point)) ** 2, axis=1)
    ranked = sorted(zip(distances.tolist(), [entry["id"] for entry in eligible]))[:10]
    wanted = {key for _, key in ranked}
    recall = len(wanted & {entry["id"] for entry in rows}) / len(wanted)
    return recall


def validate(events, count, dimension, fixture):
    acks = sorted([event for event in events if event["kind"] == "ack"], key=lambda entry: entry["epoch"])
    require([entry["epoch"] for entry in acks] == list(range(1, len(acks) + 1)), "Mutation sequence has a gap")
    snapshots = [0] + [entry["receipt"]["snapshot_id"] for entry in acks]
    require(snapshots == sorted(set(snapshots)), "Commit order changed the single producer's order")
    keys, model, changes = list(range(count)), {key: row(key, 0, dimension, fixture) for key in range(count)}, []
    for entry in acks:
        upserts, deletes, replaced = plan(keys, entry["epoch"], dimension, fixture)
        changes.append((upserts, deletes))
        for slot, key in replaced:
            keys[slot] = key

    def apply(state, change):
        upserts, deletes = change
        for key in deletes:
            del state[key]
        state.update((entry["id"], entry) for entry in upserts)

    samples = sorted([event for event in events if event["kind"] == "query"], key=lambda entry: entry["before"])
    recalls, canary_recalls, exploration_recalls, overlapping, position = [], [], [], 0, 0
    for sample in samples:
        require(
            sample["before"] in snapshots and sample["after"] in snapshots and sample["before"] <= sample["after"],
            "Query watermark has no committed model",
        )
        acknowledged = max(
            (entry["receipt"]["snapshot_id"] for entry in acks if entry["finished_ns"] <= sample["before_time_ns"]),
            default=0,
        )
        require(sample["before"] >= acknowledged, "Query missed a previously acknowledged commit")
        lower, upper = bisect.bisect_left(snapshots, sample["before"]), bisect.bisect_left(snapshots, sample["after"])
        while position < lower:
            apply(model, changes[position])
            position += 1
        candidate = dict(model)
        compatible_recalls = []
        for index in range(lower, upper + 1):
            if index > lower:
                apply(candidate, changes[index - 1])
            recall = valid_hits(sample, candidate)
            if recall is not None:
                compatible_recalls.append(recall)
        require(compatible_recalls, "Query matches no single committed state inside its read window")
        best = min(compatible_recalls)
        if sample["canary"]:
            canary_recalls.append(best)
        else:
            exploration_recalls.append(best)
        recalls.append(best)
        overlapping += upper > lower
    for change in changes[position:]:
        apply(model, change)
    expected = [model[key] for key in sorted(model)]
    for kind in ["final_rows", "retry_rows"]:
        actual = [entry for entry in events if entry["kind"] == kind]
        require(len(actual) == 1 and actual[0]["rows"] == expected, "Drain or old retry changed current logical rows")
    require(len(expected) == count and len(samples) >= 12, "Insufficient concurrent workload coverage")
    return dict(
        queries=len(samples),
        queries_overlapping_commits=overlapping,
        minimum_recall_lower_bound=min(recalls),
        canary_minimum_recall_lower_bound=min(canary_recalls),
        canary_gate_passed=min(canary_recalls) >= 0.8,
        exploration_minimum_recall_lower_bound=min(exploration_recalls),
        exploration_recall=percentiles(exploration_recalls),
        exploration_meets_0_8_target=min(exploration_recalls) >= 0.8,
        committed_batches=len(acks),
    )


def summarize(events):
    result = {}
    for name, _ in PHASES:
        phase = next(entry for entry in events if entry["kind"] == "phase" and entry["phase"] == name)
        acks = [entry for entry in events if entry["kind"] == "ack" and entry["phase"] == name]
        queries = [entry for entry in events if entry["kind"] == "query" and entry["phase"] == name]
        rejections = [entry for entry in events if entry["kind"] == "rejected" and entry["phase"] == name]
        usage = delta(phase["begin"], phase["end"])
        require(acks and queries, "Load phase did not complete writes and queries: " + name)
        logical = sum(entry["input_bytes"] for entry in acks)
        result[name] = dict(
            ack_attempt_ms=percentiles([entry["elapsed_ms"] for entry in acks]),
            ack_with_explicit_retries_ms=percentiles([entry["end_to_end_ms"] for entry in acks]),
            query_ms=percentiles([entry["elapsed_ms"] for entry in queries]),
            query_classes={
                label: percentiles([entry["elapsed_ms"] for entry in queries if entry["canary"] == canary])
                for label, canary in [("canary", True), ("exploration", False)]
            },
            committed_batches=len(acks),
            committed_operations=len(acks) * 160,
            queries=len(queries),
            rejected_attempts=len(rejections),
            backpressure_rejections=sum(entry["reason"] == "backpressure" for entry in rejections),
            usage=usage,
            canonical_mutation_bytes=logical,
            process_write_bytes_per_mutation_byte=usage["write_bytes"] / logical,
            acknowledged_batches_per_second=len(acks) / usage["seconds"],
        )
    require(result["burst"]["backpressure_rejections"] > 0, "Burst never reached the configured write budget")
    drain = next(entry for entry in events if entry["kind"] == "drained")
    samples = [entry for entry in events if entry["kind"] == "sample"]
    require(
        samples
        and all(
            entry["optimizer"]["inline_rows"] <= 256
            and entry["optimizer"]["unindexed_rows"] <= 1024
            and entry["optimizer"]["inline_bytes"] <= 4194304
            and entry["optimizer"]["unindexed_bytes"] <= 67108864
            for entry in samples
        ),
        "Observed backlog exceeded configured write budgets",
    )
    return dict(
        phases=result,
        drain=delta(drain["begin"], drain["end"]),
        footprint=drain["footprint"],
        sampled_peak_rss_kib=max(entry["usage"]["VmRSS"] for entry in samples),
        lifetime_peak_rss_kib=max(entry["usage"]["VmHWM"] for entry in samples),
        max_inline_rows=max(entry["optimizer"]["inline_rows"] for entry in samples),
        max_unindexed_rows=max(entry["optimizer"]["unindexed_rows"] for entry in samples),
        max_inline_bytes=max(entry["optimizer"]["inline_bytes"] for entry in samples),
        max_unindexed_bytes=max(entry["optimizer"]["unindexed_bytes"] for entry in samples),
        source_files_collected=drain["source"]["collected_files"],
        source_gc_blocked_samples=sum(entry["source"]["blocked_by_transactions"] for entry in samples),
        samples=len(samples),
        baseline_rss_kib=next(entry for entry in events if entry["kind"] == "measured")["begin"]["VmRSS"],
        end_rss_kib=next(entry for entry in events if entry["kind"] == "measured")["end"]["VmRSS"],
    )


def run_case(args, root, count, dimension, fixture):
    case = root / f"{fixture}-rows-{count}-dim-{dimension}"
    case.mkdir(mode=0o700)
    read_fd, write_fd = os.pipe()
    command = [
        sys.executable,
        "-I",
        str(Path(__file__).resolve()),
        "--worker-case",
        str(case),
        "--events-fd",
        str(write_fd),
        "--rows",
        str(count),
        "--dimensions",
        str(dimension),
        "--fixtures",
        fixture,
        "--seconds",
        str(args.seconds),
        "--readers",
        str(args.readers),
        "--query-pause",
        str(args.query_pause),
        "--recovery-query-pause",
        str(args.recovery_query_pause),
    ]
    events = []
    with (case / "worker.log").open("w") as log:
        child = subprocess.Popen(command, pass_fds=(write_fd,), stdout=log, stderr=subprocess.STDOUT)
        os.close(write_fd)
        watchdog = threading.Timer(3 * args.seconds + 300, child.kill)
        watchdog.start()
        try:
            with os.fdopen(read_fd, encoding="utf-8") as pipe, (case / "events.jsonl").open("w") as trace:
                for line in pipe:
                    trace.write(line)
                    events.append(json.loads(line))
            child.wait(timeout=30)
            require(child.returncode == 0, "Load worker failed: " + (case / "worker.log").read_text()[-6000:])
        finally:
            watchdog.cancel()
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
    result = dict(
        status="completed",
        fixture=fixture,
        rows=count,
        dimension=dimension,
        runtime=next(entry for entry in events if entry["kind"] == "ready"),
        **validate(events, count, dimension, fixture),
        **summarize(events),
    )
    result["correctness"] = "passed"
    if not result["canary_gate_passed"]:
        result.update(status="failed", error="Qualified canary fell below its unchanged 0.8 recall gate")
    (case / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output-dir", type=Path)
    destination.add_argument("--worker-case", type=Path)
    parser.add_argument("--events-fd", type=int)
    parser.add_argument("--fixtures", choices=["cost", "automatic"], nargs="+", default=["automatic"])
    parser.add_argument("--rows", type=int, nargs="+", default=[4096])
    parser.add_argument("--dimensions", type=int, nargs="+", default=[8])
    parser.add_argument("--seconds", type=float, default=12)
    parser.add_argument("--readers", type=int, default=2)
    parser.add_argument("--query-pause", type=float, default=0.02)
    parser.add_argument("--recovery-query-pause", type=float, default=0.5)
    args = parser.parse_args()
    require(os.environ.get("VANE_RUNNER") == "local-fast", "Set VANE_RUNNER=local-fast explicitly")
    require(
        all(512 <= n <= 32768 for n in args.rows) and all(8 <= n <= 128 for n in args.dimensions),
        "Invalid qualification size or dimension",
    )
    require(
        2 <= args.seconds <= 1800 and 2 <= args.readers <= 4 and 0 <= args.query_pause <= 1,
        "Invalid workload configuration",
    )
    require(0.25 <= args.recovery_query_pause <= 5, "Invalid planned recovery query interval")
    if args.worker_case:
        require(
            args.events_fd is not None and len(args.rows) == len(args.dimensions) == len(args.fixtures) == 1,
            "Worker requires one case and its event pipe",
        )
        worker(args)
        return
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700, parents=True)
    report = dict(
        status="running",
        cases=[],
        fixtures=args.fixtures,
        platform=platform.platform(),
        cpu_count=os.cpu_count(),
        reader_threads=args.readers,
        seconds_per_phase=args.seconds,
        query_pause_seconds=args.query_pause,
        recovery_query_pause_seconds=args.recovery_query_pause,
    )
    started = time.monotonic()
    failed = False
    try:
        for fixture in args.fixtures:
            for count in args.rows:
                for dimension in args.dimensions:
                    try:
                        result = run_case(args, root, count, dimension, fixture)
                    except Exception as error:
                        failed = True
                        result = dict(
                            status="failed", fixture=fixture, rows=count, dimension=dimension, error=str(error)
                        )
                        (root / f"{fixture}-rows-{count}-dim-{dimension}" / "report.json").write_text(
                            json.dumps(result, indent=2) + "\n"
                        )
                        print(f"Load case failed: {count} rows / {dimension}D; see worker.log", flush=True)
                    else:
                        print(
                            f"Sustained load measured: {count} rows / {dimension}D; {result['queries']} queries",
                            flush=True,
                        )
                    failed = failed or result["status"] == "failed"
                    report["cases"].append(result)
        report["status"] = "completed_with_failures" if failed else "completed"
        report["correctness"] = (
            "passed" if all(case.get("correctness") == "passed" for case in report["cases"]) else "not_fully_qualified"
        )
        report["canary_recall_gate"] = (
            "passed" if all(case.get("canary_gate_passed") for case in report["cases"]) else "not_fully_qualified"
        )
        report["exploration_meets_0_8_target"] = not failed and all(
            case["exploration_meets_0_8_target"] for case in report["cases"]
        )
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        report["seconds"] = time.monotonic() - started
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
