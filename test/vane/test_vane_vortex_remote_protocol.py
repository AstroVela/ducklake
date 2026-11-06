#!/usr/bin/env python3
"""Deterministic R6 protocol scenarios, bounded schedules and unsafe-protocol counterexamples."""

from __future__ import annotations

import argparse
from collections import Counter, deque
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import time


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


P, O = load("vortex_remote_protocol"), load("vortex_remote_oracle")
THREAD_ENV = (
    "OMP_NUM_THREADS",
    "OMP_DYNAMIC",
    "OMP_PROC_BIND",
    "OMP_PLACES",
    "GOMP_CPU_AFFINITY",
    "KMP_AFFINITY",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "RAYON_NUM_THREADS",
    "TORCH_NUM_THREADS",
    "TOKIO_WORKER_THREADS",
)
RULE = {
    "schema": 1,
    "mode": "protocol_model_only",
    "production_changes": False,
    "kernels": {"publication": {"depth": 10, "state_cap": 50000}, "late-upload-gc": {"depth": 12, "state_cap": 50000}},
    "acceptance": "No invariant violations in all explored states; all deliberate unsafe variants must produce a counterexample.",
    "qualification": "No real PostgreSQL/S3, cloud durability, ANN quality, distributed serving or stable-load qualification.",
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class Harness:
    def __init__(self, mutant="", **fixture):
        self.model = P.Protocol(mutant, **fixture)
        self.trace, self.rejected = [], Counter()
        O.inspect(self.model)

    def command(self, *command, rejection=None):
        before = self.model.clone()
        try:
            result = self.model.step(command)
        except P.Rejected as error:
            require(self.model.fingerprint() == before.fingerprint(), "Rejected command partially changed state")
            require(rejection == str(error), f"Unexpected rejection {error}: {command}")
            self.rejected[str(error)] += 1
            self.trace.append({"command": command, "rejected": str(error)})
            O.inspect(self.model)
            return None
        self.trace.append({"command": command, "result": result})
        require(rejection is None, "Expected rejection: " + str(command))
        O.inspect(self.model, before, command, result)
        return result

    def ready(self, name, token, expected, source):
        self.command("prepare", name, token, expected, source)
        for role in ("index", "manifest"):
            if role == "manifest":
                self.command("manifest", name, token)
            message = self.command("upload", name, role, token)
            self.command("deliver", message)
            self.command("seal", name, role, token)

    def staged(self, name, token):
        self.command("begin", name, token)
        for part in ("head", "refs", "receipt"):
            self.command("stage", name, part, token)

    def publish(self, name, token, expected, source, ack=True):
        self.ready(name, token, expected, source)
        self.staged(name, token)
        return self.command("commit", name, token, ack)

    def collect(self, unit, token, round_, start=True):
        if start:
            self.command("gc_round", round_, token)
            self.command("plan_gc", unit, token, round_)
        for key in self.model.m["units"][unit]["files"]:
            message = self.command("stop", unit, key, token, round_)
            if message is not None:
                self.command("deliver", message, False)
                self.command("stop", unit, key, token, round_)
            target = self.model.m["gc"][unit]["targets"][key]
            if target is not None and self.model.version(key, target) is not None:
                message = self.command("delete_payload", unit, key, token, round_)
                self.command("deliver", message, False)
        return self.command("finish_gc", unit, token, round_)


def recorded(report, name, fn, **fixture):
    h = Harness(**fixture)
    fn(h)
    report["scenarios"].append(
        {
            "name": name,
            "fixture": fixture,
            "steps": len(h.trace),
            "rejections": dict(h.rejected),
            "trace": h.trace,
            "trace_sha256": P.digest(P.compact(h.trace)),
            "final_state_sha256": P.digest(h.model.fingerprint()),
        }
    )


def scenarios(report):
    def lost_ack(h):
        result = h.publish("a", 1, 1, "source-b", False)
        require(result is None, "ACK was not lost")
        receipt = h.command("reconcile", "a", h.model.request("a")["digest"])
        before = h.model.fingerprint()
        require(
            h.command("commit", "a", 1) == receipt and h.model.fingerprint() == before, "Retry repeated publication"
        )
        h.publish("b", 1, 2, "source-a")
        require(
            h.command("reconcile", "a", h.model.request("a")["digest"]) == receipt,
            "Reconciliation followed the new head",
        )
        h.command("reconcile", "a", "changed", rejection="PAYLOAD_MISMATCH")

    recorded(report, "lost-ACK-original-receipt-and-newer-head", lost_ack)

    def fenced(h):
        h.ready("a", 1, 1, "source-b")
        h.staged("a", 1)
        h.command("takeover", 1)
        h.command("commit", "a", 1, rejection="FENCED")
        h.command("adopt", "a", 2)
        h.staged("a", 2)
        h.command("commit", "a", 2)
        h.command("takeover", 1, rejection="OWNER_CAS_CONFLICT")

    recorded(report, "fence-rechecked-at-commit-with-explicit-adoption", fenced)

    def competing(h):
        h.ready("a", 1, 1, "source-a")
        h.ready("b", 1, 1, "source-b")
        h.staged("a", 1)
        h.staged("b", 1)
        h.command("commit", "a", 1)
        h.command("commit", "b", 1, rejection="REVISION_CONFLICT")
        h.command("takeover", 1)
        h.command("adopt", "b", 2)
        h.command("begin", "b", 2, rejection="REVISION_CONFLICT")
        h.command("prepare", "b", 2, 2, "source-b", rejection="PAYLOAD_MISMATCH")

    recorded(report, "competing-revisions-do-not-rebase-a-retry", competing)

    def incomplete(h):
        h.command("prepare", "a", 1, 1, "source-b")
        h.command("begin", "a", 1, rejection="UNSEALED_PUBLICATION")
        h.command("manifest", "a", 1, rejection="INDEX_NOT_SEALED")
        h.command("seal", "a", "index", 1, rejection="UPLOAD_INCOMPLETE")
        h.command("upload", "a", "index", 1)
        h.command("deliver", "put:a:index:1", False)
        key = h.model.request("a")["index"]
        h.model.store[key][-1]["sha"] = "corrupt-checksum"
        h.command("seal", "a", "index", 1, rejection="IDENTITY_MISMATCH")

    recorded(report, "partial-upload-and-mismatched-identity-rejected", incomplete)

    def versions(h):
        h.command("prepare", "a", 1, 1, "source-b")
        h.command("upload", "a", "index", 1)
        h.command("deliver", "put:a:index:1")
        h.command("seal", "a", "index", 1, "unknown-version", rejection="UPLOAD_INCOMPLETE")
        h.command("seal", "a", "index", 1)
        h.command("upload", "a", "manifest", 1, rejection="NOT_UPLOADABLE")
        h.command("prepare", "a", 1, 1, "source-a", rejection="PAYLOAD_MISMATCH")
        h.command("upload", "a", "index", 1, rejection="RPC_ALREADY_REGISTERED")

    recorded(report, "explicit-object-versions-and-request-identity", versions)

    def cancel_between_stage_commit(h):
        h.ready("a", 1, 1, "source-b")
        h.staged("a", 1)
        h.command("cancel", "a", 1)
        h.command("gc_round", "g", 1)
        h.command("plan_gc", "generation:a", 1, "g")
        h.command("commit", "a", 1, rejection="REQUEST_CLOSED")
        h.collect("generation:a", 1, "g", False)

    recorded(report, "cancellation-and-GC-between-staging-and-commit", cancel_between_stage_commit)

    def readers(h):
        h.command("capture", "old")
        h.command("capture", "ordinary", "sql", "source-a")
        h.publish("a", 1, 1, "source-b")
        h.command("gc_round", "g", 1)
        h.command("plan_gc", "generation:seed", 1, "g", rejection="REFERENCED")
        h.command("expire_native", "source-a", 1, rejection="ORDINARY_TRANSACTION")
        h.command("takeover", 1)
        h.command("materialize", "old")
        h.command("release_copy_pin", "old")
        old_copy = P.compact(h.model.local["copies"]["old"])
        h.collect("generation:seed", 2, "g2")
        h.command("close", "ordinary")
        h.command("expire_native", "source-a", 2)
        h.collect("source-a", 2, "source-round")
        require(P.compact(h.model.local["copies"]["old"]) == old_copy, "Private copy revoked by GC")
        h.command("close", "old")
        h.command("materialize", "old", rejection="VIEW_CLOSED")

    recorded(report, "ordinary-transactions-and-private-view-after-physical-GC", readers)

    # Source-a has no named or view reference after replacement; native expiry is the final source guard.
    def native_independent(h):
        h.publish("a", 1, 1, "source-b")
        h.command("gc_round", "source-round", 1)
        h.command("plan_gc", "source-a", 1, "source-round", rejection="REFERENCED")
        h.collect("generation:seed", 1, "generation-round")
        h.command("capture", "sql", "sql", "source-b")
        h.command("expire_native", "source-a", 1, rejection="ORDINARY_TRANSACTION")
        h.command("close", "sql")
        h.command("expire_native", "source-a", 1)
        h.collect("source-a", 1, "final-source")

    recorded(report, "native-retention-independent-of-named-generation-and-global-SQL-gate", native_independent)

    def late_put(h):
        h.command("prepare", "a", 1, 1, "source-b")
        h.command("upload", "a", "index", 1)
        h.command("duplicate", "put:a:index:1", "late-copy")
        h.command("cancel", "a", 1)
        h.command("takeover", 1)
        h.collect("generation:a", 2, "g")
        h.command("deliver", "put:a:index:1", False)
        h.command("deliver", "late-copy", False)
        require(
            all(h.model.wire[m]["result"] == "PRECONDITION_FAILED" for m in ("put:a:index:1", "late-copy")),
            "Late old uploads bypassed the ordinary STOP object",
        )

    recorded(report, "late-upload-after-GC-and-owner-transfer", late_put)

    def stop_probe_race(h):
        h.command("prepare", "a", 1, 1, "source-b")
        h.command("upload", "a", "index", 1)
        h.command("cancel", "a", 1)
        h.command("gc_round", "g", 1)
        h.command("plan_gc", "generation:a", 1, "g")
        key = h.model.request("a")["index"]
        first = h.command("stop", "generation:a", key, 1, "g")
        h.command("deliver", "put:a:index:1", False)
        h.command("deliver", first, False)
        require(h.model.wire[first]["result"] == "PRECONDITION_FAILED", "Conditional stop race did not occur")
        h.collect("generation:a", 1, "g", False)

    recorded(report, "payload-arrives-between-HEAD-and-conditional-STOP", stop_probe_race)

    def scope_and_budgets(h):
        h.command(
            "prepare",
            "wrong-table",
            1,
            1,
            "source-a",
            None,
            "00000000-0000-4000-8000-000000000003",
            1,
            rejection="STALE_REQUEST_IDENTITY",
        )
        h.command("prepare", "wrong-epoch", 1, 1, "source-a", None, P.TABLE, 2, rejection="STALE_REQUEST_IDENTITY")
        for size in (0, 64 * P.MIB + 1):
            h.command("prepare", "bad-size", 1, 1, "source-a", None, P.TABLE, 1, size, rejection="ARTIFACT_BUDGET")
        for rows, dim in ((0, 8), (100001, 1), (4097, 4096), (65, 4097)):
            h.command(
                "prepare",
                "bad-vectors",
                1,
                1,
                "source-a",
                None,
                P.TABLE,
                1,
                4096,
                rows,
                dim,
                rejection="ROW_VECTOR_BUDGET",
            )
        h.command(
            "prepare", "bad-segments", 1, 1, "source-a", None, P.TABLE, 1, 4096, 128, 8, 4, rejection="SEGMENT_BUDGET"
        )
        h.command("prepare", "a", 1, 1, "source-a")
        message = h.command("upload", "a", "index", 1)
        h.command("deliver", message)
        h.command("seal", "a", "index", 1)
        body_bytes = len(P.compact(h.model.descriptor(h.model.request("a"))).encode())
        h.command("manifest", "a", 1, P.MIB - body_bytes)
        h.command("manifest", "a", 1, P.MIB - body_bytes + 1, rejection="MANIFEST_BUDGET")
        h.command("manifest", "a", 1, 0, rejection="MANIFEST_CHANGED")
        h.command("cancel", "a", 1)
        for objects, size in ((0, 1), (17, 1), (1, 0), (1, 64 * P.MIB + 1)):
            h.command("gc_round", "invalid", 1, objects, size, rejection="GC_BUDGET")
        h.command("gc_round", "small", 1, 1, 4095)
        h.command("plan_gc", "generation:a", 1, "small", rejection="GC_BUDGET")
        h.command("gc_round", "exact", 1, 1, 4096)
        h.command("plan_gc", "generation:a", 1, "exact")
        h.command("stop", "generation:a", "foreign/unregistered", 1, "exact", rejection="KEY_NOT_OWNED")
        h.collect("generation:a", 1, "exact", False)
        original = h.model.fingerprint()
        h.command("finish_gc", "generation:a", 1, "exact")
        require(h.model.fingerprint() == original, "GC receipt replay changed accounting")

    recorded(report, "identity-and-symbolic-admission-boundaries", scope_and_budgets)

    def gc_object_boundary(h):
        h.command("gc_round", "sixteen", 1, 16, 64 * P.MIB)
        for n in range(17):
            name = "pending-" + str(n)
            h.command("prepare", name, 1, 1, "source-a")
            h.command("cancel", name, 1)
            if n < 16:
                h.command("plan_gc", "generation:" + name, 1, "sixteen")
            else:
                h.command("plan_gc", "generation:" + name, 1, "sixteen", rejection="GC_BUDGET")
        for n in range(16):
            h.collect("generation:pending-" + str(n), 1, "sixteen", False)

    recorded(report, "GC-sixteen-logical-units-and-seventeenth-rejection", gc_object_boundary)

    def gc_byte_boundary(h):
        h.command("gc_round", "bytes", 1, 16, 64 * P.MIB)
        for name, size in (("half-a", 32 * P.MIB), ("half-b", 32 * P.MIB), ("extra-byte", 1)):
            h.command("prepare", name, 1, 1, "source-a", None, P.TABLE, 1, size)
            h.command("cancel", name, 1)
            h.command(
                "plan_gc", "generation:" + name, 1, "bytes", rejection="GC_BUDGET" if name == "extra-byte" else None
            )
        h.collect("generation:half-a", 1, "bytes", False)
        h.collect("generation:half-b", 1, "bytes", False)

    recorded(report, "GC-exact-shared-64MiB-and-one-extra-byte-rejection", gc_byte_boundary)

    for size in (64 * P.MIB, 64 * P.MIB + 1):
        recorded(
            report,
            f"symbolic-source-envelope-{size}",
            lambda h, s=size: h.command(
                "prepare", "a", 1, 1, "source-b", rejection="SOURCE_BUDGET" if s > 64 * P.MIB else None
            ),
            source_b_bytes=size,
        )

    for rows, dimension in ((100000, 1), (100000, 167), (99864, 168), (4096, 4096)):
        recorded(
            report,
            f"row-vector-envelope-{rows}-{dimension}",
            lambda h, r=rows, d=dimension: h.command(
                "prepare", "a", 1, 1, "source-a", None, P.TABLE, 1, 64 * P.MIB, r, d, 3
            ),
        )

    publication = [
        ("prepare", "a", 1, 1, "source-b"),
        ("upload", "a", "index", 1),
        ("deliver", "put:a:index:1", False),
        ("seal", "a", "index", 1),
        ("manifest", "a", 1),
        ("upload", "a", "manifest", 1),
        ("deliver", "put:a:manifest:1", False),
        ("seal", "a", "manifest", 1),
        ("begin", "a", 1),
        ("stage", "a", "head", 1),
        ("stage", "a", "refs", 1),
        ("stage", "a", "receipt", 1),
        ("commit", "a", 1, False),
    ]
    for cut in range(1, len(publication) + 1):

        def recover(h, cut=cut):
            for command in publication[:cut]:
                h.command(*command)
            durable = P.compact({"m": h.model.m, "s": h.model.store, "q": h.model.wire})
            h.command("crash", 1)
            require(
                P.compact({"m": h.model.m, "s": h.model.store, "q": h.model.wire}) == durable,
                "Crash rewrote durable state",
            )
            image = json.loads(h.model.fingerprint())
            h.model = P.Protocol.restore(image)
            require(h.model.fingerprint() == P.compact(image), "Restore implicitly resumed work")
            h.command("takeover", 1)
            if h.model.request("a")["receipt"] is None:
                h.command("adopt", "a", 2)
                for role in ("index", "manifest"):
                    if role == "manifest" and h.model.request("a")["body"] is None:
                        h.command("manifest", "a", 2)
                    key = h.model.request("a")[role]
                    if not h.model.m["objects"][key]["sealed"]:
                        if h.model.head_object(key) is None:
                            pending = next(
                                (m for m, rpc in h.model.wire.items() if rpc["key"] == key and not rpc["delivered"]),
                                None,
                            )
                            if pending is None:
                                pending = h.command("upload", "a", role, 2)
                            h.command("deliver", pending, False)
                        h.command("seal", "a", role, 2)
                h.staged("a", 2)
                h.command("commit", "a", 2)
            receipt = h.command("reconcile", "a", h.model.request("a")["digest"])
            require(
                receipt["revision"] == 2 and h.model.m["revision"] == 2, "Crash recovery repeated or lost publication"
            )

        recorded(report, f"publication-logical-crash-after-{cut}", recover)

    for cut in ("intent", "stop-sent", "stop-applied", "stop-confirmed", "delete-sent", "delete-applied", "finished"):

        def resume_gc(h, cut=cut):
            h.ready("a", 1, 1, "source-b")
            h.command("cancel", "a", 1)
            h.command("gc_round", "g", 1)
            h.command("plan_gc", "generation:a", 1, "g")
            key = h.model.request("a")["index"]
            position = (
                "intent",
                "stop-sent",
                "stop-applied",
                "stop-confirmed",
                "delete-sent",
                "delete-applied",
                "finished",
            ).index(cut)
            if cut != "intent":
                stop = h.command("stop", "generation:a", key, 1, "g")
                if position >= 2:
                    h.command("deliver", stop, False)
                    if position >= 3:
                        h.command("stop", "generation:a", key, 1, "g")
                    if position >= 4:
                        deletion = h.command("delete_payload", "generation:a", key, 1, "g")
                        if position >= 5:
                            h.command("deliver", deletion, False)
            if cut == "finished":
                h.collect("generation:a", 1, "g", False)
            h.command("crash", 1)
            h.command("takeover", 1)
            if h.model.m["units"]["generation:a"]["state"] != "deleted":
                h.collect("generation:a", 2, "resumed")
            for message in list(h.model.wire):
                if not h.model.wire[message]["delivered"]:
                    h.command("deliver", message, False)
            original = h.model.fingerprint()
            h.command("finish_gc", "generation:a", 2, "resumed")
            require(original == h.model.fingerprint(), "Replayed GC advanced counters")

        recorded(report, "gc-logical-crash-" + cut, resume_gc)


def mutation_cases(report):
    def ready_commands(name="a", source="source-b"):
        return [
            ("prepare", name, 1, 1, source),
            ("upload", name, "index", 1),
            ("deliver", f"put:{name}:index:1"),
            ("seal", name, "index", 1),
            ("manifest", name, 1),
            ("upload", name, "manifest", 1),
            ("deliver", f"put:{name}:manifest:1"),
            ("seal", name, "manifest", 1),
        ]

    def stage_commands(name="a", token=1):
        return [("begin", name, token)] + [("stage", name, p, token) for p in ("head", "refs", "receipt")]

    def replay(mutant, commands):
        model = P.Protocol(mutant)
        trace = []
        violation = None
        for command in commands:
            if command[0] == "fixture-corrupt":
                key = model.request("a")["index"]
                model.store[key][-1]["sha"] = "corrupt-checksum"
                trace.append({"fixture": "corrupt-unsealed-object"})
                continue
            before = model.clone()
            try:
                result = model.step(command)
            except P.Rejected as error:
                require(
                    model.fingerprint() == before.fingerprint(), "Rejected mutant transition partially changed state"
                )
                trace.append({"command": command, "rejected": str(error)})
                continue
            trace.append({"command": command, "result": result})
            try:
                O.inspect(model, before, command, result)
            except O.Violation as error:
                violation = str(error)
                break
        require(violation is not None, "Unsafe variant escaped the independent oracle: " + mutant)
        report["counterexamples"].append({"mutant": mutant, "violation": violation, "trace": trace})

    ready, staged = ready_commands(), stage_commands()
    replay("skip_owner_fence", ready + staged + [("takeover", 1), ("commit", "a", 1)])
    replay("split_publication", ready + [("begin", "a", 1), ("stage", "a", "head", 1)])
    replay(
        "skip_revision_cas",
        ready
        + ready_commands("b", "source-a")
        + staged
        + stage_commands("b")
        + [("commit", "b", 1), ("commit", "a", 1)],
    )
    replay(
        "skip_commit_revalidation",
        ready
        + staged
        + [("cancel", "a", 1), ("gc_round", "g", 1), ("plan_gc", "generation:a", 1, "g"), ("commit", "a", 1)],
    )
    replay(
        "adopt_rebases_request",
        ready
        + ready_commands("b", "source-a")
        + stage_commands("b")
        + [("commit", "b", 1), ("takeover", 1), ("adopt", "a", 2)],
    )
    replay("skip_digest_validation", ready[:3] + [("fixture-corrupt",), ("seal", "a", "index", 1)])
    replay("mixed_readview", [("capture", "v")])
    replay("takeover_drops_pins", [("capture", "v"), ("takeover", 1)])
    replay("ignore_gc_references", [("gc_round", "g", 1), ("plan_gc", "generation:seed", 1, "g")])
    replay(
        "ignore_gc_budget",
        ready + [("cancel", "a", 1), ("gc_round", "g", 1, 1, 1), ("plan_gc", "generation:a", 1, "g")],
    )
    replay(
        "ignore_sql_gate",
        ready + staged + [("commit", "a", 1), ("capture", "sql", "sql", "source-b"), ("expire_native", "source-a", 1)],
    )
    replay(
        "reconcile_returns_head",
        ready + staged + [("commit", "a", 1), ("reconcile", "seed", P.digest("seed:source-a"))],
    )

    for mutant in (
        "omit_stop_barrier",
        "delete_current_version",
        "delete_stop_after_finish",
        "sweep_foreign_keys",
        "unconditional_put",
        "stop_is_delete_marker",
    ):
        h = Harness(mutant)
        try:
            if mutant == "omit_stop_barrier":
                h.command("prepare", "a", 1, 1, "source-b")
                h.command("upload", "a", "index", 1)
                h.command("cancel", "a", 1)
                h.command("gc_round", "g", 1)
                h.command("plan_gc", "generation:a", 1, "g")
                for key in h.model.m["units"]["generation:a"]["files"]:
                    h.command("stop", "generation:a", key, 1, "g")  # RPC deliberately remains in flight.
                    h.command("delete_payload", "generation:a", key, 1, "g")
                h.command("finish_gc", "generation:a", 1, "g")
                h.command("deliver", "put:a:index:1", False)
            else:
                h.command("prepare", "a", 1, 1, "source-b")
                h.command("upload", "a", "index", 1)
                h.command("duplicate", "put:a:index:1", "late-copy")
                h.command("deliver", "put:a:index:1")
                h.command("cancel", "a", 1)
                h.collect("generation:a", 1, "g")
                h.command("deliver", "late-copy", False)
        except O.Violation as error:
            report["counterexamples"].append({"mutant": mutant, "violation": str(error), "trace": h.trace})
        else:
            raise AssertionError("Unsafe variant escaped the independent oracle: " + mutant)


def explore(name, initial, actions, report):
    rule = RULE["kernels"][name]
    queue = deque([(initial, 0)])
    initial_text = initial.fingerprint()
    visited = {P.digest(initial_text): initial_text}
    depths, rejects, coverage = Counter(), Counter(), Counter()
    attempts, accepted = 0, 0
    while queue:
        model, depth = queue.popleft()
        depths[depth] += 1
        if depth == rule["depth"]:
            continue
        for command in actions:
            attempts += 1
            candidate = model.clone()
            try:
                result = candidate.step(command)
            except P.Rejected as error:
                require(candidate.fingerprint() == model.fingerprint(), "Rejected explored transition changed state")
                rejects[str(error)] += 1
                continue
            O.inspect(candidate, model, command, result)
            accepted += 1
            if candidate.m["head"] != "seed":
                coverage["publication_committed"] += 1
            if candidate.m["owner"] == 2:
                coverage["owner_transferred"] += 1
            if candidate.local["copies"]:
                coverage["private_view_copied"] += 1
            if candidate.m["units"].get("generation:a", {}).get("state") == "deleted":
                coverage["GC_completed"] += 1
                if any(rpc["op"] == "put" and not rpc["delivered"] for rpc in candidate.wire.values()):
                    coverage["GC_complete_with_old_upload_in_flight"] += 1
            for rpc in candidate.wire.values():
                if rpc["result"] == "PRECONDITION_FAILED":
                    coverage[rpc["op"] + "_conditional_race_rejected"] += 1
            text = candidate.fingerprint()
            key = P.digest(text)
            if key in visited:
                require(visited[key] == text, "Canonical-state hash collision")
                continue
            visited[key] = text
            require(len(visited) <= rule["state_cap"], "Predeclared exploration state cap exceeded")
            queue.append((candidate, depth + 1))
    needed = (
        ("publication_committed", "owner_transferred", "private_view_copied")
        if name == "publication"
        else (
            "GC_completed",
            "GC_complete_with_old_upload_in_flight",
            "put_conditional_race_rejected",
            "stop_conditional_race_rejected",
        )
    )
    require(all(coverage[k] for k in needed), "Exploration missed a required concurrency boundary")
    report["exploration"].append(
        {
            "kernel": name,
            "depth": rule["depth"],
            "states": len(visited),
            "depth_counts": dict(depths),
            "attempts": attempts,
            "accepted": accepted,
            "rejected": dict(rejects),
            "coverage": dict(coverage),
            "state_set_sha256": P.digest("\n".join(sorted(visited))),
        }
    )


def bounded_schedules(report):
    h = Harness()
    h.ready("a", 1, 1, "source-b")
    actions = [("begin", "a", t) for t in (1, 2)]
    actions += [("stage", "a", p, t) for t in (1, 2) for p in ("head", "refs", "receipt")]
    actions += [("commit", "a", t, False) for t in (1, 2)]
    actions += [
        ("takeover", 1),
        ("adopt", "a", 2),
        ("cancel", "a", 1),
        ("cancel", "a", 2),
        ("capture", "v"),
        ("materialize", "v"),
        ("release_copy_pin", "v"),
        ("close", "v"),
        ("crash", 1),
        ("crash", 2),
    ]
    explore("publication", h.model, actions, report)

    h = Harness()
    h.command("prepare", "a", 1, 1, "source-b")
    h.command("upload", "a", "index", 1)
    h.command("cancel", "a", 1)
    h.command("gc_round", "g", 1)
    h.command("plan_gc", "generation:a", 1, "g")
    req, unit = h.model.request("a"), "generation:a"
    actions = [
        ("deliver", "put:a:index:1", False),
        ("duplicate", "put:a:index:1", "late-copy"),
        ("deliver", "late-copy", False),
        ("finish_gc", unit, 1, "g"),
    ]
    for role in ("index", "manifest"):
        key = req[role]
        actions += [("stop", unit, key, 1, "g"), ("delete_payload", unit, key, 1, "g")]
        for target in (None, f"version:{key}:1"):
            actions.append(("deliver", f"stop:{unit}:{key}:{target}:1", False))
        actions.append(("deliver", f"delete:{unit}:{key}:version:{key}:1:1", False))
    explore("late-upload-gc", h.model, actions, report)


@contextmanager
def pinned(cpus):
    original = os.sched_getaffinity(0)
    require(set(cpus) <= original, "Requested CPU set is unavailable")
    try:
        os.sched_setaffinity(0, cpus)
        require(os.sched_getaffinity(0) == set(cpus), "Affinity was not applied exactly")
        yield
    finally:
        os.sched_setaffinity(0, original)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    cpus = [int(part) for part in args.cpus.split(",")]
    require(cpus and len(set(cpus)) == len(cpus) and min(cpus) >= 0, "Invalid CPU set")
    root = args.output_dir.resolve()
    root.mkdir(mode=0o700)
    source = [Path(__file__).resolve(), Path(P.__file__), Path(O.__file__)]
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source}
    threads = {name: os.environ.get(name) for name in THREAD_ENV}
    report = {
        "status": "running",
        "rule": RULE,
        "limits": P.LIMITS,
        "source_sha256": hashes,
        "threads": threads,
        "original_affinity": sorted(os.sched_getaffinity(0)),
        "scenarios": [],
        "counterexamples": [],
        "exploration": [],
    }
    (root / "rule.json").write_text(json.dumps(RULE, indent=2) + "\n")
    start = time.monotonic()
    try:
        with pinned(cpus):
            report["applied_affinity"] = sorted(os.sched_getaffinity(0))
            scenarios(report)
            mutation_cases(report)
            print(
                "scenarios and unsafe variants passed",
                len(report["scenarios"]),
                len(report["counterexamples"]),
                flush=True,
            )
            bounded_schedules(report)
            require(threads == {name: os.environ.get(name) for name in THREAD_ENV}, "Thread settings changed")
            require(
                hashes == {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source},
                "Executed source changed",
            )
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["restored_affinity"] = sorted(os.sched_getaffinity(0))
        report["seconds_observation_only"] = time.monotonic() - start
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "scenarios": len(report["scenarios"]),
                "counterexamples": len(report["counterexamples"]),
                "exploration": report["exploration"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
