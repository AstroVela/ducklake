# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-FileCopyrightText: 2026 AstroVela contributors
# SPDX-License-Identifier: Apache-2.0

"""Execute the Vane README provider walkthrough on an owned default-Ray cluster."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ray_cluster():
    import ray
    import vane
    from ray.cluster_utils import Cluster

    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")
    if ray.is_initialized():
        raise RuntimeError("the integration suite must own its Ray cluster")
    environment = pytest.MonkeyPatch()
    environment.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    environment.setenv("RAY_task_events_report_interval_ms", "100")
    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=128 * 1024 * 1024,
        )
        for _ in range(2):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=128 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        deadline = time.monotonic() + 30
        while True:
            nodes = frozenset(
                str(node["NodeID"])
                for node in ray.nodes()
                if node.get("Alive") and (node.get("Resources") or {}).get("CPU", 0) >= 1
            )
            if len(nodes) == 2:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("expected two Ray execution nodes")
            time.sleep(0.1)
        yield nodes
    finally:
        try:
            vane.teardown_runner()
        finally:
            try:
                ray.shutdown()
            finally:
                try:
                    cluster.shutdown()
                finally:
                    environment.undo()


@pytest.fixture
def default_ray_runtime(ray_cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import vane
    from vane import runners

    assert "VANE_RUNNER" not in os.environ
    assert len(ray_cluster) == 2
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "2")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "shuffle"))
    vane.teardown_runner()
    runner = runners.get_or_create_runner()
    assert runner.name == "ray"
    try:
        yield runner
    finally:
        vane.teardown_runner()


pytestmark = [
    pytest.mark.real_ray,
    pytest.mark.ray_cluster_owner,
    pytest.mark.usefixtures("default_ray_runtime"),
]


def test_readme(tmp_path, monkeypatch):
    from vane import runners

    readme = Path(__file__).resolve().parents[2] / "VANE_README.md"
    monkeypatch.chdir(tmp_path)
    ns = {}
    runner = runners.get_or_create_runner()
    counts = {"reads": 0, "writes": 0}
    oldread, oldwrite = runner.run_iter_tables, runner.run_write

    def read(*a, **kw):
        counts["reads"] += 1
        return oldread(*a, **kw)

    def write(*a, **kw):
        counts["writes"] += 1
        return oldwrite(*a, **kw)

    monkeypatch.setattr(runner, "run_iter_tables", read)
    monkeypatch.setattr(runner, "run_write", write)
    try:
        for n, m in enumerate(re.finditer(r"^```python\n(.*?)^```", readme.read_text(), re.M | re.S), 1):
            print("README block", n, flush=True)
            exec(compile(m[1], str(readme), "exec"), ns)
            assert "VANE_RUNNER" not in os.environ and runner.name == "ray"
        c = ns["connection"]
        expected = [(i, "merged" if i == 0 else "updated" if i < 5 else f"value-{i}") for i in range(1005)] + [
            (1010, "new")
        ]
        assert sorted(c.sql("SELECT id,payload FROM lake.events").fetchall()) == expected
        assert c.sql("SELECT count(*),sum(id)::BIGINT FROM lake.events").fetchall() == [(1006, 505520)]
        assert c.sql("SELECT count(*),sum(id)::BIGINT FROM lake.events WHERE id>=100").fetchall() == [(906, 500570)]
        assert sorted(c.sql("SELECT id,payload FROM lake.events AT (VERSION => 1)").fetchall()) == [
            (i, f"value-{i}") for i in range(1000)
        ]
        assert counts["writes"] == 5, counts
        print("PASS blocks", n, "dispatch", counts, flush=True)
    finally:
        if "connection" in ns:
            ns["connection"].close()
