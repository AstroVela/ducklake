#!/usr/bin/env python3
"""Exercise DuckLake scans through a packaged two-worker Vane Ray runtime."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
import uuid
from pathlib import Path

WORKER_COUNT = 2
FILE_COUNT = 8
ROWS_PER_FILE = 256
ROW_COUNT = FILE_COUNT * ROWS_PER_FILE


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def verify_extension_is_wheel_linked(connection: object) -> None:
    extension = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain ducklake")
    require_equal(extension[1], "STATICALLY_LINKED", "ducklake install mode before LOAD")
    connection.execute("LOAD parquet")
    connection.execute("LOAD ducklake")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "ducklake after LOAD")


def create_two_worker_cluster(ray: object) -> object:
    from ray.cluster_utils import Cluster

    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=100 * 1024 * 1024,
        )
        for _ in range(WORKER_COUNT):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=100 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        return cluster
    except BaseException:
        cluster.shutdown()
        raise


def execution_node_ids(ray: object) -> set[str]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        node_ids = {
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive") and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
        }
        if len(node_ids) == WORKER_COUNT:
            return node_ids
        time.sleep(0.25)
    raise AssertionError(f"expected {WORKER_COUNT} live Ray execution nodes")


class AnnotateWorkerNode:
    """Record the Ray node that consumes each DuckLake-backed batch."""

    def __call__(self, table: object) -> object:
        import pyarrow as pa
        import ray

        time.sleep(0.25)
        node_id = str(ray.get_runtime_context().get_node_id())
        return pa.table(
            {
                "id": table.column("id"),
                "worker_node_id": [node_id] * table.num_rows,
            }
        )


class NoOutputHandle:
    def __init__(self, vane: object, task: object, partition_id: int) -> None:
        from vane.runners.fte import FteTaskAttemptId, FteTaskId

        context = task.context()
        query_id = str(context["query_id"])
        self._vane = vane
        self.task_context_info = task.task_context()
        self.task_id = FteTaskAttemptId(FteTaskId(query_id, 0, partition_id), 0)
        self.worker_id = "capture-worker"

    def done(self) -> bool:
        return True

    def get_result_sync(self) -> object:
        return self._vane.ray_cxx.RayTaskResult.no_output()

    def ack(self) -> None:
        pass

    def release_result_payload(self) -> None:
        pass


class CaptureWorkerPlanBackend:
    def __init__(self, vane: object) -> None:
        self.vane = vane
        self.fragments: dict[str, object] = {}
        self.handles: list[NoOutputHandle] = []

    def register_query_owner(self, query_id: str, owner_query_id: str) -> None:
        require_true(bool(query_id) and bool(owner_query_id), "captured query identity")

    def worker_snapshots(self) -> list[dict[str, object]]:
        return [
            {
                "worker_id": "capture-worker",
                "num_cpus": 1.0,
                "num_gpus": 0.0,
                "total_memory_bytes": 1024 * 1024 * 1024,
            }
        ]

    def submit_tasks(self, tasks: object) -> list[NoOutputHandle]:
        handles = []
        for task in tasks:
            fragment = task.plan()
            inputs = task.Inputs()
            if fragment is not None:
                for node_id, task_input in inputs.items():
                    if task_input.get("kind") == "scan_split_batch":
                        self.fragments[str(node_id)] = fragment
            handles.append(NoOutputHandle(self.vane, task, len(self.handles) + len(handles)))
        self.handles.extend(handles)
        return handles

    def task_input_stream_exhausted(self, query_id: str, source_node_ids: object) -> list[object]:
        return []

    def fte_query_status(self, query_id: str) -> dict[str, object]:
        return {
            "finished": True,
            "failed": False,
            "selected_attempt_task_ids": [str(handle.task_id) for handle in self.handles],
            "message": "finished",
        }

    def drop_query(self, query_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


async def collect_result_stream_async(stream: object) -> list[object]:
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    stream.set_ready_callback(loop, ready.set)
    results = []
    try:
        while True:
            try:
                item = stream.next_nowait()
            except (StopIteration, StopAsyncIteration):
                return results
            except RuntimeError as error:
                if "StopIteration" in str(error):
                    return results
                raise
            if item is not None:
                results.append(item)
                continue
            ready.clear()
            stream.arm_ready_notification()
            await ready.wait()
    finally:
        stream.clear_ready_callback()


def make_physical_plan(vane: object, connection: object, query: str) -> object:
    relation = connection.sql(query)
    return vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        relation,
        f"vane-ducklake-plan-{uuid.uuid4().hex}",
    ).to_physical_plan(connection)


def scan_split_count(vane: object, connection: object, query: str) -> int:
    plan = make_physical_plan(vane, connection, query)
    return sum(len(batches) for batches in plan.scan_split_batch_map().values())


def single_scan_source(plan: object) -> tuple[str, list[bytes]]:
    split_map = dict(plan.scan_split_batch_map())
    require_equal(len(split_map), 1, "single distributed scan source")
    node_id, batches = next(iter(split_map.items()))
    require_true(bool(batches), "explicit scan split batches")
    return str(node_id), list(batches)


def capture_worker_fragment(vane: object, connection: object, plan: object, node_id: str) -> object:
    backend = CaptureWorkerPlanBackend(vane)
    runner = vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    try:
        asyncio.run(collect_result_stream_async(runner.run_plan(plan, connection)))
    finally:
        runner.shutdown()
    fragment = backend.fragments.get(node_id)
    if fragment is None:
        raise AssertionError(f"worker fragment for scan node {node_id} was not captured")
    return fragment


def worker_connection(vane: object) -> object:
    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    verify_extension_is_wheel_linked(connection)
    require_equal(
        connection.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'").fetchone(),
        (0,),
        "worker catalog isolation",
    )
    return connection


def require_error(call: object, message: str, description: str) -> None:
    try:
        call()
    except Exception as error:
        if message.lower() not in str(error).lower():
            raise AssertionError(f"{description}: unexpected error: {error}") from error
        return
    raise AssertionError(f"{description}: expected an error")


def verify_worker_transport(vane: object, connection: object) -> None:
    query = "SELECT id, payload FROM lake.source WHERE id BETWEEN 0 AND 3"
    plan = make_physical_plan(vane, connection, query)
    node_id, batches = single_scan_source(plan)
    fragment = capture_worker_fragment(vane, connection, plan, node_id)
    first_worker = worker_connection(vane)
    second_worker = worker_connection(vane)
    native_runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
    try:
        first_template = fragment.clone(first_worker)
        second_template = first_template.clone(second_worker)
        require_equal(
            second_worker.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lake'").fetchone(),
            (0,),
            "worker plan does not attach the coordinator catalog",
        )
        require_error(
            lambda: native_runner.execute_native(second_worker.cursor(), first_template.clone(second_worker)),
            "assignment",
            "missing worker split assignment",
        )
        require_error(
            lambda: vane.ray_cxx.merge_scan_split_batches([batches[0], batches[0]]),
            "duplicate",
            "duplicate worker split assignment",
        )
        require_error(
            lambda: native_runner.execute_native(
                second_worker.cursor(),
                first_template.clone(second_worker),
                scan_split_batch={node_id: b"malformed"},
            ),
            "",
            "malformed worker split assignment",
        )
        merged_batch = vane.ray_cxx.merge_scan_split_batches(batches)
        result = native_runner.execute_native(
            second_worker.cursor(),
            second_template,
            scan_split_batch={node_id: merged_batch},
        )
        require_equal(result.completion_status, "ok", "worker-template execution status")
        rows = []
        for table in result.partition_payloads:
            rows.extend(zip(table.column(0).to_pylist(), table.column(1).to_pylist()))
        require_equal(
            sorted(rows),
            [(value, f"value-{value}") for value in range(4)],
            "worker-template rows",
        )
    finally:
        try:
            native_runner.shutdown()
        finally:
            second_worker.close()
            first_worker.close()


def verify_stale_split_rejected(
    vane: object,
    connection: object,
    table_name: str,
    fresh_query: str,
    mutate: object,
    description: str,
) -> None:
    old_plan = make_physical_plan(vane, connection, f"SELECT id FROM lake.{table_name}")
    _, old_batches = single_scan_source(old_plan)
    mutate()
    fresh_plan = make_physical_plan(vane, connection, fresh_query)
    fresh_node_id, _ = single_scan_source(fresh_plan)
    fragment = capture_worker_fragment(vane, connection, fresh_plan, fresh_node_id)
    worker = worker_connection(vane)
    native_runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
    try:
        template = fragment.clone(worker)
        stale_batch = vane.ray_cxx.merge_scan_split_batches(old_batches)
        require_error(
            lambda: native_runner.execute_native(
                worker.cursor(),
                template,
                scan_split_batch={fresh_node_id: stale_batch},
            ),
            "metadata does not match",
            description,
        )
    finally:
        try:
            native_runner.shutdown()
        finally:
            worker.close()


def seed_tables(connection: object, root: Path) -> None:
    configured_runner = os.environ.get("VANE_RUNNER")
    os.environ["VANE_RUNNER"] = "local-fast"
    try:
        connection.execute(
            f"ATTACH {sql_string('ducklake:' + str(root / 'metadata.ducklake'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0)"
        )
        connection.execute("CREATE TABLE lake.source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.empty_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.inlined_delete_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mapped_source(old_id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.stale_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.schema_source(id INTEGER, payload VARCHAR)")
        for file_index in range(FILE_COUNT):
            start = file_index * ROWS_PER_FILE
            stop = start + ROWS_PER_FILE
            connection.execute(
                "INSERT INTO lake.source "
                "SELECT i::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
        connection.execute("DELETE FROM lake.source WHERE id = 17")
        connection.execute(
            "INSERT INTO lake.inlined_delete_source "
            "SELECT i::INTEGER, ('inline-' || i::VARCHAR)::VARCHAR FROM range(32) AS rows(i)"
        )
        connection.execute("CALL lake.set_option('data_inlining_row_limit', 10, table_name => 'inlined_delete_source')")
        connection.execute("DELETE FROM lake.inlined_delete_source WHERE id = 5")

        external_file = root / "mapped-source.parquet"
        connection.execute(
            "COPY (SELECT 42::INTEGER AS old_id, 'mapped'::VARCHAR AS payload) "
            f"TO {sql_string(external_file)} (FORMAT PARQUET)"
        )
        connection.execute(f"CALL ducklake_add_data_files('lake', 'mapped_source', {sql_string(external_file)})")
        connection.execute("ALTER TABLE lake.mapped_source RENAME COLUMN old_id TO id")
        connection.execute("ALTER TABLE lake.mapped_source ADD COLUMN added INTEGER DEFAULT 7")
        connection.execute("INSERT INTO lake.mapped_source VALUES (43, 'new', 9)")
        connection.execute("INSERT INTO lake.stale_source VALUES (1, 'old')")
        connection.execute("INSERT INTO lake.schema_source VALUES (1, 'old')")
    finally:
        if configured_runner is None:
            os.environ.pop("VANE_RUNNER", None)
        else:
            os.environ["VANE_RUNNER"] = configured_runner


def reopen_lake_read_only(connection: object, root: Path) -> None:
    configured_runner = os.environ.get("VANE_RUNNER")
    os.environ["VANE_RUNNER"] = "local-fast"
    try:
        connection.execute("DETACH lake")
        connection.execute(
            f"ATTACH {sql_string('ducklake:' + str(root / 'metadata.ducklake'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, READ_ONLY)"
        )
    finally:
        if configured_runner is None:
            os.environ.pop("VANE_RUNNER", None)
        else:
            os.environ["VANE_RUNNER"] = configured_runner


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "ray":
        raise RuntimeError("the distributed wheel integration test requires VANE_RUNNER=ray")
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the Ray wheel integration test must own its Ray cluster")

    cluster = create_two_worker_cluster(ray)
    connection = None
    runner = None
    original_run_iter_tables = None
    try:
        expected_nodes = execution_node_ids(ray)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "configured Vane runner")

        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        verify_extension_is_wheel_linked(connection)

        with tempfile.TemporaryDirectory(prefix="vane-ducklake-ray-") as temporary_directory:
            root = Path(temporary_directory)
            seed_tables(connection, root)

            verify_worker_transport(vane, connection)
            verify_stale_split_rejected(
                vane,
                connection,
                "stale_source",
                "SELECT id FROM lake.stale_source",
                lambda: connection.execute("INSERT INTO lake.stale_source VALUES (2, 'new')"),
                "stale snapshot split",
            )
            verify_stale_split_rejected(
                vane,
                connection,
                "schema_source",
                "SELECT id, added FROM lake.schema_source",
                lambda: connection.execute("ALTER TABLE lake.schema_source ADD COLUMN added INTEGER DEFAULT 7"),
                "stale schema split",
            )
            reopen_lake_read_only(connection, root)

            dispatch_count = 0
            original_run_iter_tables = runner.run_iter_tables

            def record_distributed_read(*args: object, **kwargs: object) -> object:
                nonlocal dispatch_count
                dispatch_count += 1
                return original_run_iter_tables(*args, **kwargs)

            runner.run_iter_tables = record_distributed_read

            require_equal(
                scan_split_count(vane, connection, "SELECT id, payload FROM lake.source"),
                FILE_COUNT,
                "independently schedulable data-file splits",
            )
            require_equal(
                connection.sql("SELECT count(*)::BIGINT FROM lake.source").fetchall(),
                [(ROW_COUNT - 1,)],
                "distributed delete-aware row count",
            )
            require_equal(
                connection.sql("SELECT payload FROM lake.source WHERE id BETWEEN 510 AND 514 ORDER BY id").fetchall(),
                [(f"value-{value}",) for value in range(510, 515)],
                "distributed projection and filter pushdown",
            )
            require_equal(
                connection.sql(
                    "SELECT file_index, min(id), max(id) FROM lake.source " "GROUP BY file_index ORDER BY file_index"
                ).fetchall(),
                [
                    (file_index, file_index * ROWS_PER_FILE, (file_index + 1) * ROWS_PER_FILE - 1)
                    for file_index in range(FILE_COUNT)
                ],
                "stable coordinator file indexes",
            )
            require_equal(
                connection.sql("SELECT count(*)::BIGINT FROM lake.empty_source").fetchall(),
                [(0,)],
                "distributed empty table scan",
            )
            require_equal(
                scan_split_count(vane, connection, "SELECT id FROM lake.empty_source"),
                1,
                "explicit empty split",
            )
            require_equal(
                connection.sql("SELECT count(*)::BIGINT FROM lake.inlined_delete_source").fetchall(),
                [(31,)],
                "distributed inlined file deletion",
            )
            require_equal(
                scan_split_count(
                    vane,
                    connection,
                    "SELECT id, payload, added FROM lake.mapped_source",
                ),
                2,
                "schema-evolved mapped file splits",
            )
            require_equal(
                connection.sql("SELECT id, payload, added FROM lake.mapped_source ORDER BY id").fetchall(),
                [(42, "mapped", 7), (43, "new", 9)],
                "distributed name mapping and schema evolution",
            )
            annotated_rows = (
                connection.sql("SELECT id, payload FROM lake.source")
                .map_batches(
                    AnnotateWorkerNode,
                    schema={
                        "id": vane.sqltype("INTEGER"),
                        "worker_node_id": vane.sqltype("VARCHAR"),
                    },
                    batch_size=64,
                    cpus=1.0,
                    execution_backend="ray_actor",
                    actor_number=WORKER_COUNT,
                    target_max_batch_bytes=4096,
                )
                .fetchall()
            )
            require_equal(len(annotated_rows), ROW_COUNT - 1, "annotated distributed row count")
            observed_nodes = {str(row[1]) for row in annotated_rows}
            require_equal(observed_nodes, expected_nodes, "two-worker DuckLake scan topology")
            require_true(dispatch_count >= 4, "DuckLake queries did not use the Ray runner")
    finally:
        if runner is not None and original_run_iter_tables is not None:
            runner.run_iter_tables = original_run_iter_tables
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                if ray.is_initialized():
                    ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()
