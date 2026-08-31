#!/usr/bin/env python3
"""Exercise DuckLake scans and writes through a packaged two-worker Vane Ray runtime."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

WORKER_COUNT = 2
FILE_COUNT = 8
ROWS_PER_FILE = 256
ROW_COUNT = FILE_COUNT * ROWS_PER_FILE
CONFLICT_ID = f"{os.getpid()}-{uuid.uuid4().hex}"
CONFLICT_STARTED_PATH = Path("/tmp") / f"vane-ray-ducklake-conflict-{CONFLICT_ID}.started"
CONFLICT_RELEASE_PATH = Path("/tmp") / f"vane-ray-ducklake-conflict-{CONFLICT_ID}.release"
DISTRIBUTED_ARTIFACT_PREFIX = ".vane-ducklake-"


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def distributed_artifact_roots(connection: object, table_name: str) -> set[str]:
    rows = connection.execute(f"SELECT data_file FROM ducklake_list_files('lake', '{table_name}')").fetchall()
    roots = set()
    for (data_file,) in rows:
        candidates = [part for part in Path(data_file).parts if part.startswith(DISTRIBUTED_ARTIFACT_PREFIX)]
        require_equal(len(candidates), 1, f"{table_name} distributed artifact root count")
        write_id = candidates[0][len(DISTRIBUTED_ARTIFACT_PREFIX) :]
        try:
            uuid.UUID(write_id)
        except ValueError as error:
            raise AssertionError(f"{table_name} has an invalid distributed write identity: {write_id}") from error
        roots.add(candidates[0])
    return roots


def distributed_artifact_directories(root: Path) -> set[Path]:
    return {path for path in (root / "data").rglob(f"{DISTRIBUTED_ARTIFACT_PREFIX}*") if path.is_dir()}


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
    connection.execute("LOAD sqlite_scanner")
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


class WaitForCoordinatorConflict:
    """Hold worker output until another connection changes the target."""

    def __call__(self, table: object) -> object:
        CONFLICT_STARTED_PATH.touch()
        deadline = time.monotonic() + 90
        while not CONFLICT_RELEASE_PATH.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("timed out waiting for the DuckLake conflict mutation")
            time.sleep(0.05)
        return table


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


def require_error(call: object, message: str | tuple[str, ...], description: str) -> None:
    try:
        call()
    except Exception as error:
        messages = (message,) if isinstance(message, str) else message
        if not any(expected.lower() in str(error).lower() for expected in messages):
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
    with sqlite3.connect(root / "metadata.sqlite") as metadata_connection:
        journal_mode = metadata_connection.execute("PRAGMA journal_mode=WAL").fetchone()
    require_equal(journal_mode, ("wal",), "SQLite metadata journal mode")

    configured_runner = os.environ.get("VANE_RUNNER")
    os.environ["VANE_RUNNER"] = "local-fast"
    try:
        connection.execute(
            f"ATTACH {sql_string('ducklake:sqlite:' + str(root / 'metadata.sqlite'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0, BUSY_TIMEOUT 30000)"
        )
        connection.execute("CREATE TABLE lake.source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.empty_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.inlined_delete_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mapped_source(old_id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.stale_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.schema_source(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.not_null_write_target(id INTEGER NOT NULL, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.row_count_write_target(id INTEGER NOT NULL, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.schema_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.duplicate_path_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.footer_stats_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.inexact_stats_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.nested_write_target(payload STRUCT(value INTEGER), items INTEGER[])")
        connection.execute("CREATE TABLE lake.not_null_nested_target(payload STRUCT(value INTEGER) NOT NULL)")
        connection.execute("CREATE TABLE lake.rollback_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.partitioned_write_target(id INTEGER, category VARCHAR, payload VARCHAR)")
        connection.execute("ALTER TABLE lake.partitioned_write_target SET PARTITIONED BY (category)")
        connection.execute("CREATE TABLE lake.concurrent_write_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mutation_unpartitioned(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mutation_partitioned(id INTEGER, category VARCHAR, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mutation_legacy_mapping(old_id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.mutation_duplicate_delete(id INTEGER)")
        connection.execute("ALTER TABLE lake.mutation_partitioned SET PARTITIONED BY (category)")
        connection.execute("CREATE TABLE lake.merge_update_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_delete_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_partitioned_target(id INTEGER, category VARCHAR, payload VARCHAR)")
        connection.execute("ALTER TABLE lake.merge_partitioned_target SET PARTITIONED BY (category)")
        connection.execute("CREATE TABLE lake.merge_noop_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_by_source_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_retry_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_failure_target(id INTEGER, payload VARCHAR)")
        connection.execute("CREATE TABLE lake.merge_conflict_target(id INTEGER, payload VARCHAR)")
        for file_index in range(FILE_COUNT):
            start = file_index * ROWS_PER_FILE
            stop = start + ROWS_PER_FILE
            connection.execute(
                "INSERT INTO lake.source "
                "SELECT i::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
        for file_index in range(4):
            start = file_index * 128
            stop = start + 128
            connection.execute(
                "INSERT INTO lake.mutation_unpartitioned "
                "SELECT i::INTEGER, ('mutation-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
            connection.execute(
                "INSERT INTO lake.mutation_partitioned "
                "SELECT i::INTEGER, ('category-' || (i % 4)::VARCHAR)::VARCHAR, "
                "('partitioned-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
            connection.execute(
                "INSERT INTO lake.merge_update_target "
                "SELECT i::INTEGER, ('merge-old-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
            connection.execute(
                "INSERT INTO lake.merge_failure_target "
                "SELECT i::INTEGER, ('failure-old-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start + 512}, {stop + 512}) AS rows(i)"
            )
            connection.execute(
                "INSERT INTO lake.merge_conflict_target "
                "SELECT i::INTEGER, ('conflict-old-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start + 100}, {stop + 100}) AS rows(i)"
            )
        for file_index in range(4):
            start = 768 + file_index * 96
            stop = start + 96
            connection.execute(
                "INSERT INTO lake.merge_delete_target "
                "SELECT i::INTEGER, ('delete-old-' || i::VARCHAR)::VARCHAR "
                f"FROM range({start}, {stop}) AS rows(i)"
            )
        connection.execute(
            "INSERT INTO lake.merge_partitioned_target "
            "SELECT i::INTEGER, ('category-' || (i % 4)::VARCHAR)::VARCHAR, "
            "('partition-old-' || i::VARCHAR)::VARCHAR FROM range(1280, 1664) AS rows(i)"
        )
        connection.execute("DELETE FROM lake.source WHERE id = 17")
        connection.execute("DELETE FROM lake.mutation_unpartitioned WHERE id = 17")
        connection.execute("DELETE FROM lake.mutation_partitioned WHERE id = 18")
        connection.execute("DELETE FROM lake.merge_update_target WHERE id = 18")
        connection.execute("INSERT INTO lake.mutation_legacy_mapping VALUES (42, 'legacy'), (43, 'delete-me')")
        connection.execute("ALTER TABLE lake.mutation_legacy_mapping RENAME COLUMN old_id TO id")
        connection.execute(
            "UPDATE __ducklake_metadata_lake.ducklake_data_file SET mapping_id = NULL "
            "WHERE table_id = (SELECT table_id FROM __ducklake_metadata_lake.ducklake_table "
            "WHERE table_name = 'mutation_legacy_mapping')"
        )
        connection.execute("INSERT INTO lake.mutation_duplicate_delete VALUES (10)")
        connection.execute("INSERT INTO lake.mutation_duplicate_delete VALUES (20), (30)")
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
        connection.execute("INSERT INTO lake.concurrent_write_target VALUES (-1, 'seed')")
        connection.execute("INSERT INTO lake.merge_noop_target VALUES (1, 'noop-old'), (2, 'noop-keep')")
        connection.execute("INSERT INTO lake.merge_by_source_target VALUES (10, 'remove'), (11, 'keep')")
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
            f"ATTACH {sql_string('ducklake:sqlite:' + str(root / 'metadata.sqlite'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, READ_ONLY, BUSY_TIMEOUT 30000)"
        )
    finally:
        if configured_runner is None:
            os.environ.pop("VANE_RUNNER", None)
        else:
            os.environ["VANE_RUNNER"] = configured_runner


def capture_write_plan(vane: object, runner: object, operation: Callable[[], object]) -> object:
    captured = []
    original_run_write = runner.run_write

    def capture(relation: object) -> dict[str, object]:
        captured.append(
            vane.ray_cxx.PyLogicalPlan.from_duckdb_write_relation(
                relation,
                f"vane-ducklake-stale-{uuid.uuid4().hex}",
            )
        )
        return {}

    runner.run_write = capture
    try:
        operation()
    finally:
        runner.run_write = original_run_write
    require_equal(len(captured), 1, "captured DuckLake write plan count")
    return captured[0]


def capture_physical_write_plan(
    vane: object,
    connection: object,
    runner: object,
    operation: Callable[[], object],
) -> object:
    return capture_write_plan(vane, runner, operation).to_physical_plan(connection)


def require_concurrent_write_conflict(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
    require_write_count: Callable[[int, str], None],
    operation: Callable[[object], None],
    conflict_sql: str,
    description: str,
) -> None:
    CONFLICT_STARTED_PATH.unlink(missing_ok=True)
    CONFLICT_RELEASE_PATH.unlink(missing_ok=True)
    directories_before_write = distributed_artifact_directories(root)
    errors = []
    source = connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 100 AND 700").map_batches(
        WaitForCoordinatorConflict,
        schema={
            "id": vane.sqltype("INTEGER"),
            "payload": vane.sqltype("VARCHAR"),
        },
        batch_size=64,
        cpus=1.0,
        execution_backend="ray_actor",
        actor_number=WORKER_COUNT,
        target_max_batch_bytes=4096,
    )

    def execute_write() -> None:
        try:
            operation(source)
        except BaseException as error:
            errors.append(error)

    write_thread = threading.Thread(target=execute_write, name="vane-ducklake-conflict", daemon=True)
    write_thread.start()
    coordination_error = None
    conflict_connection = None
    files_after_conflict_commit = None
    try:
        deadline = time.monotonic() + 90
        while not CONFLICT_STARTED_PATH.exists():
            if not write_thread.is_alive():
                if errors:
                    raise AssertionError(f"write failed before conflict injection: {errors[0]!r}") from errors[0]
                raise AssertionError("write stopped before conflict injection")
            if time.monotonic() >= deadline:
                raise AssertionError("timed out waiting for distributed DuckLake worker output")
            time.sleep(0.05)
        conflict_connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        verify_extension_is_wheel_linked(conflict_connection)
        conflict_connection.execute(
            f"ATTACH {sql_string('ducklake:sqlite:' + str(root / 'metadata.sqlite'))} AS lake "
            f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 0, BUSY_TIMEOUT 30000)"
        )
        conflict_connection.execute(conflict_sql)
        files_after_conflict_commit = set((root / "data").rglob("*.parquet"))
    except BaseException as error:
        coordination_error = error
    finally:
        CONFLICT_RELEASE_PATH.touch()
        if conflict_connection is not None:
            conflict_connection.close()

    write_thread.join(timeout=120)
    CONFLICT_STARTED_PATH.unlink(missing_ok=True)
    CONFLICT_RELEASE_PATH.unlink(missing_ok=True)
    if write_thread.is_alive():
        raise AssertionError("distributed DuckLake conflict write did not stop")
    if coordination_error is not None:
        raise coordination_error
    require_write_count(1, f"{description} Ray dispatch count")
    require_equal(len(errors), 1, f"{description} failure count")
    require_true(
        "snapshot" in str(errors[0]).lower(),
        f"unexpected {description} error: {errors[0]}",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_after_conflict_commit,
        f"{description} artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_write,
        f"{description} artifact-root cleanup",
    )


def run_mutated_worker_write(
    vane: object,
    connection: object,
    plan: object,
    mutate: Callable[[dict[str, object], object], list[dict[str, object]] | None],
    selected_attempt_id: int = 0,
    artifact_data_root: Path | None = None,
) -> tuple[dict[str, object], int]:
    import pyarrow as pa
    from vane.runners.fte.backends.native import NativeFteWorkerManagerBackend
    from vane.runners.local.runner import _InProcessFragmentExecutor

    fragment_executor = _InProcessFragmentExecutor()
    mutated_file_count = 0
    loser_artifacts: set[Path] = set()

    def execute_with_mutated_result(request: object) -> object:
        nonlocal mutated_file_count
        result = fragment_executor(request)
        payloads = []
        for payload in result.partition_payloads:
            if payload.num_columns < 5:
                raise AssertionError("distributed write result is missing file-statistics columns")
            mutated_rows = []
            for row in payload.to_pylist():
                additional_rows = mutate(row, payload)
                mutated_rows.append(row)
                if additional_rows:
                    mutated_rows.extend(additional_rows)
                mutated_file_count += 1
            payloads.append(pa.Table.from_pylist(mutated_rows, schema=payload.schema))
        return vane.ray_cxx.NativeDistributedTaskResult(
            payloads,
            result.partition_metadatas,
            result.result_schema,
            result.stats,
            result.completion_status,
            result.flight_port,
            result.exchange_sink_instance,
            result.task_stats,
        )

    class SelectedAttemptBackend(NativeFteWorkerManagerBackend):
        @staticmethod
        def _request_from_task(task: object) -> dict[str, object]:
            request = NativeFteWorkerManagerBackend._request_from_task(task)
            if request.get("exchange_sink_instance") is not None:
                return request
            task_id = dict(request.get("task_id") or {})
            if not task_id:
                raise AssertionError("selected retry task is missing its identity")
            task_id["attempt_id"] = selected_attempt_id
            request["task_id"] = task_id
            for key in ("context", "task_context", "task_context_info"):
                value = request.get(key)
                if not isinstance(value, dict):
                    continue
                updated = dict(value)
                if "attempt_id" in updated or key == "context":
                    updated["attempt_id"] = selected_attempt_id
                request[key] = updated
            return request

    def execute_selected_attempt(request: object) -> object:
        result = fragment_executor(request)
        if artifact_data_root is None:
            return result
        task_id = dict(request).get("task_id")
        query_id = str(dict(task_id or {}).get("query_id") or "")
        for artifact_root in distributed_artifact_directories(artifact_data_root):
            for attempt_root in artifact_root.iterdir():
                if not attempt_root.is_dir():
                    continue
                try:
                    attempt_id = bytes.fromhex(attempt_root.name).decode()
                except (UnicodeDecodeError, ValueError):
                    continue
                if not query_id or not attempt_id.startswith(f"{query_id}."):
                    continue
                if not attempt_id.endswith(f".{selected_attempt_id}"):
                    continue
                loser_attempt_id = attempt_id.rsplit(".", 1)[0] + ".0"
                loser_root = artifact_root / loser_attempt_id.encode().hex()
                loser_root.mkdir(parents=True, exist_ok=True)
                loser_artifact = loser_root / "unselected-attempt.artifact"
                loser_artifact.write_bytes(b"unselected")
                loser_artifacts.add(loser_artifact)
        return result

    backend_type = SelectedAttemptBackend if selected_attempt_id != 0 else NativeFteWorkerManagerBackend
    backend = backend_type(
        execute_fn=execute_selected_attempt if selected_attempt_id != 0 else execute_with_mutated_result,
        num_workers=2 if selected_attempt_id != 0 else 1,
        max_running_tasks=1,
    )
    plan_runner = vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    try:
        outcome = plan_runner.run_copy_plan(plan, connection)
        if selected_attempt_id != 0:
            mutated_file_count = int(outcome.get("extension_artifact_count") or 0)
            if artifact_data_root is not None:
                require_true(bool(loser_artifacts), "selected retry did not create an unselected attempt artifact")
                require_true(
                    all(not artifact.exists() for artifact in loser_artifacts),
                    "selected retry retained an unselected attempt artifact",
                )
    finally:
        cleanup_errors = []
        for cleanup in (
            plan_runner.shutdown,
            backend.request_shutdown,
            fragment_executor.request_shutdown,
            lambda: backend.shutdown(timeout_s=30),
            lambda: fragment_executor.close(timeout_s=30),
        ):
            try:
                cleanup()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise RuntimeError(f"failed to stop mutated workers: {cleanup_errors[0]}") from cleanup_errors[0]
    return outcome, mutated_file_count


def run_repeated_mutation_input_write(
    vane: object,
    connection: object,
    plan: object,
) -> dict[str, object]:
    from vane.runners.fte.backends.native import NativeFteWorkerManagerBackend
    from vane.runners.local.runner import _InProcessFragmentExecutor

    fragment_executor = _InProcessFragmentExecutor()

    class RepeatedMutationInputBackend(NativeFteWorkerManagerBackend):
        def _request_from_task(self, task: object) -> dict[str, object]:
            request = super()._request_from_task(task)
            for splits in request.get("initial_splits", {}).values():
                if not splits or any(split.get("kind") != "exchange_source_task" for split in splits):
                    continue
                next_sequence = max(int(split["sequence_id"]) for split in splits) + 1
                repeated_splits = []
                for split in list(splits):
                    repeated_split = dict(split)
                    repeated_split["sequence_id"] = next_sequence
                    repeated_split["split_id"] = f"repeat-{next_sequence}"
                    next_sequence += 1
                    repeated_splits.append(repeated_split)
                splits.extend(repeated_splits)
            return request

    backend = RepeatedMutationInputBackend(execute_fn=fragment_executor, num_workers=2, max_running_tasks=1)
    plan_runner = vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    try:
        return plan_runner.run_copy_plan(plan, connection)
    finally:
        cleanup_errors = []
        for cleanup in (
            plan_runner.shutdown,
            backend.request_shutdown,
            fragment_executor.request_shutdown,
            lambda: backend.shutdown(timeout_s=30),
            lambda: fragment_executor.close(timeout_s=30),
        ):
            try:
                cleanup()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise RuntimeError(f"failed to stop repeated-input workers: {cleanup_errors[0]}") from cleanup_errors[0]


def require_forged_row_count_rejected(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
) -> None:
    files_before_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_failure = distributed_artifact_directories(root)
    source = connection.sql("SELECT NULL::INTEGER AS id, 'forged'::VARCHAR AS payload")
    plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: source.insert_into("lake.row_count_write_target"),
    )

    def forge_row_count(row: dict[str, object], payload: object) -> None:
        row_count_name = payload.column_names[1]
        column_statistics_name = payload.column_names[4]
        row[row_count_name] = 0
        forged_statistics = []
        forged_counts = set()
        for column_path, statistics in row[column_statistics_name] or []:
            column_statistics = []
            for statistic_name, statistic_value in statistics or []:
                if statistic_name in {"null_count", "num_values"}:
                    statistic_value = "0"
                    forged_counts.add(statistic_name)
                column_statistics.append((statistic_name, statistic_value))
            forged_statistics.append((column_path, column_statistics))
        require_equal(
            forged_counts,
            {"null_count", "num_values"},
            "forged worker row/null statistics",
        )
        row[column_statistics_name] = forged_statistics

    outcome, forged_file_count = run_mutated_worker_write(vane, connection, plan, forge_row_count)
    require_equal(
        outcome.get("extension_catalog_committed"),
        False,
        "forged row-count catalog outcome",
    )
    require_true(
        "row-count mismatch" in str(outcome.get("copy_output_outcome_error", "")).lower(),
        f"unexpected forged row-count outcome: {outcome}",
    )

    require_true(forged_file_count > 0, "forged distributed write produced no worker files")
    require_equal(
        connection.execute("SELECT count(*) FROM lake.row_count_write_target").fetchone(),
        (0,),
        "forged row-count table visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_failure,
        "forged row-count artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_failure,
        "forged row-count artifact-root cleanup",
    )


def require_forged_parquet_schema_rejected(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    files_before_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_failure = distributed_artifact_directories(root)
    source = connection.sql("SELECT 42::INTEGER AS id, 'schema'::VARCHAR AS payload")
    plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: source.insert_into("lake.schema_write_target"),
    )

    def remove_field_ids(row: dict[str, object], payload: object) -> None:
        path = Path(row[payload.column_names[0]])
        table = pq.read_table(path)
        schema = pa.schema([pa.field(field.name, field.type, field.nullable) for field in table.schema])
        pq.write_table(pa.Table.from_arrays(table.columns, schema=schema), path)
        row[payload.column_names[2]] = path.stat().st_size
        with path.open("rb") as parquet_file:
            parquet_file.seek(-8, os.SEEK_END)
            row[payload.column_names[3]] = int.from_bytes(parquet_file.read(4), "little")

    outcome, forged_file_count = run_mutated_worker_write(vane, connection, plan, remove_field_ids)
    require_equal(
        outcome.get("extension_catalog_committed"),
        False,
        "forged schema catalog outcome",
    )
    require_true(
        "field id" in str(outcome.get("copy_output_outcome_error", "")).lower(),
        f"unexpected forged schema outcome: {outcome}",
    )
    require_true(forged_file_count > 0, "forged schema write produced no worker files")
    require_equal(
        connection.execute("SELECT count(*) FROM lake.schema_write_target").fetchone(),
        (0,),
        "forged schema table visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_failure,
        "forged schema artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_failure,
        "forged schema artifact-root cleanup",
    )


def require_duplicate_artifact_rejected(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
) -> None:
    files_before_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_failure = distributed_artifact_directories(root)
    source = connection.sql("SELECT 43::INTEGER AS id, 'duplicate'::VARCHAR AS payload")
    plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: source.insert_into("lake.duplicate_path_write_target"),
    )

    def duplicate_artifact(row: dict[str, object], _payload: object) -> list[dict[str, object]]:
        return [dict(row)]

    try:
        outcome, _ = run_mutated_worker_write(vane, connection, plan, duplicate_artifact)
    except BaseException as error:
        require_true(
            "duplicate final path" in str(error).lower(),
            f"unexpected duplicate artifact error: {error}",
        )
    else:
        require_equal(
            outcome.get("extension_catalog_committed"),
            False,
            "duplicate artifact catalog outcome",
        )
        require_true(
            "duplicate data-file artifact" in str(outcome.get("copy_output_outcome_error", "")).lower(),
            f"unexpected duplicate artifact outcome: {outcome}",
        )
    require_equal(
        connection.execute("SELECT count(*) FROM lake.duplicate_path_write_target").fetchone(),
        (0,),
        "duplicate artifact table visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_failure,
        "duplicate artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_failure,
        "duplicate artifact-root cleanup",
    )


def require_forged_min_max_replaced(
    vane: object,
    connection: object,
    runner: object,
    require_write: Callable[[str, Callable[[], object]], None],
) -> None:
    source = connection.sql(
        "SELECT id::INTEGER AS id, ('footer-' || id::VARCHAR)::VARCHAR AS payload FROM range(100, 128) rows(id)"
    )
    plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: source.insert_into("lake.footer_stats_write_target"),
    )

    def forge_min_max(row: dict[str, object], payload: object) -> None:
        column_statistics_name = payload.column_names[4]
        forged_statistics = []
        forged_names = set()
        for column_path, statistics in row[column_statistics_name] or []:
            column_statistics = []
            for statistic_name, statistic_value in statistics or []:
                if statistic_name in {"min", "max"}:
                    statistic_value = "0"
                    forged_names.add(statistic_name)
                column_statistics.append((statistic_name, statistic_value))
            forged_statistics.append((column_path, column_statistics))
        require_equal(forged_names, {"min", "max"}, "forged worker min/max statistics")
        row[column_statistics_name] = forged_statistics

    outcome, forged_file_count = run_mutated_worker_write(vane, connection, plan, forge_min_max)
    require_equal(
        outcome.get("extension_catalog_committed"),
        True,
        "forged min/max catalog outcome",
    )
    require_true(
        not outcome.get("copy_output_outcome_error"),
        f"unexpected forged min/max outcome: {outcome}",
    )
    require_true(forged_file_count > 0, "forged min/max write produced no worker files")
    require_equal(
        connection.execute(
            "SELECT stats.min_value, stats.max_value "
            "FROM __ducklake_metadata_lake.ducklake_table_column_stats stats "
            "JOIN __ducklake_metadata_lake.ducklake_table tables USING (table_id) "
            "JOIN __ducklake_metadata_lake.ducklake_column columns USING (table_id, column_id) "
            "WHERE tables.table_name = 'footer_stats_write_target' AND tables.end_snapshot IS NULL "
            "AND columns.column_name = 'id' AND columns.end_snapshot IS NULL"
        ).fetchone(),
        ("100", "127"),
        "footer-derived min/max metadata",
    )
    require_equal(
        connection.execute("SELECT id FROM lake.footer_stats_write_target ORDER BY id DESC LIMIT 1").fetchone(),
        (127,),
        "footer-derived min/max Top-N readback",
    )

    inexact_source = connection.sql(
        "SELECT id::INTEGER AS id, (repeat('x', 300) || id::VARCHAR)::VARCHAR AS payload " "FROM range(4) rows(id)"
    )
    require_write(
        "distributed DuckLake inexact footer statistics",
        lambda: inexact_source.insert_into("lake.inexact_stats_write_target"),
    )
    require_equal(
        connection.execute(
            "SELECT stats.min_value, stats.max_value "
            "FROM __ducklake_metadata_lake.ducklake_table_column_stats stats "
            "JOIN __ducklake_metadata_lake.ducklake_table tables USING (table_id) "
            "JOIN __ducklake_metadata_lake.ducklake_column columns USING (table_id, column_id) "
            "WHERE tables.table_name = 'inexact_stats_write_target' AND tables.end_snapshot IS NULL "
            "AND columns.column_name = 'payload' AND columns.end_snapshot IS NULL"
        ).fetchone(),
        (None, None),
        "inexact footer min/max omission",
    )


def exercise_distributed_mutations(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
    require_write: Callable[[str, Callable[[], object]], None],
    require_write_count: Callable[[int, str], None],
) -> None:
    def delete_rows(table_name: str, condition: str) -> None:
        connection.table(table_name).delete(condition=vane.SQLExpression(condition))

    def update_rows(table_name: str, assignments: dict[str, str], condition: str) -> None:
        connection.table(table_name).update(
            {name: vane.SQLExpression(expression) for name, expression in assignments.items()},
            condition=vane.SQLExpression(condition),
        )

    initial_unpartitioned_count = connection.execute("SELECT count(*) FROM lake.mutation_unpartitioned").fetchone()[0]
    files_before_noop = {path for path in (root / "data").rglob("*") if path.is_file()}
    directories_before_noop = distributed_artifact_directories(root)
    require_write(
        "zero-match distributed DuckLake DELETE",
        lambda: delete_rows("lake.mutation_unpartitioned", "regexp_matches(payload, '^never-match$')"),
    )
    require_write(
        "zero-match distributed DuckLake UPDATE",
        lambda: update_rows(
            "lake.mutation_unpartitioned",
            {"payload": "'never'"},
            "regexp_matches(payload, '^never-match$')",
        ),
    )
    require_equal(
        connection.execute("SELECT count(*) FROM lake.mutation_unpartitioned").fetchone(),
        (initial_unpartitioned_count,),
        "zero-match mutation visibility",
    )
    require_equal(
        {path for path in (root / "data").rglob("*") if path.is_file()},
        files_before_noop,
        "zero-match mutation artifacts",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_noop,
        "zero-match mutation artifact roots",
    )

    require_equal(
        connection.execute(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file files "
            "JOIN __ducklake_metadata_lake.ducklake_table tables USING (table_id) "
            "WHERE tables.table_name = 'mutation_legacy_mapping' AND files.mapping_id IS NULL"
        ).fetchone(),
        (1,),
        "legacy mutation source mapping state",
    )
    require_write(
        "legacy-mapping distributed DuckLake UPDATE",
        lambda: update_rows(
            "lake.mutation_legacy_mapping",
            {"payload": "'updated-legacy'"},
            "id = 42",
        ),
    )
    require_write(
        "legacy-mapping distributed DuckLake DELETE",
        lambda: delete_rows("lake.mutation_legacy_mapping", "id = 43"),
    )
    require_equal(
        connection.execute("SELECT id, payload FROM lake.mutation_legacy_mapping ORDER BY id").fetchall(),
        [(42, "updated-legacy")],
        "legacy-mapping mutation readback",
    )

    duplicate_delete_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: delete_rows("lake.mutation_duplicate_delete", "id < 30"),
    )
    require_equal(
        sum(len(batches) for batches in duplicate_delete_plan.scan_split_batch_map().values()),
        2,
        "duplicate-match DELETE source split count",
    )
    duplicate_delete_outcome = run_repeated_mutation_input_write(
        vane,
        connection,
        duplicate_delete_plan,
    )
    require_equal(duplicate_delete_outcome.get("rows_copied"), 4, "duplicate-match DELETE affected rows")
    require_equal(duplicate_delete_outcome.get("extension_artifact_count"), 2, "duplicate-match DELETE artifacts")
    require_equal(
        connection.execute("SELECT id FROM lake.mutation_duplicate_delete ORDER BY id").fetchall(),
        [(30,)],
        "duplicate-match DELETE readback",
    )

    unpartitioned_delete_count = connection.execute(
        "SELECT count(*) FROM lake.mutation_unpartitioned WHERE id % 11 = 0"
    ).fetchone()[0]
    require_true(unpartitioned_delete_count > 0, "unpartitioned DELETE selected no rows")
    require_write(
        "unpartitioned distributed DuckLake DELETE",
        lambda: delete_rows("lake.mutation_unpartitioned", "id % 11 = 0"),
    )
    require_equal(
        connection.execute(
            "SELECT count(*), count(*) FILTER (WHERE id % 11 = 0) FROM lake.mutation_unpartitioned"
        ).fetchone(),
        (initial_unpartitioned_count - unpartitioned_delete_count, 0),
        "unpartitioned distributed DELETE readback",
    )

    unpartitioned_update_count = connection.execute(
        "SELECT count(*) FROM lake.mutation_unpartitioned WHERE id BETWEEN 100 AND 299"
    ).fetchone()[0]
    require_true(unpartitioned_update_count > 0, "unpartitioned UPDATE selected no rows")
    require_write(
        "unpartitioned distributed DuckLake UPDATE",
        lambda: update_rows(
            "lake.mutation_unpartitioned",
            {"payload": "'updated-' || id::VARCHAR"},
            "id BETWEEN 100 AND 299",
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*) FROM lake.mutation_unpartitioned "
            "WHERE id BETWEEN 100 AND 299 AND payload = 'updated-' || id::VARCHAR"
        ).fetchone(),
        (unpartitioned_update_count,),
        "unpartitioned distributed UPDATE readback",
    )

    initial_partitioned_count = connection.execute("SELECT count(*) FROM lake.mutation_partitioned").fetchone()[0]
    partitioned_update_count = connection.execute(
        "SELECT count(*) FROM lake.mutation_partitioned " "WHERE id BETWEEN 64 AND 319 AND id % 13 <> 0"
    ).fetchone()[0]
    require_true(partitioned_update_count > 0, "partitioned UPDATE selected no rows")
    require_write(
        "partitioned distributed DuckLake UPDATE",
        lambda: update_rows(
            "lake.mutation_partitioned",
            {"payload": "'partition-updated-' || id::VARCHAR"},
            "id BETWEEN 64 AND 319 AND id % 13 <> 0",
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*) FROM lake.mutation_partitioned "
            "WHERE id BETWEEN 64 AND 319 AND id % 13 <> 0 "
            "AND payload = 'partition-updated-' || id::VARCHAR"
        ).fetchone(),
        (partitioned_update_count,),
        "partitioned distributed UPDATE readback",
    )

    partitioned_delete_count = connection.execute(
        "SELECT count(*) FROM lake.mutation_partitioned " "WHERE id BETWEEN 128 AND 447 AND id % 17 = 0"
    ).fetchone()[0]
    require_true(partitioned_delete_count > 0, "partitioned DELETE selected no rows")
    require_write(
        "partitioned distributed DuckLake DELETE",
        lambda: delete_rows(
            "lake.mutation_partitioned",
            "id BETWEEN 128 AND 447 AND id % 17 = 0",
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*), count(*) FILTER (WHERE id BETWEEN 128 AND 447 AND id % 17 = 0) "
            "FROM lake.mutation_partitioned"
        ).fetchone(),
        (initial_partitioned_count - partitioned_delete_count, 0),
        "partitioned distributed DELETE readback",
    )
    require_equal(
        connection.execute(
            "SELECT count(DISTINCT regexp_extract(data_file, 'category=([^/]+)', 1)) "
            "FROM ducklake_list_files('lake', 'mutation_partitioned')"
        ).fetchone(),
        (4,),
        "partitioned mutation data paths",
    )

    state_before_failure = connection.execute(
        "SELECT count(*), sum(id), sum(length(payload)) FROM lake.mutation_unpartitioned"
    ).fetchone()
    files_before_failure = {path for path in (root / "data").rglob("*") if path.is_file()}
    directories_before_failure = distributed_artifact_directories(root)
    require_error(
        lambda: update_rows(
            "lake.mutation_unpartitioned",
            {"id": "CASE WHEN id < 256 THEN id ELSE payload::INTEGER END"},
            "id BETWEEN 200 AND 400",
        ),
        ("could not convert string", "no selected task results"),
        "distributed DuckLake UPDATE partial worker failure",
    )
    require_write_count(1, "failed UPDATE Ray dispatch count")
    require_equal(
        connection.execute(
            "SELECT count(*), sum(id), sum(length(payload)) FROM lake.mutation_unpartitioned"
        ).fetchone(),
        state_before_failure,
        "failed UPDATE table visibility",
    )
    require_equal(
        {path for path in (root / "data").rglob("*") if path.is_file()},
        files_before_failure,
        "failed UPDATE artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_failure,
        "failed UPDATE artifact-root cleanup",
    )

    files_before_retry = set(
        row[0]
        for row in connection.execute(
            "SELECT data_file FROM ducklake_list_files('lake', 'mutation_unpartitioned')"
        ).fetchall()
    )
    retry_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: update_rows(
            "lake.mutation_unpartitioned",
            {"payload": "'retry-selected'"},
            "id = 499",
        ),
    )
    retry_outcome, retry_file_count = run_mutated_worker_write(
        vane,
        connection,
        retry_plan,
        lambda _row, _payload: None,
        selected_attempt_id=1,
        artifact_data_root=root,
    )
    require_equal(retry_outcome.get("extension_catalog_committed"), True, "selected retry UPDATE commit")
    require_equal(retry_outcome.get("rows_copied"), 1, "selected retry UPDATE affected rows")
    require_true(retry_file_count > 0, "selected retry UPDATE produced no worker artifacts")
    require_equal(
        connection.execute("SELECT payload FROM lake.mutation_unpartitioned WHERE id = 499").fetchone(),
        ("retry-selected",),
        "selected retry UPDATE readback",
    )
    files_after_retry = set(
        row[0]
        for row in connection.execute(
            "SELECT data_file FROM ducklake_list_files('lake', 'mutation_unpartitioned')"
        ).fetchall()
    )
    retry_files = files_after_retry - files_before_retry
    require_equal(len(retry_files), 1, "selected retry UPDATE data-file count")
    retry_file_parts = Path(next(iter(retry_files))).parts
    artifact_root_index = next(
        index for index, part in enumerate(retry_file_parts) if part.startswith(DISTRIBUTED_ARTIFACT_PREFIX)
    )
    selected_attempt = bytes.fromhex(retry_file_parts[artifact_root_index + 1]).decode()
    require_true(selected_attempt.endswith(".1"), "selected retry UPDATE task-attempt identity")

    stale_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: delete_rows("lake.mutation_partitioned", "id = 333"),
    )
    configured_runner = os.environ.get("VANE_RUNNER")
    os.environ["VANE_RUNNER"] = "local-fast"
    try:
        connection.execute("INSERT INTO lake.stale_source VALUES (4, 'mutation-snapshot')")
    finally:
        if configured_runner is None:
            os.environ.pop("VANE_RUNNER", None)
        else:
            os.environ["VANE_RUNNER"] = configured_runner
    stale_directories = distributed_artifact_directories(root)
    physical_plan_runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
    try:
        require_error(
            lambda: physical_plan_runner.run_copy_plan(stale_plan, connection),
            "snapshot",
            "stale snapshot distributed DELETE",
        )
    finally:
        physical_plan_runner.shutdown()
    require_equal(
        connection.execute("SELECT count(*) FROM lake.mutation_partitioned WHERE id = 333").fetchone(),
        (1,),
        "stale distributed DELETE did not commit",
    )
    require_equal(
        distributed_artifact_directories(root),
        stale_directories,
        "stale distributed DELETE artifact cleanup",
    )

    native_unpartitioned = connection.execute(
        "SELECT id, payload FROM lake.mutation_unpartitioned ORDER BY id"
    ).fetchall()
    require_equal(
        connection.sql("SELECT id, payload FROM lake.mutation_unpartitioned ORDER BY id").fetchall(),
        native_unpartitioned,
        "Vane and native unpartitioned mutation readback",
    )
    native_partitioned = connection.execute(
        "SELECT id, category, payload FROM lake.mutation_partitioned ORDER BY id"
    ).fetchall()
    require_equal(
        connection.sql("SELECT id, category, payload FROM lake.mutation_partitioned ORDER BY id").fetchall(),
        native_partitioned,
        "Vane and native partitioned mutation readback",
    )


def exercise_distributed_merges(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
    expected_worker_nodes: set[str],
    require_write: Callable[[str, Callable[[], object]], None],
    require_write_count: Callable[[int, str], None],
) -> None:
    require_equal(len(expected_worker_nodes), WORKER_COUNT, "MERGE Ray cluster topology")
    update_clauses = [
        "WHEN MATCHED THEN UPDATE SET payload = source.payload",
        "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
    ]

    require_equal(
        connection.execute(
            "SELECT count(*) FROM ducklake_list_files('lake', 'merge_update_target') " "WHERE delete_file IS NOT NULL"
        ).fetchone(),
        (1,),
        "MERGE target existing delete state",
    )

    def update_source() -> object:
        return connection.sql("SELECT id, payload FROM lake.source WHERE id < 640")

    update_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: update_source().merge_into(
            "lake.merge_update_target",
            "target.id = source.id",
            update_clauses,
        ),
    )
    require_true(
        sum(len(batches) for batches in update_plan.scan_split_batch_map().values()) > 1,
        "distributed MERGE did not preserve multiple source-file splits",
    )
    require_write(
        "unpartitioned distributed DuckLake MERGE UPDATE and INSERT",
        lambda: update_source().merge_into(
            "lake.merge_update_target",
            "target.id = source.id",
            update_clauses,
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*), "
            "count(*) FILTER (WHERE id = 17 AND payload = 'merge-old-17'), "
            "count(*) FILTER (WHERE id <> 17 AND payload = 'value-' || id::VARCHAR) "
            "FROM lake.merge_update_target"
        ).fetchone(),
        (640, 1, 639),
        "distributed MERGE UPDATE and INSERT readback",
    )
    merge_attempt_ids = set()
    for (data_file,) in connection.execute(
        "SELECT data_file FROM ducklake_list_files('lake', 'merge_update_target')"
    ).fetchall():
        parts = Path(data_file).parts
        for index, part in enumerate(parts):
            if part.startswith(DISTRIBUTED_ARTIFACT_PREFIX):
                merge_attempt_ids.add(bytes.fromhex(parts[index + 1]).decode())
                break
    require_true(len(merge_attempt_ids) > 1, "distributed MERGE did not publish multiple task attempts")

    retry_source = connection.sql("SELECT 9001::INTEGER AS id, 'retry-selected'::VARCHAR AS payload")
    retry_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: retry_source.merge_into(
            "lake.merge_retry_target",
            "target.id = source.id",
            ["WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)"],
        ),
    )
    retry_outcome, retry_file_count = run_mutated_worker_write(
        vane,
        connection,
        retry_plan,
        lambda _row, _payload: None,
        selected_attempt_id=1,
        artifact_data_root=root,
    )
    require_equal(retry_outcome.get("extension_catalog_committed"), True, "selected retry MERGE commit")
    require_equal(retry_outcome.get("rows_copied"), 1, "selected retry MERGE affected rows")
    require_true(retry_file_count > 0, "selected retry MERGE produced no worker artifacts")
    require_equal(
        connection.execute("SELECT id, payload FROM lake.merge_retry_target").fetchone(),
        (9001, "retry-selected"),
        "selected retry MERGE readback",
    )
    retry_files = {
        row[0]
        for row in connection.execute(
            "SELECT data_file FROM ducklake_list_files('lake', 'merge_retry_target')"
        ).fetchall()
    }
    require_equal(len(retry_files), 1, "selected retry MERGE data-file count")
    retry_file_parts = Path(next(iter(retry_files))).parts
    artifact_root_index = next(
        index for index, part in enumerate(retry_file_parts) if part.startswith(DISTRIBUTED_ARTIFACT_PREFIX)
    )
    selected_attempt = bytes.fromhex(retry_file_parts[artifact_root_index + 1]).decode()
    require_true(selected_attempt.endswith(".1"), "selected retry MERGE task-attempt identity")

    delete_source = connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 896 AND 1279")
    require_write(
        "unpartitioned distributed DuckLake MERGE DELETE and INSERT",
        lambda: delete_source.merge_into(
            "lake.merge_delete_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN DELETE",
                "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
            ],
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*), min(id), max(id), "
            "count(*) FILTER (WHERE id BETWEEN 896 AND 1151), "
            "count(*) FILTER (WHERE id >= 1152 AND payload = 'value-' || id::VARCHAR) "
            "FROM lake.merge_delete_target"
        ).fetchone(),
        (256, 768, 1279, 0, 128),
        "distributed MERGE DELETE and INSERT readback",
    )

    by_source = connection.sql("SELECT id, payload FROM lake.source WHERE id IN (11, 12)")
    require_write(
        "not-matched-by-source distributed DuckLake MERGE",
        lambda: by_source.merge_into(
            "lake.merge_by_source_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN DO NOTHING",
                "WHEN NOT MATCHED BY SOURCE THEN DELETE",
                "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
            ],
        ),
    )
    require_equal(
        connection.execute("SELECT id, payload FROM lake.merge_by_source_target ORDER BY id").fetchall(),
        [(11, "keep"), (12, "value-12")],
        "not-matched-by-source distributed MERGE readback",
    )

    def partitioned_source() -> object:
        return connection.sql(
            "SELECT id, ('category-' || (id % 4)::VARCHAR)::VARCHAR AS category, payload "
            "FROM lake.source WHERE id BETWEEN 1408 AND 1791"
        )

    require_write(
        "partitioned distributed DuckLake MERGE",
        lambda: partitioned_source().merge_into(
            "lake.merge_partitioned_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN UPDATE SET category = source.category, payload = source.payload",
                "WHEN NOT MATCHED THEN INSERT (id, category, payload) "
                "VALUES (source.id, source.category, source.payload)",
            ],
        ),
    )
    require_equal(
        connection.execute(
            "SELECT count(*), "
            "count(*) FILTER (WHERE id >= 1408 AND payload = 'value-' || id::VARCHAR), "
            "count(DISTINCT category) FROM lake.merge_partitioned_target"
        ).fetchone(),
        (512, 384, 4),
        "partitioned distributed MERGE readback",
    )
    require_equal(
        connection.execute(
            "SELECT count(DISTINCT regexp_extract(data_file, 'category=([^/]+)', 1)) "
            "FROM ducklake_list_files('lake', 'merge_partitioned_target')"
        ).fetchone(),
        (4,),
        "partitioned distributed MERGE data paths",
    )

    files_before_noop = {path for path in (root / "data").rglob("*") if path.is_file()}
    directories_before_noop = distributed_artifact_directories(root)
    noop_source = connection.sql("SELECT id, payload FROM lake.source WHERE id IN (1, 900)")
    require_write(
        "zero-action distributed DuckLake MERGE",
        lambda: noop_source.merge_into(
            "lake.merge_noop_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN DO NOTHING",
                "WHEN NOT MATCHED AND source.id < 0 THEN " "INSERT (id, payload) VALUES (source.id, source.payload)",
            ],
        ),
    )
    require_equal(
        connection.execute("SELECT id, payload FROM lake.merge_noop_target ORDER BY id").fetchall(),
        [(1, "noop-old"), (2, "noop-keep")],
        "zero-action distributed MERGE visibility",
    )
    require_equal(
        {path for path in (root / "data").rglob("*") if path.is_file()},
        files_before_noop,
        "zero-action distributed MERGE artifacts",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_noop,
        "zero-action distributed MERGE artifact roots",
    )

    files_before_error = {path for path in (root / "data").rglob("*") if path.is_file()}
    directories_before_error = distributed_artifact_directories(root)
    error_source = connection.sql("SELECT id, payload FROM lake.source WHERE id = 1")
    require_error(
        lambda: error_source.merge_into(
            "lake.merge_noop_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN ERROR 'distributed merge error'",
                "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
            ],
        ),
        "merge error",
        "distributed DuckLake MERGE ERROR action",
    )
    require_write_count(1, "MERGE ERROR Ray dispatch count")
    require_equal(
        connection.execute("SELECT id, payload FROM lake.merge_noop_target ORDER BY id").fetchall(),
        [(1, "noop-old"), (2, "noop-keep")],
        "MERGE ERROR action visibility",
    )
    require_equal(
        {path for path in (root / "data").rglob("*") if path.is_file()},
        files_before_error,
        "MERGE ERROR artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_error,
        "MERGE ERROR artifact-root cleanup",
    )

    state_before_failure = connection.execute(
        "SELECT count(*), sum(id), sum(length(payload)) FROM lake.merge_failure_target"
    ).fetchone()
    files_before_failure = {path for path in (root / "data").rglob("*") if path.is_file()}
    directories_before_failure = distributed_artifact_directories(root)
    failing_source = connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 256 AND 1279")
    require_error(
        lambda: failing_source.merge_into(
            "lake.merge_failure_target",
            "target.id = source.id",
            [
                "WHEN MATCHED THEN UPDATE SET payload = CASE WHEN source.id < 768 "
                "THEN source.payload ELSE source.payload::INTEGER::VARCHAR END",
                "WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (source.id, source.payload)",
            ],
        ),
        ("could not convert string", "no selected task results"),
        "distributed DuckLake MERGE partial worker failure",
    )
    require_write_count(1, "failed MERGE Ray dispatch count")
    require_equal(
        connection.execute("SELECT count(*), sum(id), sum(length(payload)) FROM lake.merge_failure_target").fetchone(),
        state_before_failure,
        "failed MERGE atomic visibility",
    )
    require_equal(
        {path for path in (root / "data").rglob("*") if path.is_file()},
        files_before_failure,
        "failed MERGE artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        directories_before_failure,
        "failed MERGE artifact-root cleanup",
    )

    require_concurrent_write_conflict(
        vane,
        connection,
        runner,
        root,
        require_write_count,
        lambda source: source.merge_into(
            "lake.merge_conflict_target",
            "target.id = source.id",
            update_clauses,
        ),
        "INSERT INTO lake.merge_conflict_target VALUES (-2, 'concurrent')",
        "concurrent MERGE conflict",
    )
    require_equal(
        connection.execute(
            "SELECT count(*), count(*) FILTER (WHERE id = -2 AND payload = 'concurrent'), "
            "count(*) FILTER (WHERE payload LIKE 'value-%') FROM lake.merge_conflict_target"
        ).fetchone(),
        (513, 1, 0),
        "concurrent MERGE conflict visibility",
    )

    for table_name, columns in (
        ("merge_update_target", "id, payload"),
        ("merge_delete_target", "id, payload"),
        ("merge_by_source_target", "id, payload"),
        ("merge_partitioned_target", "id, category, payload"),
    ):
        native_rows = connection.execute(f"SELECT {columns} FROM lake.{table_name} ORDER BY id").fetchall()
        require_equal(
            connection.sql(f"SELECT {columns} FROM lake.{table_name} ORDER BY id").fetchall(),
            native_rows,
            f"Vane and native {table_name} MERGE readback",
        )


def exercise_distributed_writes(
    vane: object,
    connection: object,
    runner: object,
    root: Path,
    require_write: Callable[[str, Callable[[], object]], None],
    require_write_count: Callable[[int, str], None],
) -> None:
    expected_rows = ROW_COUNT - 1
    first_half = connection.sql("SELECT id, payload FROM lake.source WHERE id < 1024")
    require_write(
        "distributed DuckLake INSERT",
        lambda: first_half.insert_into("lake.write_target"),
    )
    second_half = connection.sql("SELECT id, payload FROM lake.source WHERE id >= 1024")
    require_write(
        "distributed DuckLake append",
        lambda: second_half.insert_into("lake.write_target"),
    )
    empty_source = connection.sql("SELECT id, payload FROM lake.source WHERE false")
    require_write(
        "empty distributed DuckLake INSERT",
        lambda: empty_source.insert_into("lake.write_target"),
    )
    require_equal(
        connection.execute("SELECT count(*)::BIGINT, sum(id)::BIGINT FROM lake.write_target").fetchone(),
        (expected_rows, ROW_COUNT * (ROW_COUNT - 1) // 2 - 17),
        "native readback after distributed INSERT",
    )
    file_count = connection.execute("SELECT count(*) FROM ducklake_list_files('lake', 'write_target')").fetchone()[0]
    require_true(
        file_count >= WORKER_COUNT,
        "distributed INSERT did not publish multiple worker artifacts",
    )
    require_equal(
        len(distributed_artifact_roots(connection, "write_target")),
        2,
        "distributed INSERT write roots",
    )

    nested_source = connection.sql(
        "SELECT {'value': id}::STRUCT(value INTEGER) AS payload, [id, id + 1]::INTEGER[] AS items "
        "FROM lake.source WHERE id BETWEEN 40 AND 42"
    )
    require_write(
        "nested distributed DuckLake INSERT",
        lambda: nested_source.insert_into("lake.nested_write_target"),
    )
    require_equal(
        connection.execute(
            "SELECT payload.value, items[1], items[2] FROM lake.nested_write_target ORDER BY payload.value"
        ).fetchall(),
        [(value, value, value + 1) for value in range(40, 43)],
        "nested distributed INSERT readback",
    )

    partitioned_source = connection.sql(
        "SELECT id, ('category-' || (id % 4)::VARCHAR)::VARCHAR AS category, payload " "FROM lake.source WHERE id < 512"
    )
    require_write(
        "partitioned distributed DuckLake INSERT",
        lambda: partitioned_source.insert_into("lake.partitioned_write_target"),
    )
    partitions = connection.execute(
        "SELECT DISTINCT regexp_extract(data_file, 'category=([^/]+)', 1) "
        "FROM ducklake_list_files('lake', 'partitioned_write_target') ORDER BY 1"
    ).fetchall()
    require_equal(
        partitions,
        [(f"category-{index}",) for index in range(4)],
        "distributed INSERT partition paths",
    )
    require_equal(
        len(distributed_artifact_roots(connection, "partitioned_write_target")),
        1,
        "partitioned distributed INSERT write roots",
    )

    ctas_source = connection.sql("SELECT id, payload FROM lake.source WHERE id < 768")
    require_write("distributed DuckLake CTAS", lambda: ctas_source.create("lake.ctas_target"))
    require_equal(
        connection.execute("SELECT count(*)::BIGINT, sum(id)::BIGINT FROM lake.ctas_target").fetchone(),
        (767, 768 * 767 // 2 - 17),
        "native readback after distributed CTAS",
    )
    require_equal(
        len(distributed_artifact_roots(connection, "ctas_target")),
        1,
        "distributed CTAS write roots",
    )

    empty_ctas_source = connection.sql("SELECT id, payload FROM lake.empty_source")
    require_write(
        "empty distributed DuckLake CTAS",
        lambda: empty_ctas_source.create("lake.empty_ctas_target"),
    )
    require_equal(
        connection.execute("SELECT count(*)::BIGINT FROM lake.empty_ctas_target").fetchone(),
        (0,),
        "empty distributed CTAS table visibility",
    )
    require_equal(
        connection.execute("SELECT count(*) FROM ducklake_list_files('lake', 'empty_ctas_target')").fetchone(),
        (0,),
        "empty distributed CTAS artifacts",
    )

    partitioned_ctas_source = connection.sql(
        "SELECT id, ('group-' || (id % 3)::VARCHAR)::VARCHAR AS category, payload " "FROM lake.source WHERE id < 384"
    )
    require_write(
        "partitioned distributed DuckLake CTAS",
        lambda: partitioned_ctas_source.create("lake.partitioned_ctas_target", partition_by=["category"]),
    )
    require_equal(
        connection.execute("SELECT count(*)::BIGINT FROM lake.partitioned_ctas_target").fetchone(),
        (383,),
        "native readback after partitioned distributed CTAS",
    )
    require_equal(
        connection.execute(
            "SELECT count(DISTINCT regexp_extract(data_file, 'category=([^/]+)', 1)) "
            "FROM ducklake_list_files('lake', 'partitioned_ctas_target')"
        ).fetchone(),
        (3,),
        "distributed CTAS partition paths",
    )

    files_before_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_failure = distributed_artifact_directories(root)
    failing_source = connection.sql(
        "SELECT CASE WHEN id < 512 THEN id ELSE payload::INTEGER END AS id " "FROM lake.source"
    )
    require_error(
        lambda: failing_source.create("lake.failed_ctas_target"),
        "Could not convert string",
        "distributed DuckLake CTAS worker failure",
    )
    require_write_count(1, "failed CTAS Ray dispatch count")
    require_equal(
        connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = 'lake' AND table_name = 'failed_ctas_target'"
        ).fetchone(),
        (0,),
        "failed distributed CTAS catalog visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_failure,
        "failed CTAS artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_failure,
        "failed CTAS artifact-root cleanup",
    )

    files_before_constraint_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_constraint_failure = distributed_artifact_directories(root)
    null_source = connection.sql(
        "SELECT CASE WHEN id = 31 THEN NULL ELSE id END::INTEGER AS id, payload FROM lake.source WHERE id < 128"
    )
    require_error(
        lambda: null_source.insert_into("lake.not_null_write_target"),
        "not null",
        "distributed DuckLake INSERT coordinator constraint failure",
    )
    require_write_count(1, "constraint failure Ray dispatch count")
    require_equal(
        connection.execute("SELECT count(*) FROM lake.not_null_write_target").fetchone(),
        (0,),
        "constraint failure table visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_constraint_failure,
        "constraint failure artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_constraint_failure,
        "constraint failure artifact-root cleanup",
    )

    require_forged_row_count_rejected(vane, connection, runner, root)
    require_forged_parquet_schema_rejected(vane, connection, runner, root)
    require_duplicate_artifact_rejected(vane, connection, runner, root)
    require_forged_min_max_replaced(vane, connection, runner, require_write)

    files_before_missing_stats = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_missing_stats = distributed_artifact_directories(root)
    nested_source = connection.sql(
        "SELECT {'value': id}::STRUCT(value INTEGER) AS payload FROM lake.source WHERE id < 64"
    )
    require_error(
        lambda: nested_source.insert_into("lake.not_null_nested_target"),
        "missing column statistics for not null column",
        "distributed DuckLake missing NOT NULL statistics",
    )
    require_write_count(1, "missing NOT NULL statistics Ray dispatch count")
    require_equal(
        connection.execute("SELECT count(*) FROM lake.not_null_nested_target").fetchone(),
        (0,),
        "missing NOT NULL statistics table visibility",
    )
    require_equal(
        set((root / "data").rglob("*.parquet")),
        files_before_missing_stats,
        "missing NOT NULL statistics artifact cleanup",
    )
    require_equal(
        distributed_artifact_directories(root),
        artifact_directories_before_missing_stats,
        "missing NOT NULL statistics artifact-root cleanup",
    )

    files_before_commit_failure = set((root / "data").rglob("*.parquet"))
    artifact_directories_before_commit_failure = distributed_artifact_directories(root)
    connection.execute("CALL lake.set_option('require_commit_message', true)")
    commit_failure_source = connection.sql("SELECT id, payload FROM lake.source WHERE id < 64")
    try:
        require_error(
            lambda: commit_failure_source.insert_into("lake.rollback_write_target"),
            "commit information",
            "distributed DuckLake transaction commit failure",
        )
        require_write_count(1, "transaction commit failure Ray dispatch count")
        require_equal(
            connection.execute("SELECT count(*) FROM lake.rollback_write_target").fetchone(),
            (0,),
            "transaction commit failure table visibility",
        )
        require_equal(
            set((root / "data").rglob("*.parquet")),
            files_before_commit_failure,
            "transaction commit failure artifact cleanup",
        )
        require_equal(
            distributed_artifact_directories(root),
            artifact_directories_before_commit_failure,
            "transaction commit failure artifact-root cleanup",
        )
    finally:
        connection.execute("CALL lake.set_option('require_commit_message', false)")

    stale_source = connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 800 AND 900")
    stale_plan = capture_write_plan(
        vane,
        runner,
        lambda: stale_source.insert_into("lake.write_target"),
    )
    connection.execute("ALTER TABLE lake.write_target ADD COLUMN added INTEGER DEFAULT 7")
    query_driver = runner.query_driver_client
    if query_driver is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    require_error(
        lambda: query_driver.run_copy_plan(stale_plan),
        "definition",
        "stale schema distributed INSERT",
    )
    require_equal(
        connection.execute("SELECT count(*)::BIGINT FROM lake.write_target").fetchone(),
        (expected_rows,),
        "stale distributed INSERT did not commit",
    )

    stale_snapshot_source = connection.sql("SELECT id, payload FROM lake.source WHERE id BETWEEN 901 AND 950")
    stale_snapshot_plan = capture_physical_write_plan(
        vane,
        connection,
        runner,
        lambda: stale_snapshot_source.insert_into("lake.concurrent_write_target"),
    )
    connection.execute("INSERT INTO lake.stale_source VALUES (3, 'snapshot')")
    physical_plan_runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
    try:
        require_error(
            lambda: physical_plan_runner.run_copy_plan(stale_snapshot_plan, connection),
            "snapshot",
            "stale snapshot distributed INSERT",
        )
    finally:
        physical_plan_runner.shutdown()
    require_equal(
        connection.execute("SELECT count(*)::BIGINT FROM lake.concurrent_write_target").fetchone(),
        (1,),
        "stale snapshot distributed INSERT did not commit",
    )

    require_concurrent_write_conflict(
        vane,
        connection,
        runner,
        root,
        require_write_count,
        lambda source: source.insert_into("lake.concurrent_write_target"),
        "INSERT INTO lake.concurrent_write_target VALUES (-2, 'concurrent')",
        "concurrent INSERT conflict",
    )


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
    original_run_write = None
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

            write_dispatch_count = 0
            write_dispatch_checkpoint = 0
            original_run_write = runner.run_write

            def record_distributed_write(*args: object, **kwargs: object) -> object:
                nonlocal write_dispatch_count
                write_dispatch_count += 1
                return original_run_write(*args, **kwargs)

            def require_write_count(expected: int, description: str) -> None:
                nonlocal write_dispatch_checkpoint
                require_equal(
                    write_dispatch_count - write_dispatch_checkpoint,
                    expected,
                    description,
                )
                write_dispatch_checkpoint = write_dispatch_count

            def require_distributed_write(description: str, operation: Callable[[], object]) -> None:
                operation()
                require_write_count(1, f"{description} Ray dispatch count")

            runner.run_write = record_distributed_write
            exercise_distributed_writes(
                vane,
                connection,
                runner,
                root,
                require_distributed_write,
                require_write_count,
            )
            exercise_distributed_mutations(
                vane,
                connection,
                runner,
                root,
                require_distributed_write,
                require_write_count,
            )
            exercise_distributed_merges(
                vane,
                connection,
                runner,
                root,
                expected_nodes,
                require_distributed_write,
                require_write_count,
            )

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
                connection.sql("SELECT count(*)::BIGINT FROM lake.write_target").fetchall(),
                [(ROW_COUNT - 1,)],
                "Vane readback after distributed write",
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
            require_true(write_dispatch_count >= 25, "DuckLake writes did not use the Ray runner")
    finally:
        if runner is not None and original_run_iter_tables is not None:
            runner.run_iter_tables = original_run_iter_tables
        if runner is not None and original_run_write is not None:
            runner.run_write = original_run_write
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
