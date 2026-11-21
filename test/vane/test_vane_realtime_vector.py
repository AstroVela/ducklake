#!/usr/bin/env python3
"""Qualify frozen inline vectors through two real Vane Ray execution nodes."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path


def load_helpers():
    path = Path(__file__).with_name('test_vane_wheel_ray_ducklake.py')
    spec = importlib.util.spec_from_file_location('ducklake_ray_helpers', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helpers = load_helpers()
require_equal = helpers.require_equal
sql_string = helpers.sql_string
RANK = (
    "SELECT id,payload,list_distance(embedding,[0,0,0]::FLOAT[]) AS distance "
    "FROM lake.items WHERE category='A' ORDER BY distance,id LIMIT 5"
)


def cleanup(report, actions):
    # Preserve the query failure if closing an unhealthy distributed session
    # also fails, and always attempt cleanup of every resource owned by the proof.
    active_error = sys.exc_info()[0] is not None
    errors = []
    for label, action in actions:
        try:
            action()
        except BaseException as error:
            errors.append(f'{label}: {type(error).__name__}: {error}')
    if errors:
        report['passed'] = False
        report.setdefault('cleanup_errors', []).extend(errors)
        if not active_error:
            raise RuntimeError(errors[0])


class RecordNode:
    def __init__(self, gate: str | None = None):
        self.gate = gate

    def __call__(self, table):
        import pyarrow as pa
        import ray

        node_id = str(ray.get_runtime_context().get_node_id())
        if self.gate:
            root = Path(self.gate)
            (root / f'ready-{node_id}').touch()
            deadline = time.monotonic() + 120
            while not (root / 'release').exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError('timed out waiting for the snapshot writer')
                time.sleep(0.05)
        else:
            time.sleep(0.1)
        return table.append_column('worker_node_id', pa.array([node_id] * table.num_rows))


def annotated_rows(vane, relation, gate=None):
    class Recorder(RecordNode):
        def __init__(self):
            super().__init__(gate)

    return relation.map_batches(
        Recorder,
        schema={
            'id': vane.sqltype('BIGINT'),
            'embedding': vane.sqltype('FLOAT[]'),
            'category': vane.sqltype('VARCHAR'),
            'payload': vane.sqltype('VARCHAR'),
            'worker_node_id': vane.sqltype('VARCHAR'),
        },
        batch_size=64,
        cpus=1.0,
        execution_backend='ray_actor',
        actor_number=2,
        target_max_batch_bytes=4096,
    ).fetchall()


def native_connection(vane):
    # The fixture chooses native writes/oracles explicitly. Query connections
    # keep the default Ray policy throughout the proof.
    connection = vane._native._connect_with_runner('local-fast')
    connection.execute('SET autoinstall_known_extensions=false')
    connection.execute('SET autoload_known_extensions=false')
    helpers.load_test_extensions(connection)
    return connection


def frozen_replay(vane, connection, native, mutate):
    query = 'SELECT id,embedding,category,payload FROM lake.items WHERE id < 12'
    expected = native.execute(query + ' ORDER BY id').fetchall()
    plan = helpers.make_physical_plan(vane, connection, query)
    node_id, batches = helpers.single_scan_source(plan)
    fragment = helpers.capture_worker_fragment(vane, connection, plan, node_id)
    batch = vane.ray_cxx.merge_scan_split_batches(batches)
    mutate()
    runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
    try:
        for _ in range(2):
            worker = native_connection(vane)
            try:
                result = runner.execute_native(
                    worker.cursor(), fragment.clone(worker), scan_split_batch={node_id: batch}
                )
                require_equal(result.completion_status, 'ok', 'frozen worker execution')
                rows = []
                for table in result.partition_payloads:
                    rows.extend(zip(*(column.to_pylist() for column in table.columns)))
                require_equal(sorted(rows), expected, 'snapshot rows on an isolated replacement worker')
                require_equal(
                    worker.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name='lake'").fetchone(),
                    (0,),
                    'worker never attaches the metadata catalog',
                )
            finally:
                worker.close()
    finally:
        runner.shutdown()


def exercise(vane, ray, cluster, root, dsn, report, kill_node):
    connection = vane.connect(
        ':memory:', config={'autoinstall_known_extensions': 'false', 'autoload_known_extensions': 'false'}
    )
    helpers.load_test_extensions(connection)
    native = native_connection(vane)
    schema = 'vector_proof_' + uuid.uuid4().hex
    if dsn:
        connection.execute('LOAD postgres_scanner')
        native.execute('LOAD postgres_scanner')
        location = 'ducklake:postgres:' + dsn
        schema_option = f', METADATA_SCHEMA {sql_string(schema)}'
    else:
        location = 'ducklake:sqlite:' + str(root / 'metadata.sqlite')
        schema_option = ''
        require_equal(
            helpers.metadata_sql(root, ('PRAGMA journal_mode=WAL', ()))[0],
            [('wal',)],
            'SQLite fixture permits concurrent snapshot readers and a writer',
        )
    attach = (
        f'ATTACH {sql_string(location)} AS lake '
        f"(DATA_PATH {sql_string(root / 'data')}, DATA_INLINING_ROW_LIMIT 10000{schema_option})"
    )
    native.execute(attach)
    try:
        native.execute(
            'CREATE TABLE lake.items(id BIGINT NOT NULL,embedding FLOAT[] NOT NULL,category VARCHAR,payload VARCHAR)'
        )
        native.execute(
            "INSERT INTO lake.items SELECT i,[i::FLOAT,0,0],CASE WHEN i%2=0 THEN 'A' ELSE 'B' END,'λ-'||i "
            'FROM range(4096) t(i)'
        )
        connection.execute(attach)
        require_equal(list((root / 'data').rglob('*.parquet')), [], 'inline writes generate no Parquet files')
        expected_nodes = helpers.execution_node_ids(ray)
        result = annotated_rows(vane, connection.sql('SELECT id,embedding,category,payload FROM lake.items'))
        require_equal(len(result), 4096, 'inline rows consumed on Ray')
        require_equal({row[-1] for row in result}, expected_nodes, 'both Ray execution nodes consume inline data')
        require_equal(len({row[0] for row in result}), 4096, 'inline splits do not duplicate rows')
        virtual_query = 'SELECT id,rowid,snapshot_id,file_index,filename,file_row_number FROM lake.items ORDER BY id'
        require_equal(
            connection.sql(virtual_query).fetchall(),
            native.execute(virtual_query).fetchall(),
            'virtual row and source identities agree across frozen batches',
        )

        def check_rank(label, committed_at=None):
            expected = native.execute(RANK).fetchall()
            started = time.monotonic()
            require_equal(connection.sql(RANK).fetchall(), expected, label)
            report.setdefault('query_seconds', {})[label] = time.monotonic() - started
            if committed_at is not None:
                report.setdefault('commit_to_read_complete_seconds', {})[label] = time.monotonic() - committed_at

        check_rank('inline exact top-k')
        native.execute('ALTER TABLE lake.items ADD COLUMN revision INTEGER DEFAULT 7')
        require_equal(
            connection.sql('SELECT id,revision FROM lake.items WHERE id%3=0 ORDER BY id').fetchall(),
            [(i, 7) for i in range(4096) if i % 3 == 0],
            'schema defaults apply to previously committed inline batches',
        )
        snapshot = native.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        historical = RANK.replace('FROM lake.items WHERE', f'FROM lake.items AT (VERSION => {snapshot}) WHERE')
        old_rank = native.execute(historical).fetchall()

        def mutate_and_flush():
            native.execute('BEGIN')
            native.execute("UPDATE lake.items SET embedding=[99,0,0],category='B',payload='updated' WHERE id=0")
            native.execute('DELETE FROM lake.items WHERE id=2')
            native.execute("INSERT INTO lake.items(id,embedding,category,payload) VALUES (5001,[0.5,0,0],'A','new')")
            native.execute('COMMIT')
            native.execute("CALL ducklake_flush_inlined_data('lake')")

        frozen_replay(vane, connection, native, mutate_and_flush)
        report['isolated_worker_replay_passed'] = True
        check_rank('materialized update and delete')
        require_equal(native.execute(historical).fetchall(), old_rank, 'native historical vectors survive flush')
        require_equal(connection.sql(historical).fetchall(), old_rank, 'historical vectors after flush')
        report['historical_snapshot_passed'] = True
        native.execute("INSERT INTO lake.items(id,embedding,category,payload) VALUES (6000,[0.25,0,0],'A','delta')")
        native.execute('DELETE FROM lake.items WHERE id=4')
        check_rank('mixed files and inline delta', time.monotonic())
        require_equal(
            connection.sql('SELECT id,payload FROM lake.items WHERE id IN (0,2,4) ORDER BY id').fetchall(),
            [(0, 'updated')],
            'stale vectors and deleted file rows cannot return',
        )

        # Hold a real distributed query after its snapshot has been frozen.
        gate = root / 'gate'
        gate.mkdir()
        expected = native.execute('SELECT id,embedding,category,payload FROM lake.items ORDER BY id').fetchall()
        output, errors = [], []

        def read_snapshot():
            try:
                output.extend(
                    annotated_rows(
                        vane, connection.sql('SELECT id,embedding,category,payload FROM lake.items'), str(gate)
                    )
                )
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=read_snapshot, daemon=True)
        report['phase'] = 'concurrent snapshot and node recovery' if kill_node else 'concurrent snapshot'
        thread.start()
        try:
            deadline = time.monotonic() + 120
            while not list(gate.glob('ready-*')):
                if errors:
                    raise errors[0]
                if not thread.is_alive() or time.monotonic() >= deadline:
                    raise AssertionError('Ray query never reached the snapshot boundary')
                time.sleep(0.05)
            writer = native_connection(vane)
            try:
                if dsn:
                    writer.execute('LOAD postgres_scanner')
                writer.execute(attach)
                writer.execute(
                    "INSERT INTO lake.items(id,embedding,category,payload) VALUES (7000,[0,0,0],'A','after-snapshot')"
                )
                writer.execute("CALL ducklake_flush_inlined_data('lake')")
            finally:
                writer.close()
            if kill_node:
                victim = next(gate.glob('ready-*')).name.removeprefix('ready-')
                victim_address = next(node['NodeManagerAddress'] for node in ray.nodes() if node['NodeID'] == victim)
                victims = [node for node in cluster.list_all_nodes() if node.node_ip_address == victim_address]
                # Local cluster nodes share an IP; match the unique node-manager port instead.
                victim_port = next(node['NodeManagerPort'] for node in ray.nodes() if node['NodeID'] == victim)
                victim_node = next(node for node in victims if node.node_manager_port == victim_port)
                cluster.remove_node(victim_node, allow_graceful=False)
                report['killed_execution_node'] = victim
        finally:
            (gate / 'release').touch()
            thread.join(timeout=180)
        if thread.is_alive():
            raise AssertionError('distributed snapshot query did not finish')
        if errors:
            raise errors[0]
        require_equal(sorted(row[:-1] for row in output), expected, 'snapshot across concurrent commit and flush')
        require_equal(len({row[0] for row in output}), len(expected), 'retry has no duplicate rows')
        check_rank('new query sees the later commit')
        report['snapshot_replay_passed'] = True
        report['execution_nodes'] = sorted(expected_nodes)
    finally:
        actions = [('query connection', connection.close), ('native connection', native.close)]
        if dsn:

            def drop_schema():
                catalog = native_connection(vane)
                try:
                    catalog.execute('LOAD postgres_scanner')
                    catalog.execute(f'ATTACH {sql_string(dsn)} AS pg (TYPE postgres)')
                    catalog.execute(f'DROP SCHEMA pg.{schema} CASCADE')
                finally:
                    catalog.close()

            actions.append(('owned PostgreSQL schema', drop_schema))
        cleanup(report, actions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn', help='dedicated PostgreSQL test database; otherwise use a temporary SQLite catalog')
    parser.add_argument('--report', type=Path)
    parser.add_argument(
        '--kill-node', action='store_true', help='also kill an execution node while its query is paused'
    )
    args = parser.parse_args()
    if os.environ.get('VANE_RUNNER'):
        raise RuntimeError('leave VANE_RUNNER unset to qualify the default Ray runner')
    os.environ['VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION'] = '1'
    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError('the realtime vector proof must own its Ray cluster')
    report = {
        'passed': False,
        'catalog': 'postgres' if args.dsn else 'sqlite',
        'kill_node': args.kill_node,
        'vane_version': vane.__version__,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
    cluster = helpers.create_two_worker_cluster(ray)
    runner = None
    original_run = None
    dispatches = []

    def distributed_read(plan):
        dispatches.append(None)
        return original_run(plan)

    try:
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, 'name', None), 'ray', 'default Vane runner')
        original_run = runner.run_iter_tables
        runner.run_iter_tables = distributed_read
        with tempfile.TemporaryDirectory(prefix='vane-realtime-vector-') as directory:
            exercise(vane, ray, cluster, Path(directory), args.dsn, report, args.kill_node)
        if len(dispatches) < 6:
            raise AssertionError('realtime queries did not dispatch through the Ray runner')
        report['distributed_read_dispatches'] = len(dispatches)
        report['passed'] = True
        report['phase'] = 'complete'
    except BaseException as error:
        report['error'] = str(error)
        raise
    finally:
        if runner is not None and original_run is not None:
            runner.run_iter_tables = original_run
        try:
            cleanup(
                report,
                [
                    ('Vane runner', vane.teardown_runner),
                    ('Ray client', ray.shutdown),
                    ('owned cluster', cluster.shutdown),
                ],
            )
        finally:
            if args.report:
                args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
