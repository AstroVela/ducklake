
WITH events AS (
    SELECT end_snapshot AS boundary, 0 AS event_kind, data_file_id AS file_id,
           CAST(NULL AS BIGINT) AS snapshot_id
    FROM ducklake_data_file
    WHERE end_snapshot IS NOT NULL
    UNION ALL
    SELECT snapshot_id, 1, CAST(NULL AS BIGINT), snapshot_id
    FROM ducklake_snapshot
    WHERE snapshot_id < (SELECT MAX(end_snapshot) FROM ducklake_data_file)
), file_predecessors AS (
    SELECT file_id, MAX(snapshot_id) OVER (
        ORDER BY boundary, event_kind ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS snapshot_before_end
    FROM events
)
SELECT f.data_file_id, f.table_id, f.path, f.path_is_relative
FROM ducklake_data_file f
LEFT JOIN file_predecessors p ON p.file_id = f.data_file_id
WHERE {forced} (f.end_snapshot IS NOT NULL AND NOT COALESCE(p.snapshot_before_end >= f.begin_snapshot, false));