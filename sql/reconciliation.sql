-- ---------------------------------------------------------------------------
-- Reconciliation queries (SRS FR-012, AC-12).
--
-- Run with:  rake query:file[sql/reconciliation.sql]
--
-- Reconciliation compares the materialised projection against an independently
-- derived expectation from the append-only history. Everything here is derived
-- from raw.order_cdc_events, so the history is the audit trail and the
-- projection is the thing under test.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 1. Live vs deleted split of the projection. Business queries always filter
--    is_deleted = false (SRS 6.2); this makes an accidental filter regression
--    immediately visible.
-- ---------------------------------------------------------------------------
SELECT is_deleted, count(*) AS rows_in_projection
  FROM silver.orders_current
 GROUP BY is_deleted
 ORDER BY is_deleted;

-- ---------------------------------------------------------------------------
-- 2. Expected key set, derived by replaying the history: the set of keys ever
--    observed. Any key present in the projection but absent here would be a
--    phantom row; any key here but absent from the projection a lost row.
-- ---------------------------------------------------------------------------
SELECT DISTINCT primary_key AS order_key
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3. Last event per key by source position, which is what the projection is
--    expected to reflect. source_timestamp is deliberately NOT used for
--    ordering (SRS 5.2): arrival or wall-clock order is not a valid precedence.
-- ---------------------------------------------------------------------------
SELECT primary_key,
       max(lsn)            AS last_lsn,
       count(*)            AS events_seen,
       bool_or(operation = 'DELETE') AS saw_delete
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 GROUP BY primary_key
 ORDER BY primary_key;

-- ---------------------------------------------------------------------------
-- 4. Discrepancy report: keys whose projection disagrees with the history.
--    A row here is a failed reconciliation, not an informational record.
--    `projection_missing` means the history knows a key that the projection
--    does not; `position_behind` means the projection applied an older source
--    position than the history has.
-- ---------------------------------------------------------------------------
WITH expected AS (
    SELECT primary_key,
           max(lsn) AS last_lsn
      FROM raw.order_cdc_events
     WHERE table_name = 'orders'
     GROUP BY primary_key
),
projection AS (
    SELECT order_id,
           last_lsn,
           is_deleted
      FROM silver.orders_current
)
SELECT expected.primary_key                AS order_key,
       expected.last_lsn                  AS history_last_lsn,
       projection.last_lsn                 AS projection_last_lsn,
       projection.is_deleted               AS projection_is_deleted,
       CASE
         WHEN projection.order_id IS NULL            THEN 'projection_missing'
         WHEN projection.last_lsn < expected.last_lsn THEN 'position_behind'
         ELSE 'ok'
       END                                 AS discrepancy
  FROM expected
  LEFT JOIN projection
    ON projection.order_id = CAST(
      regexp_extract(expected.primary_key, 'order_id=([^#]+)', 1) AS BIGINT
    )
 ORDER BY discrepancy, order_key;

-- ---------------------------------------------------------------------------
-- 5. Event counts per operation, the headline observability counter (SRS FR-017).
-- ---------------------------------------------------------------------------
SELECT operation,
       count(*)                            AS events,
       count(DISTINCT event_id)            AS distinct_events,
       bool_or(is_snapshot)                AS includes_snapshot
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 GROUP BY operation
 ORDER BY operation;

-- ---------------------------------------------------------------------------
-- 6. Dead letter overview, by reason code.
-- ---------------------------------------------------------------------------
SELECT reason_code, count(*) AS rejected_events
  FROM ops.cdc_dead_letter
 GROUP BY reason_code
 ORDER BY reason_code;
