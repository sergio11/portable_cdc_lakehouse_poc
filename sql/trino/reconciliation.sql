-- ---------------------------------------------------------------------------
-- Reconciliation queries.
--
-- Run with:  rake query:file[sql/trino/reconciliation.sql]
--
-- Trino is read-only here: this file only reads the append-only history and the
-- projection. Transformation lives in Spark (rake lakehouse:*).
--
-- Everything here is derived from raw.order_cdc_events, so the append-only
-- history is the audit trail and the projection is the thing under test.
--
-- Raw rows are deduplicated by event_id first: Kafka delivery is at-least-once
-- and a restart without committed offsets can append the same event again.
-- Comparing the projection against the raw stream without that step would
-- report false duplicates.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 1. Live vs deleted split of the projection. Business queries always filter
--    is_deleted = false; this makes an accidental filter regression visible.
-- ---------------------------------------------------------------------------
SELECT is_deleted, count(*) AS rows_in_projection
  FROM silver.orders_current
 GROUP BY is_deleted
 ORDER BY is_deleted;

-- ---------------------------------------------------------------------------
-- 2. Duplicate delivery report. A non-empty result is expected after a replay
--    or a restart without checkpointing; it is not a data-loss condition, but
--    it must be visible. `copies` is the number of times the same event_id was
--    appended to the audit table.
-- ---------------------------------------------------------------------------
SELECT count(*) AS duplicated_event_ids,
       coalesce(sum(copies - 1), 0) AS extra_copies
  FROM (
    SELECT event_id, count(*) AS copies
      FROM raw.order_cdc_events
     WHERE table_name = 'orders'
     GROUP BY event_id
    HAVING count(*) > 1
  ) duplicates;

-- ---------------------------------------------------------------------------
-- 3. Expected key set, derived by replaying the history: the set of keys ever
--    observed. Any key present in the projection but absent here would be a
--    phantom row; any key here but absent from the projection a lost row.
-- ---------------------------------------------------------------------------
SELECT DISTINCT primary_key AS order_key
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 4. Last event per key by source position, which is what the projection is
--    expected to reflect. source_timestamp is deliberately NOT used for
--    ordering: arrival or wall-clock order is not a valid precedence.
-- ---------------------------------------------------------------------------
SELECT primary_key,
       max(lsn)            AS last_lsn,
       count(*)            AS events_seen,
       count(DISTINCT event_id) AS distinct_events,
       bool_or(operation = 'DELETE') AS saw_delete
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 GROUP BY primary_key
 ORDER BY primary_key;

-- ---------------------------------------------------------------------------
-- 5. Discrepancy report. This compares the projection against the latest
--    business image in the history, not just the source position. A row here
--    is a failed reconciliation, not an informational record.
--
--    Checks:
--      projection_missing  - the history knows a key the projection does not
--      position_behind     - the projection applied an older source position
--      position_ahead      - the projection is newer than the history
--      tombstone_mismatch  - is_deleted disagrees with the last operation
--      operation_mismatch  - last_operation disagrees with the history
--      *_mismatch          - a business column differs from the latest image
-- ---------------------------------------------------------------------------
WITH deduped AS (
    SELECT *
      FROM (
        SELECT e.*,
               row_number() OVER (
                 PARTITION BY event_id
                 ORDER BY ingested_at DESC
               ) AS dedup_rank
          FROM raw.order_cdc_events e
         WHERE table_name = 'orders'
      ) copies
     WHERE dedup_rank = 1
),
ranked AS (
    SELECT primary_key,
           operation,
           lsn,
           tx_id,
           seq_no,
           event_id,
           coalesce(after_image, before_image) AS image,
           row_number() OVER (
             PARTITION BY primary_key
             ORDER BY lsn DESC,
                      try_cast(tx_id AS BIGINT) DESC NULLS LAST,
                      seq_no DESC NULLS LAST,
                      event_id DESC
           ) AS event_rank
      FROM deduped
),
expected AS (
    SELECT try_cast(regexp_extract(primary_key, 'order_id=([^#]+)', 1) AS BIGINT) AS order_id,
           operation AS last_operation,
           operation = 'DELETE' AS is_deleted,
           image,
           lsn AS last_lsn,
           event_id AS last_event_id
      FROM ranked
     WHERE event_rank = 1
),
projection AS (
    SELECT order_id,
           customer_id,
           status,
           total_amount,
           currency,
           is_deleted,
           last_lsn,
           last_operation,
           last_event_id
      FROM silver.orders_current
)
SELECT expected.order_id,
       expected.last_lsn              AS history_last_lsn,
       projection.last_lsn            AS projection_last_lsn,
       expected.last_operation        AS history_last_operation,
       projection.last_operation      AS projection_last_operation,
       expected.is_deleted            AS expected_is_deleted,
       projection.is_deleted          AS projection_is_deleted,
       CASE
         WHEN projection.order_id IS NULL              THEN 'projection_missing'
         WHEN projection.last_lsn < expected.last_lsn  THEN 'position_behind'
         WHEN projection.last_lsn > expected.last_lsn  THEN 'position_ahead'
         WHEN projection.is_deleted IS DISTINCT FROM expected.is_deleted
                                                       THEN 'tombstone_mismatch'
         WHEN projection.last_operation IS DISTINCT FROM expected.last_operation
                                                       THEN 'operation_mismatch'
         WHEN projection.customer_id IS DISTINCT FROM try_cast(expected.image['customer_id'] AS BIGINT)
                                                       THEN 'customer_id_mismatch'
         WHEN projection.status IS DISTINCT FROM expected.image['status']
                                                       THEN 'status_mismatch'
         WHEN projection.total_amount IS DISTINCT FROM try_cast(expected.image['total_amount'] AS DECIMAL(12,2))
                                                       THEN 'total_amount_mismatch'
         WHEN projection.currency IS DISTINCT FROM expected.image['currency']
                                                       THEN 'currency_mismatch'
         ELSE 'ok'
       END                            AS discrepancy
  FROM expected
  LEFT JOIN projection
    ON projection.order_id = expected.order_id
 ORDER BY discrepancy, expected.order_id;

-- ---------------------------------------------------------------------------
-- 6. Event counts per operation, the headline observability counter.
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
-- 7. Dead letter overview, by reason code.
-- ---------------------------------------------------------------------------
SELECT reason_code, count(*) AS rejected_events
  FROM ops.cdc_dead_letter
 GROUP BY reason_code
 ORDER BY reason_code;
