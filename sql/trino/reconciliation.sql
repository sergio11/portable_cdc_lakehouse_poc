-- Reconciliation queries. Run: rake query:file[sql/trino/reconciliation.sql]
--
-- raw is at-least-once, so duplicate event_id deliveries are expected (section 2).
-- silver is authoritative; serving may be ahead but must never be behind (8, 9).

-- 1. Live vs deleted split of the projection.
SELECT is_deleted, count(*) AS rows_in_projection
  FROM silver.orders_current
 GROUP BY is_deleted
 ORDER BY is_deleted;

-- 2. Event log shape: distinct event_id is what silver projects.
SELECT count(*)                            AS raw_deliveries,
       count(DISTINCT event_id)             AS raw_events,
       count(*) - count(DISTINCT event_id)  AS duplicate_deliveries
  FROM raw.order_cdc_events
 WHERE table_name = 'orders';

-- 3. Key set ever observed in the history.
SELECT DISTINCT primary_key AS order_key
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 ORDER BY 1;

-- 4. Last event per key by source position (never by wall-clock time).
SELECT primary_key,
       max(lsn)                 AS last_lsn,
       count(*)                 AS deliveries,
       count(DISTINCT event_id) AS distinct_events,
       bool_or(operation = 'DELETE') AS saw_delete
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 GROUP BY primary_key
 ORDER BY primary_key;

-- 5. Discrepancy report: projection vs the latest business image in the history.
WITH deduped AS (
    SELECT *
      FROM (
        SELECT e.*,
               row_number() OVER (
                 PARTITION BY event_id
                 ORDER BY ingested_at DESC
               ) AS delivery_rank
          FROM raw.order_cdc_events e
         WHERE table_name = 'orders'
      ) copies
     WHERE delivery_rank = 1
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

-- 6. Deliveries per operation.
SELECT operation,
       count(*)                 AS deliveries,
       count(DISTINCT event_id) AS events,
       count_if(is_snapshot)    AS snapshot_deliveries
  FROM raw.order_cdc_events
 WHERE table_name = 'orders'
 GROUP BY operation
 ORDER BY operation;

-- 7. Dead letter overview, by reason code.
SELECT reason_code, count(*) AS rejected_events
  FROM ops.cdc_dead_letter
 GROUP BY reason_code
 ORDER BY reason_code;

-- 8. Serving shape: it may be ahead of silver, never behind.
SELECT count(*)                                              AS serving_rows,
       count_if(NOT s.is_deleted)                            AS serving_live,
       count_if(b.order_id IS NULL)                          AS serving_ahead_of_silver,
       count_if(b.order_id IS NOT NULL AND s.last_lsn < b.last_lsn) AS serving_behind_silver
  FROM serving.orders_current s
  LEFT JOIN silver.orders_current b
    ON b.order_id = s.order_id;

-- 9. Serving discrepancies: every key silver knows must be present and not behind.
SELECT b.order_id,
       b.last_lsn            AS silver_last_lsn,
       s.last_lsn            AS serving_last_lsn,
       b.last_operation      AS silver_last_operation,
       s.last_operation      AS serving_last_operation,
       CASE
         WHEN s.order_id IS NULL          THEN 'serving_missing'
         WHEN s.last_lsn < b.last_lsn     THEN 'serving_behind'
         WHEN s.customer_id IS DISTINCT FROM b.customer_id   THEN 'customer_id_mismatch'
         WHEN s.status IS DISTINCT FROM b.status             THEN 'status_mismatch'
         WHEN s.total_amount IS DISTINCT FROM b.total_amount THEN 'total_amount_mismatch'
         WHEN s.currency IS DISTINCT FROM b.currency         THEN 'currency_mismatch'
         WHEN s.is_deleted IS DISTINCT FROM b.is_deleted     THEN 'tombstone_mismatch'
         WHEN s.last_operation IS DISTINCT FROM b.last_operation
                                                             THEN 'operation_mismatch'
         WHEN s.last_event_id IS DISTINCT FROM b.last_event_id
                                                             THEN 'event_mismatch'
         ELSE 'ok'
       END                   AS serving_discrepancy
  FROM silver.orders_current b
  LEFT JOIN serving.orders_current s
    ON s.order_id = b.order_id
 WHERE s.order_id IS NULL
    OR s.last_lsn < b.last_lsn
    OR (
      s.last_lsn = b.last_lsn
      AND (
        s.customer_id IS DISTINCT FROM b.customer_id
        OR s.status IS DISTINCT FROM b.status
        OR s.total_amount IS DISTINCT FROM b.total_amount
        OR s.currency IS DISTINCT FROM b.currency
        OR s.is_deleted IS DISTINCT FROM b.is_deleted
        OR s.last_operation IS DISTINCT FROM b.last_operation
        OR s.last_event_id IS DISTINCT FROM b.last_event_id
      )
    )
 ORDER BY serving_discrepancy, b.order_id;
