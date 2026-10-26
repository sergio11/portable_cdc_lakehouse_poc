-- Materialize the latest source-positioned event per order into the current-state
-- projection. The append-only raw table remains the audit trail.
--
-- Two corrections are applied here:
--   1. Raw rows are deduplicated by `event_id` before ranking. Kafka delivery is
--      at-least-once and the POC does not rely on exactly-once, so the same event
--      can be appended more than once. `ingested_at DESC` keeps the newest copy.
--   2. Ordering is LSN, then numeric transaction id, then intra-transaction
--      sequence, then event id. `event_id` is the deterministic final tie-breaker.
--
-- Safe to re-run: old/equal positions cannot overwrite a newer projection row.

-- Trino does not accept a WITH clause in front of MERGE, so the CTEs live inside
-- the USING subquery instead of preceding the statement.
MERGE INTO __TARGET_TABLE__ AS target
USING (
    WITH deduped AS (
        SELECT *
          FROM (
            SELECT e.*,
                   row_number() OVER (
                     PARTITION BY event_id
                     ORDER BY ingested_at DESC
                   ) AS dedup_rank
              FROM iceberg.raw.order_cdc_events e
             WHERE __RAW_FILTER__
          ) copies
         WHERE dedup_rank = 1
    ),
    ranked AS (
        SELECT order_id,
               operation,
               lsn,
               tx_id,
               tx_id_num,
               seq_no,
               event_id,
               image,
               source_timestamp,
               business_updated_at,
               batch_id,
               row_number() OVER (
                 PARTITION BY primary_key
                 ORDER BY lsn DESC,
                          tx_id_num DESC NULLS LAST,
                          seq_no DESC NULLS LAST,
                          event_id DESC
               ) AS event_rank
          FROM (
            SELECT try_cast(regexp_extract(primary_key, 'order_id=([^#]+)', 1) AS BIGINT) AS order_id,
                   operation,
                   lsn,
                   tx_id,
                   try_cast(tx_id AS BIGINT) AS tx_id_num,
                   seq_no,
                   event_id,
                   coalesce(after_image, before_image) AS image,
                   source_timestamp,
                   -- Debezium's JSON converter emits timestamps as epoch millis when
                   -- schemas are disabled, while a hand-written envelope may carry an
                   -- ISO-8601 string. Accept both, then fall back to the change time.
                   coalesce(
                     try_cast(from_iso8601_timestamp(coalesce(after_image, before_image)['updated_at']) AS TIMESTAMP),
                     try_cast(from_unixtime(try_cast(coalesce(after_image, before_image)['updated_at'] AS DOUBLE) / 1000.0) AS TIMESTAMP),
                     source_timestamp,
                     CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                   ) AS business_updated_at,
                   batch_id,
                   primary_key
              FROM deduped
          ) projected
    )
    SELECT *
      FROM ranked
     WHERE event_rank = 1
       AND order_id IS NOT NULL
) AS source
ON target.order_id = source.order_id
WHEN MATCHED
 AND (
   source.lsn > target.last_lsn
   OR (
     source.lsn = target.last_lsn
     AND coalesce(source.tx_id_num, -1) > coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
   )
   OR (
     source.lsn = target.last_lsn
     AND coalesce(source.tx_id_num, -1) = coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
     AND coalesce(source.seq_no, -1) > coalesce(target.last_seq_no, -1)
   )
 )
 AND source.operation = 'DELETE'
THEN UPDATE SET is_deleted = true,
                last_lsn = source.lsn,
                last_tx_id = source.tx_id,
                last_seq_no = source.seq_no,
                last_event_id = source.event_id,
                last_operation = source.operation,
                last_source_timestamp = source.source_timestamp,
                updated_at = source.source_timestamp,
                batch_id = source.batch_id
WHEN MATCHED
 AND (
   source.lsn > target.last_lsn
   OR (
     source.lsn = target.last_lsn
     AND coalesce(source.tx_id_num, -1) > coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
   )
   OR (
     source.lsn = target.last_lsn
     AND coalesce(source.tx_id_num, -1) = coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
     AND coalesce(source.seq_no, -1) > coalesce(target.last_seq_no, -1)
   )
 )
THEN UPDATE SET customer_id = try_cast(source.image['customer_id'] AS BIGINT),
                status = source.image['status'],
                total_amount = try_cast(source.image['total_amount'] AS DECIMAL(12,2)),
                currency = source.image['currency'],
                is_deleted = false,
                last_lsn = source.lsn,
                last_tx_id = source.tx_id,
                last_seq_no = source.seq_no,
                last_event_id = source.event_id,
                last_operation = source.operation,
                last_source_timestamp = source.source_timestamp,
                updated_at = source.business_updated_at,
                batch_id = source.batch_id
WHEN NOT MATCHED
THEN INSERT (
       order_id,
       customer_id,
       status,
       total_amount,
       currency,
       is_deleted,
       last_lsn,
       last_tx_id,
       last_seq_no,
       last_event_id,
       last_operation,
       last_source_timestamp,
       updated_at,
       batch_id
     ) VALUES (
       source.order_id,
       try_cast(source.image['customer_id'] AS BIGINT),
       source.image['status'],
       try_cast(source.image['total_amount'] AS DECIMAL(12,2)),
       source.image['currency'],
       source.operation = 'DELETE',
       source.lsn,
       source.tx_id,
       source.seq_no,
       source.event_id,
       source.operation,
       source.source_timestamp,
       source.business_updated_at,
       source.batch_id
     );
