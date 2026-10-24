-- Materialize the latest source-positioned event per order into the current-state
-- projection (SRS FR-007, FR-008). The append-only raw table remains authoritative.
-- Safe to re-run: old/equal positions cannot overwrite a newer projection row.

MERGE INTO __TARGET_TABLE__ AS target
USING (
    SELECT order_id,
           operation,
           lsn,
           tx_id,
           tx_id_num,
           seq_no,
           event_id,
           image,
           source_timestamp,
           batch_id
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
               batch_id,
               row_number() OVER (
                 PARTITION BY primary_key
                 ORDER BY lsn DESC,
                          try_cast(tx_id AS BIGINT) DESC NULLS LAST,
                          seq_no DESC NULLS LAST,
                          event_id DESC
               ) AS event_rank
          FROM iceberg.raw.order_cdc_events
         WHERE __RAW_FILTER__
      ) ranked
     WHERE event_rank = 1
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
                updated_at = try_cast(from_iso8601_timestamp(source.image['updated_at']) AS TIMESTAMP),
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
       coalesce(
         try_cast(from_iso8601_timestamp(source.image['updated_at']) AS TIMESTAMP),
         source.source_timestamp
       ),
       source.batch_id
     );
