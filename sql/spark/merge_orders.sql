-- Shared MERGE, spliced into materialize and replay by Rake.
-- Spark allows one WHEN MATCHED, so a DELETE preserves business columns via CASE.

MERGE INTO __TARGET_TABLE__ AS target
USING pcl_projection_source AS source
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
THEN UPDATE SET
  customer_id = CASE WHEN source.operation = 'DELETE' THEN target.customer_id
                     ELSE try_cast(source.image['customer_id'] AS BIGINT) END,
  status = CASE WHEN source.operation = 'DELETE' THEN target.status
                ELSE source.image['status'] END,
  total_amount = CASE WHEN source.operation = 'DELETE' THEN target.total_amount
                      ELSE try_cast(source.image['total_amount'] AS DECIMAL(12,2)) END,
  currency = CASE WHEN source.operation = 'DELETE' THEN target.currency
                  ELSE source.image['currency'] END,
  is_deleted = source.operation = 'DELETE',
  last_lsn = source.lsn,
  last_tx_id = source.tx_id,
  last_seq_no = source.seq_no,
  last_event_id = source.event_id,
  last_operation = source.operation,
  last_source_timestamp = source.source_timestamp,
  updated_at = CASE WHEN source.operation = 'DELETE'
                    THEN coalesce(source.source_timestamp, current_timestamp())
                    ELSE source.business_updated_at END,
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
