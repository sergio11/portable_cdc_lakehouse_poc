-- Batch correction of serving.orders_current after silver is materialized.

MERGE INTO pcl.serving.orders_current AS target
USING (
  SELECT order_id,
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
    FROM pcl.silver.orders_current
) AS source
ON target.order_id = source.order_id
WHEN MATCHED
 AND (
   source.last_lsn > target.last_lsn
   OR (
     source.last_lsn = target.last_lsn
     AND coalesce(try_cast(source.last_tx_id AS BIGINT), -1)
         > coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
   )
   OR (
     source.last_lsn = target.last_lsn
     AND coalesce(try_cast(source.last_tx_id AS BIGINT), -1)
         = coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
     AND coalesce(source.last_seq_no, -1) > coalesce(target.last_seq_no, -1)
   )
   OR (
     source.last_lsn = target.last_lsn
     AND coalesce(try_cast(source.last_tx_id AS BIGINT), -1)
         = coalesce(try_cast(target.last_tx_id AS BIGINT), -1)
     AND coalesce(source.last_seq_no, -1) = coalesce(target.last_seq_no, -1)
     AND source.last_event_id >= target.last_event_id
   )
 )
THEN UPDATE SET
  customer_id = source.customer_id,
  status = source.status,
  total_amount = source.total_amount,
  currency = source.currency,
  is_deleted = source.is_deleted,
  last_lsn = source.last_lsn,
  last_tx_id = source.last_tx_id,
  last_seq_no = source.last_seq_no,
  last_event_id = source.last_event_id,
  last_operation = source.last_operation,
  last_source_timestamp = source.last_source_timestamp,
  updated_at = source.updated_at,
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
       source.customer_id,
       source.status,
       source.total_amount,
       source.currency,
       source.is_deleted,
       source.last_lsn,
       source.last_tx_id,
       source.last_seq_no,
       source.last_event_id,
       source.last_operation,
       source.last_source_timestamp,
       source.updated_at,
       source.batch_id
     );

SELECT 'serving_rows' AS metric, count(*) AS value
  FROM pcl.serving.orders_current;
SELECT 'serving_live' AS metric, count(*) AS value
  FROM pcl.serving.orders_current
 WHERE NOT is_deleted;
