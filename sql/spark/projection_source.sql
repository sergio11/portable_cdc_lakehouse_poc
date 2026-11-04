-- Shared ranking for materialize and replay: newest delivery per event_id,
-- then LSN order decides the winning row per key.

CREATE OR REPLACE TEMPORARY VIEW pcl_projection_source AS
WITH deduped AS (
    SELECT *
      FROM (
        SELECT e.*,
               row_number() OVER (
                 PARTITION BY event_id
                 ORDER BY ingested_at DESC
               ) AS dedup_rank
          FROM pcl_raw_scope e
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
               coalesce(
                 try_cast(coalesce(after_image, before_image)['updated_at'] AS TIMESTAMP),
                 timestamp_millis(try_cast(coalesce(after_image, before_image)['updated_at'] AS BIGINT)),
                 source_timestamp,
                 current_timestamp()
               ) AS business_updated_at,
               batch_id,
               primary_key
          FROM deduped
      ) projected
)
SELECT *
  FROM ranked
 WHERE event_rank = 1
   AND order_id IS NOT NULL;
