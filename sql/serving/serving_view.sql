-- Lambda serving layer: merge the authoritative batch view with the real-time
-- speed layer at query time. A speed event only overrides batch when its source
-- position is genuinely newer, so a delayed batch run cannot regress a key that
-- the speed layer already advanced.
--
-- Created and refreshed by `rake lakehouse:serve` (and by the scheduled graph)
-- through Trino, using the dedicated `pcl-provisioner` identity. Trino owns the
-- view definition because Iceberg stores one SQL representation per dialect and
-- Trino rejects a view written in Spark's dialect; all table writes stay in
-- Spark. The body references the `pcl` catalog alias, which Trino also
-- registers, so the same view is readable as `iceberg.serving.orders_current`
-- and `pcl.serving.orders_current`.

CREATE OR REPLACE VIEW pcl.serving.orders_current AS
WITH batch AS (
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
),
speed_events AS (
    -- `primary_key` is the canonical `k=v#k=v` reference produced by the
    -- pipeline. Take the `order_id=` entry explicitly instead of assuming it
    -- sorts first, so a wider primary key cannot silently pick another column.
    SELECT TRY_CAST(
             element_at(
               transform(
                 filter(split(primary_key, '#'), part -> part LIKE 'order_id=%'),
                 part -> element_at(split(part, '='), 2)
               ),
               1
             ) AS BIGINT
           ) AS order_id,
           operation,
           lsn,
           tx_id,
           seq_no,
           event_id,
           coalesce(after_image, before_image) AS image,
           source_timestamp,
           batch_id
      FROM pcl.speed.order_cdc_events
     WHERE table_name = 'orders'
),
speed AS (
    SELECT order_id,
           TRY_CAST(image['customer_id'] AS BIGINT) AS customer_id,
           image['status'] AS status,
           TRY_CAST(image['total_amount'] AS DECIMAL(12, 2)) AS total_amount,
           image['currency'] AS currency,
           operation = 'DELETE' AS is_deleted,
           lsn AS last_lsn,
           tx_id AS last_tx_id,
           seq_no AS last_seq_no,
           event_id AS last_event_id,
           operation AS last_operation,
           source_timestamp AS last_source_timestamp,
           -- Same precedence as the batch projection, so the row does not
           -- change shape when the same event moves from speed to batch.
           coalesce(
             TRY_CAST(image['updated_at'] AS TIMESTAMP),
             source_timestamp,
             CAST(current_timestamp AS TIMESTAMP)
           ) AS updated_at,
           batch_id
      FROM speed_events
     WHERE order_id IS NOT NULL
),
merged AS (
    SELECT *, 0 AS layer_rank FROM batch
    UNION ALL
    SELECT *, 1 AS layer_rank FROM speed
),
deduped AS (
    SELECT *,
           row_number() OVER (
             PARTITION BY last_event_id
             ORDER BY layer_rank
           ) AS copy_rank
      FROM merged
),
ranked AS (
    SELECT *,
           row_number() OVER (
             PARTITION BY order_id
             ORDER BY last_lsn DESC,
                      TRY_CAST(last_tx_id AS BIGINT) DESC NULLS LAST,
                      last_seq_no DESC NULLS LAST,
                      last_event_id DESC
           ) AS event_rank
      FROM deduped
     WHERE copy_rank = 1
)
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
  FROM ranked
 WHERE event_rank = 1;
