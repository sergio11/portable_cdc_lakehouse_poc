-- Incremental materialization. Run with `rake lakehouse:materialize`.
-- Re-ranks only the keys touched since the watermark; a replay is a full rebuild.

CREATE OR REPLACE TEMPORARY VIEW pcl_watermark AS
SELECT coalesce(max(last_ingested_at), TIMESTAMP '1970-01-01 00:00:00') AS last_ingested_at
  FROM pcl.ops.materialization_state
 WHERE target_table = 'silver.orders_current';

CREATE OR REPLACE TEMPORARY VIEW pcl_upper_bound AS
SELECT coalesce(max(ingested_at), TIMESTAMP '1970-01-01 00:00:00') AS upper_bound
  FROM pcl.raw.order_cdc_events
 WHERE table_name = 'orders';

-- Detection uses arrival time so late events with an old LSN are still noticed.
CREATE OR REPLACE TEMPORARY VIEW pcl_changed_keys AS
SELECT DISTINCT e.primary_key
  FROM pcl.raw.order_cdc_events e
 CROSS JOIN pcl_watermark w
 CROSS JOIN pcl_upper_bound u
 WHERE e.table_name = 'orders'
   AND e.ingested_at >= w.last_ingested_at
   AND e.ingested_at <= u.upper_bound;

CREATE OR REPLACE TEMPORARY VIEW pcl_raw_scope AS
SELECT e.*
  FROM pcl.raw.order_cdc_events e
  JOIN pcl_changed_keys k
    ON e.primary_key = k.primary_key
 WHERE e.table_name = 'orders';

-- __PROJECTION_SOURCE__

SELECT 'changed_keys' AS metric, count(*) AS value FROM pcl_changed_keys;
SELECT 'projected_keys' AS metric, count(*) AS value FROM pcl_projection_source;

-- __MERGE__

MERGE INTO pcl.ops.materialization_state AS target
USING (
  SELECT 'silver.orders_current' AS target_table,
         upper_bound AS last_ingested_at
    FROM pcl_upper_bound
) AS source
ON target.target_table = source.target_table
WHEN MATCHED THEN UPDATE SET
  last_ingested_at = source.last_ingested_at,
  updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (target_table, last_ingested_at, updated_at)
VALUES (source.target_table, source.last_ingested_at, current_timestamp());
