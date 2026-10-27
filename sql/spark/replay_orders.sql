-- Full replay into an isolated table. Run with `rake lakehouse:replay`.
-- Rake fills in the target table and the raw filter. Never touches the watermark.

DROP TABLE IF EXISTS __TARGET_TABLE__ PURGE;

CREATE TABLE __TARGET_TABLE__
USING iceberg
TBLPROPERTIES ('format-version' = '2')
AS SELECT * FROM pcl.silver.orders_current WHERE 1 = 0;

CREATE OR REPLACE TEMPORARY VIEW pcl_raw_scope AS
SELECT *
  FROM pcl.raw.order_cdc_events
 WHERE table_name = 'orders'
   AND (__RAW_FILTER__);

-- __PROJECTION_SOURCE__

-- __MERGE__
