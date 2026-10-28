-- Idempotent ingest: raw.order_cdc_events_landing -> raw.order_cdc_events.
-- Beam appends every Kafka delivery to the landing table; this script keeps one
-- row per event_id, so repeated delivery never accumulates in canonical raw.
-- Run with `rake lakehouse:merge`; Dagster runs it before materializing silver.

-- 1. Heal history written before the landing table existed: keep the newest copy.
CREATE OR REPLACE TEMPORARY VIEW pcl_raw_keepers AS
SELECT event_id, max(ingested_at) AS keep_ingested_at
  FROM pcl.raw.order_cdc_events
 GROUP BY event_id;

MERGE INTO pcl.raw.order_cdc_events AS target
USING pcl_raw_keepers AS source
ON target.event_id = source.event_id
WHEN MATCHED AND target.ingested_at < source.keep_ingested_at THEN DELETE;

-- 2. One candidate per event: the newest delivery wins, and its batch is the one
--    recorded in canonical history.
CREATE OR REPLACE TEMPORARY VIEW pcl_landing_deduped AS
SELECT *
  FROM (
    SELECT e.*,
           row_number() OVER (
             PARTITION BY event_id
             ORDER BY ingested_at DESC, batch_id DESC
           ) AS delivery_rank
      FROM pcl.raw.order_cdc_events_landing e
  ) ranked
 WHERE delivery_rank = 1;

-- 3. Canonical history: an event is inserted once; a redelivery is ignored.
MERGE INTO pcl.raw.order_cdc_events AS target
USING pcl_landing_deduped AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT (
  event_id, source_id, schema_name, table_name, primary_key, operation,
  lsn, tx_id, seq_no, source_timestamp, connector_timestamp, is_snapshot,
  before_image, after_image, schema_version, raw_reference, batch_id, ingested_at
) VALUES (
  source.event_id, source.source_id, source.schema_name, source.table_name,
  source.primary_key, source.operation, source.lsn, source.tx_id, source.seq_no,
  source.source_timestamp, source.connector_timestamp, source.is_snapshot,
  source.before_image, source.after_image, source.schema_version,
  source.raw_reference, source.batch_id, source.ingested_at
);

-- 4. Metrics: deliveries, distinct events, absorbed duplicates, canonical counts.
SELECT 'landing_rows' AS metric, count(*) AS value
  FROM pcl.raw.order_cdc_events_landing;
SELECT 'landing_events' AS metric, count(DISTINCT event_id) AS value
  FROM pcl.raw.order_cdc_events_landing;
SELECT 'landing_duplicates' AS metric,
       count(*) - count(DISTINCT event_id) AS value
  FROM pcl.raw.order_cdc_events_landing;
SELECT 'raw_rows' AS metric, count(*) AS value FROM pcl.raw.order_cdc_events;
SELECT 'raw_event_ids' AS metric, count(DISTINCT event_id) AS value
  FROM pcl.raw.order_cdc_events;

-- 5. Release the deliveries whose event is already canonical. Cleanup is keyed on
--    raw rather than on a timestamp, so a delivery that arrives while the merge
--    runs is only dropped when it is a duplicate of an event raw already holds;
--    a new event waits for the next run. A failure before this point is safe:
--    the next run re-merges the same rows and the event_id match is a no-op.
CREATE OR REPLACE TEMPORARY VIEW pcl_landing_absorbed AS
SELECT DISTINCT l.event_id, l.batch_id, l.ingested_at
  FROM pcl.raw.order_cdc_events_landing l
  JOIN pcl.raw.order_cdc_events r
    ON r.event_id = l.event_id;

MERGE INTO pcl.raw.order_cdc_events_landing AS target
USING pcl_landing_absorbed AS source
ON target.event_id = source.event_id
   AND target.batch_id = source.batch_id
   AND target.ingested_at = source.ingested_at
WHEN MATCHED THEN DELETE;

SELECT 'landing_remaining' AS metric, count(*) AS value
  FROM pcl.raw.order_cdc_events_landing;
