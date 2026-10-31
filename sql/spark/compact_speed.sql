-- Lambda speed-layer compaction: release every real-time delivery whose event is
-- already canonical in raw. Keying on event_id (not on time or on a batch id) is
-- what makes this safe while the speed job keeps writing: a brand-new event is
-- never removed, and a duplicate is removed only after batch owns it.
--
-- Run with `rake lakehouse:compact-speed`; the scheduled graph runs it after
-- every materialization.

MERGE INTO pcl.speed.order_cdc_events AS target
USING (
    SELECT DISTINCT event_id
      FROM pcl.raw.order_cdc_events
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN DELETE;

SELECT 'speed_rows' AS metric, count(*) AS value
  FROM pcl.speed.order_cdc_events;
SELECT 'speed_event_ids' AS metric, count(DISTINCT event_id) AS value
  FROM pcl.speed.order_cdc_events;
SELECT 'speed_stale' AS metric, count(*) AS value
  FROM pcl.speed.order_cdc_events s
  JOIN pcl.raw.order_cdc_events r
    ON r.event_id = s.event_id;
