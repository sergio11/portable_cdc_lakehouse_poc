-- Spark DDL for the lakehouse. Run with `rake lakehouse:init`; idempotent.

CREATE NAMESPACE IF NOT EXISTS pcl.raw;
CREATE NAMESPACE IF NOT EXISTS pcl.silver;
CREATE NAMESPACE IF NOT EXISTS pcl.serving;
CREATE NAMESPACE IF NOT EXISTS pcl.ops;

-- Append-only event log; at-least-once, so event_id dedup happens downstream.
CREATE TABLE IF NOT EXISTS pcl.raw.order_cdc_events (
    event_id               STRING         NOT NULL,
    source_id              STRING         NOT NULL,
    schema_name            STRING         NOT NULL,
    table_name             STRING         NOT NULL,
    primary_key            STRING         NOT NULL,
    operation              STRING         NOT NULL,
    lsn                    BIGINT         NOT NULL,
    tx_id                  STRING,
    seq_no                 BIGINT,
    source_timestamp       TIMESTAMP,
    connector_timestamp    TIMESTAMP,
    is_snapshot            BOOLEAN        NOT NULL,
    before_image           MAP<STRING, STRING>,
    after_image            MAP<STRING, STRING>,
    schema_version         INT            NOT NULL,
    raw_reference          STRING,
    batch_id               STRING         NOT NULL,
    ingested_at            TIMESTAMP      NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Authoritative projection, one row per order.
CREATE TABLE IF NOT EXISTS pcl.silver.orders_current (
    order_id       BIGINT         NOT NULL,
    customer_id    BIGINT,
    status         STRING,
    total_amount   DECIMAL(12,2),
    currency       STRING,
    is_deleted     BOOLEAN        NOT NULL,
    last_lsn       BIGINT         NOT NULL,
    last_tx_id     STRING,
    last_seq_no    BIGINT,
    last_event_id  STRING         NOT NULL,
    last_operation STRING         NOT NULL,
    last_source_timestamp TIMESTAMP,
    updated_at     TIMESTAMP      NOT NULL,
    batch_id       STRING         NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Low-latency upsert target: identifier field plus merge-on-read upserts.
CREATE TABLE IF NOT EXISTS pcl.serving.orders_current (
    order_id       BIGINT         NOT NULL,
    customer_id    BIGINT,
    status         STRING,
    total_amount   DECIMAL(12,2),
    currency       STRING,
    is_deleted     BOOLEAN        NOT NULL,
    last_lsn       BIGINT         NOT NULL,
    last_tx_id     STRING,
    last_seq_no    BIGINT,
    last_event_id  STRING         NOT NULL,
    last_operation STRING         NOT NULL,
    last_source_timestamp TIMESTAMP,
    updated_at     TIMESTAMP      NOT NULL,
    batch_id       STRING         NOT NULL
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read'
);

ALTER TABLE pcl.serving.orders_current SET IDENTIFIER FIELDS order_id;

CREATE TABLE IF NOT EXISTS pcl.ops.cdc_dead_letter (
    event_id        STRING,
    reason_code     STRING         NOT NULL,
    raw_reference   STRING,
    payload         STRING,
    observed_at     TIMESTAMP      NOT NULL,
    run_id          STRING         NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- Incremental watermark: one row per target table.
CREATE TABLE IF NOT EXISTS pcl.ops.materialization_state (
    target_table      STRING         NOT NULL,
    last_ingested_at  TIMESTAMP      NOT NULL,
    updated_at        TIMESTAMP      NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
