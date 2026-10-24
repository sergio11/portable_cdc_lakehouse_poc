-- ---------------------------------------------------------------------------
-- Create the Iceberg schemas and tables used by the POC.
--
-- Executed against Trino, which creates Iceberg tables through the REST catalog
-- configured in infra/trino/etc/catalog/iceberg.properties.template. The REST
-- catalog and Trino both use the MinIO-backed S3 warehouse.
--
-- Conventions
--   * Business payloads are modelled as MAP(VARCHAR, VARCHAR) rather than JSON.
--     The canonical CDC contract carries column values as strings (see
--     domain/model/CdcEvent), which keeps the mapping identical for every
--     PostgreSQL type Debezium emits and avoids depending on engine-specific
--     JSON support in the query layer.
--   * Timestamps are Iceberg `timestamp(6)` (without zone). This is the mapping
--     Beam's IcebergIO documents as SqlTypes.DATETIME -> TIMESTAMP.
--   * format_version=2 is required for the equality deletes and row-level
--     lineage the state projection relies on.
--
-- Idempotent: safe to re-run.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- raw.order_cdc_events: append-only history
--
-- Every accepted event lands here and is never overwritten. This table is the
-- source of truth for replay and reconciliation.
-- `batch_id` ties a row to the run/microbatch that produced it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.order_cdc_events (
    event_id               VARCHAR        NOT NULL,
    source_id              VARCHAR        NOT NULL,
    schema_name            VARCHAR        NOT NULL,
    table_name             VARCHAR        NOT NULL,
    primary_key            VARCHAR        NOT NULL,
    operation              VARCHAR        NOT NULL,
    lsn                    BIGINT         NOT NULL,
    tx_id                  VARCHAR,
    seq_no                 BIGINT,
    source_timestamp       TIMESTAMP(6),
    is_snapshot            BOOLEAN        NOT NULL,
    before_image           MAP(VARCHAR, VARCHAR),
    after_image            MAP(VARCHAR, VARCHAR),
    schema_version         INTEGER        NOT NULL,
    raw_reference          VARCHAR,
    batch_id               VARCHAR        NOT NULL,
    ingested_at            TIMESTAMP(6)   NOT NULL
) WITH (
    format_version = 2
);

-- ---------------------------------------------------------------------------
-- silver.orders_current: current-state projection
--
-- One row per business key. Deletes are represented as a versioned tombstone
-- (is_deleted = true) rather than a physical removal, so an older event replayed
-- later can never resurrect the row. Business queries filter on
-- is_deleted = false.
--
-- last_lsn / last_tx_id / last_seq_no record the source position of the event
-- that produced this row; they are the ordering authority for future updates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.orders_current (
    order_id       BIGINT         NOT NULL,
    customer_id    BIGINT,
    status         VARCHAR,
    total_amount   DECIMAL(12,2),
    currency       VARCHAR,
    is_deleted     BOOLEAN        NOT NULL,
    last_lsn       BIGINT         NOT NULL,
    last_tx_id     VARCHAR,
    last_seq_no    BIGINT,
    last_event_id  VARCHAR        NOT NULL,
    updated_at     TIMESTAMP(6)   NOT NULL,
    batch_id       VARCHAR        NOT NULL
) WITH (
    format_version = 2
);

-- ---------------------------------------------------------------------------
-- ops.cdc_dead_letter: rejected events
--
-- reason_code is a stable, documented identifier so operators and tests can
-- assert on rejection causes without string-matching log output.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.cdc_dead_letter (
    event_id        VARCHAR,
    reason_code     VARCHAR        NOT NULL,
    raw_reference   VARCHAR,
    payload         VARCHAR,
    observed_at     TIMESTAMP(6)   NOT NULL,
    run_id          VARCHAR        NOT NULL
) WITH (
    format_version = 2
);
