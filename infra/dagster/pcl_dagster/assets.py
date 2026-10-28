"""The lakehouse graph.

    debezium_connector ─┐
    lakehouse_tables ───┴─> raw_cdc_events ──> orders_current ──> checks

`raw_cdc_events` lands deliveries and merges them into canonical raw by event_id;
`orders_current` materializes the projection. Every step is idempotent, so a
retry or an overlapping tick cannot corrupt the tables. Replay is deliberately
outside the scheduled graph: it is an explicit request with its own window.
"""

import re
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    Config,
    asset,
    asset_check,
)

from .config import WORKSPACE
from .resources import Kafka, Podman, Trino
from .steps import (
    connector_status,
    ensure_pipeline_jar,
    expand_sql,
    run_beam,
    run_spark,
    spark_metrics,
)

REPLAY_ID = re.compile(r"^[a-z][a-z0-9_]{0,39}$")

EXPECTED_TABLES = (
    ("raw", "order_cdc_events"),
    ("raw", "order_cdc_events_landing"),
    ("silver", "orders_current"),
    ("ops", "materialization_state"),
    ("ops", "cdc_dead_letter"),
)


@asset(group_name="source", description="Registers the Debezium connector if it is missing.")
def debezium_connector(context: AssetExecutionContext) -> dict[str, str]:
    status = connector_status()
    context.add_output_metadata(status)
    return status


@asset(group_name="lakehouse", description="Creates the Iceberg schemas and tables when missing.")
def lakehouse_tables(context: AssetExecutionContext, trino: Trino, podman: Podman) -> dict[str, int]:
    present = {
        (schema, table)
        for schema, table in trino.query(
            "SELECT table_schema, table_name FROM iceberg.information_schema.tables "
            "WHERE table_schema IN ('raw', 'silver', 'ops')"
        )
    }
    missing = [table for table in EXPECTED_TABLES if table not in present]

    if missing:
        # DDL is idempotent, but it costs a Spark container: only pay for it when
        # the catalog is actually incomplete.
        context.log.info("Missing tables %s; running the bootstrap DDL.", missing)
        bootstrap = (WORKSPACE / "sql/spark/bootstrap_catalog.sql").read_text(encoding="utf-8")
        run_spark(podman, bootstrap, "bootstrap_catalog")
    else:
        context.log.info("All %d Iceberg tables are present.", len(EXPECTED_TABLES))

    context.add_output_metadata({"missing_tables": len(missing)})
    return {"missing_tables": len(missing)}


@asset(
    deps=[debezium_connector, lakehouse_tables],
    group_name="ingest",
    description="Lands new Kafka deliveries and merges them into raw, one row per event_id.",
)
def raw_cdc_events(
    context: AssetExecutionContext, kafka: Kafka, trino: Trino, podman: Podman
) -> dict[str, int]:
    pending, end_offsets = kafka.lag()
    landing = int(
        trino.scalar("SELECT count(*) FROM iceberg.raw.order_cdc_events_landing") or 0
    )
    duplicates = int(
        trino.scalar(
            """SELECT count(*)
                 FROM (
                   SELECT event_id
                     FROM iceberg.raw.order_cdc_events
                    GROUP BY event_id
                   HAVING count(*) > 1
                 ) duplicated"""
        )
        or 0
    )
    context.add_output_metadata(
        {
            "pending_records": pending,
            "landing_backlog": landing,
            "raw_duplicates_before": duplicates,
        }
    )

    if pending == 0 and landing == 0 and duplicates == 0:
        # An idle tick must not start a JVM: report it and leave the offsets alone.
        context.log.info("No pending records for %s.", kafka.topic)
        return {
            "pending": 0,
            "read": 0,
            "accepted": 0,
            "rejected": 0,
            "tombstones": 0,
            "landing_rows": 0,
        }

    read = accepted = rejected = tombstones = 0
    if pending > 0:
        ensure_pipeline_jar(podman)
        context.log.info("Reading %d pending records from %s.", pending, kafka.topic)
        metrics = run_beam(podman, pending)

        read = metrics.get("elements_read", 0)
        accepted = metrics.get("accepted_events", 0)
        rejected = metrics.get("rejected_events", 0)
        tombstones = metrics.get("kafka_tombstones", 0)
        context.add_output_metadata(
            {
                "pending_records": pending,
                "records_read": read,
                "accepted_events": accepted,
                "rejected_events": rejected,
                "kafka_tombstones": metrics.get("kafka_tombstones", 0),
                "data_files_written": metrics.get("dataFilesWritten", 0),
            }
        )

    merge_sql = (WORKSPACE / "sql/spark/merge_raw.sql").read_text(encoding="utf-8")
    merge = spark_metrics(run_spark(podman, merge_sql, "merge_raw"))
    context.add_output_metadata(
        {
            "landing_rows": merge.get("landing_rows", -1),
            "landing_events": merge.get("landing_events", -1),
            "landing_duplicates": merge.get("landing_duplicates", -1),
            "raw_rows": merge.get("raw_rows", -1),
            "raw_event_ids": merge.get("raw_event_ids", -1),
            "landing_remaining": merge.get("landing_remaining", -1),
        }
    )

    # Commit only after the canonical MERGE. A crash before this point leaves the
    # offsets uncommitted; the next run re-reads and the MERGE is idempotent.
    if pending > 0 and read == pending:
        kafka.commit(end_offsets)
        context.log.info("Committed offsets %s after the merge.", end_offsets)
    elif pending > 0:
        context.log.warning(
            "Read %d of %d pending records; leaving offsets uncommitted.", read, pending
        )

    return {
        "pending": pending,
        "read": read,
        "accepted": accepted,
        "rejected": rejected,
        "tombstones": tombstones,
        "landing_rows": merge.get("landing_rows", 0),
    }


@asset(
    deps=[raw_cdc_events],
    group_name="transform",
    description="Merges new raw events into the current-state projection with Spark.",
)
def orders_current(context: AssetExecutionContext, trino: Trino, podman: Podman) -> dict[str, int]:
    if int(trino.scalar("SELECT count(*) FROM iceberg.ops.materialization_state")) == 0:
        context.log.info("No watermark yet; the first materialization is a full scan.")
        pending = 1
    else:
        # Strictly newer than the watermark: the watermark is the newest arrival
        # already folded, so the batch it came from is not pending work. The SQL
        # itself keeps `>=` so a re-run never loses a boundary-timestamp event.
        pending = int(
            trino.scalar(
                "SELECT count(*) FROM iceberg.raw.order_cdc_events e "
                "JOIN iceberg.ops.materialization_state s "
                "  ON s.target_table = 'silver.orders_current' "
                "WHERE e.ingested_at > s.last_ingested_at"
            )
        )

    if pending == 0:
        # The MERGE is idempotent but not free: it rewrites matched files and
        # commits an overwrite snapshot even when no value changes.
        context.log.info("No raw events since the watermark; skipping the MERGE.")
        context.add_output_metadata({"pending_raw_events": 0, "spark_ran": False})
        return {"pending": 0}

    output = run_spark(
        podman,
        expand_sql("materialize_orders.sql", target="pcl.silver.orders_current"),
        "materialize_orders",
    )
    metrics = spark_metrics(output)

    watermark = trino.scalar(
        "SELECT CAST(last_ingested_at AS VARCHAR) FROM iceberg.ops.materialization_state "
        "WHERE target_table = 'silver.orders_current'"
    )
    keys = int(trino.scalar("SELECT count(*) FROM iceberg.silver.orders_current"))
    live = int(
        trino.scalar("SELECT count(*) FROM iceberg.silver.orders_current WHERE NOT is_deleted")
    )

    context.add_output_metadata(
        {
            "pending_raw_events": pending,
            "changed_keys": metrics.get("changed_keys", -1),
            "projected_keys": metrics.get("projected_keys", -1),
            "watermark": watermark,
            "projection_keys": keys,
            "live_keys": live,
        }
    )
    return {"pending": pending, "keys": keys, "live": live}


@asset_check(
    asset=raw_cdc_events,
    blocking=True,
    description="Every record in the batch was accepted, quarantined, or a delete marker.",
)
def batch_fully_accounted(raw_cdc_events: dict) -> AssetCheckResult:
    # Rejections are expected: a malformed envelope is quarantined, not lost. The
    # invariant is that the batch accounts for every record it read, so a silent
    # drop fails the run while a dead letter does not.
    read = raw_cdc_events.get("read", 0)
    accepted = raw_cdc_events.get("accepted", 0)
    rejected = raw_cdc_events.get("rejected", 0)
    tombstones = raw_cdc_events.get("tombstones", 0)
    accounted = accepted + rejected + tombstones
    return AssetCheckResult(
        passed=accounted == read,
        metadata={
            "records_read": read,
            "accepted_events": accepted,
            "rejected_events": rejected,
            "kafka_tombstones": tombstones,
        },
        description=f"{accounted} of {read} records were accepted, quarantined, or delete markers.",
    )


@asset_check(
    asset=raw_cdc_events, blocking=True, description="raw holds exactly one row per event_id."
)
def raw_events_unique(trino: Trino) -> AssetCheckResult:
    duplicates = int(
        trino.scalar(
            """SELECT count(*)
                 FROM (
                   SELECT event_id
                     FROM iceberg.raw.order_cdc_events
                    GROUP BY event_id
                   HAVING count(*) > 1
                 ) duplicated"""
        )
        or 0
    )
    return AssetCheckResult(
        passed=duplicates == 0,
        metadata={"duplicated_event_ids": duplicates},
        description=f"{duplicates} event ids appear more than once in raw.",
    )


@asset_check(
    asset=orders_current,
    blocking=True,
    description="The projection agrees with the replayed history, key by key.",
)
def projection_matches_history(trino: Trino) -> AssetCheckResult:
    # The discrepancy report is reused from the manual reconciliation file, so
    # the scheduled check and `rake query:file` cannot drift apart.
    report = next(
        statement
        for statement in _statements(WORKSPACE / "sql/trino/reconciliation.sql")
        if "AS discrepancy" in statement
    )
    mismatches = int(
        trino.scalar(f"SELECT count(*) FROM ({report}) AS checked WHERE discrepancy <> 'ok'")
    )
    return AssetCheckResult(
        passed=mismatches == 0,
        metadata={"mismatched_keys": mismatches},
        description=f"{mismatches} keys disagree with the history.",
    )


class ReplayConfig(Config):
    """Rebuild window. Omit both bounds to replay the whole history."""

    replay_id: str
    from_lsn: int | None = None
    to_lsn: int | None = None


@asset(
    group_name="replay",
    description="Rebuilds the projection from raw history into an isolated table.",
)
def orders_current_replay(
    context: AssetExecutionContext, config: ReplayConfig, trino: Trino, podman: Podman
) -> None:
    if not REPLAY_ID.match(config.replay_id):
        raise ValueError("replay_id must match [a-z][a-z0-9_]{0,39}")
    if config.from_lsn is not None and config.to_lsn is not None:
        if config.from_lsn > config.to_lsn:
            raise ValueError("from_lsn must not exceed to_lsn")

    target = f"pcl.silver.orders_current_replay_{config.replay_id}"
    serving = f"iceberg.silver.orders_current_replay_{config.replay_id}"

    bounds = [
        f"lsn >= {config.from_lsn}" if config.from_lsn is not None else None,
        f"lsn <= {config.to_lsn}" if config.to_lsn is not None else None,
    ]
    raw_filter = " AND ".join(bound for bound in bounds if bound) or "TRUE"

    run_spark(
        podman,
        expand_sql("replay_orders.sql", target=target, raw_filter=raw_filter),
        f"replay_{config.replay_id}",
    )

    keys, tombstones = trino.query(
        f"SELECT count(*), count_if(is_deleted) FROM {serving}"
    )[0]
    context.add_output_metadata(
        {"target": target, "keys": keys, "tombstones": tombstones, "raw_filter": raw_filter}
    )

    # A bounded window is a partial history by construction, so only a full
    # replay can be compared against the live projection.
    if config.from_lsn is None and config.to_lsn is None:
        mismatches = int(
            trino.scalar(
                f"""SELECT count(*)
                      FROM (
                        SELECT coalesce(r.order_id, c.order_id) AS order_id
                          FROM {serving} r
                          FULL OUTER JOIN iceberg.silver.orders_current c
                            ON r.order_id = c.order_id
                         WHERE r.order_id IS NULL
                            OR c.order_id IS NULL
                            OR r.is_deleted IS DISTINCT FROM c.is_deleted
                            OR r.status IS DISTINCT FROM c.status
                            OR r.total_amount IS DISTINCT FROM c.total_amount
                            OR r.customer_id IS DISTINCT FROM c.customer_id
                            OR r.currency IS DISTINCT FROM c.currency
                            OR r.last_lsn IS DISTINCT FROM c.last_lsn
                            OR r.last_event_id IS DISTINCT FROM c.last_event_id
                      ) AS mismatches"""
            )
        )
        if mismatches:
            raise ValueError(
                f"Full replay {config.replay_id} differs from silver.orders_current "
                f"in {mismatches} keys."
            )
        context.log.info("Full replay %s matches silver.orders_current exactly.", config.replay_id)


def _statements(path: Path) -> list[str]:
    """Split a SQL file the same way `rake query:file` does."""
    text = path.read_text(encoding="utf-8")
    return [statement.strip() for statement in re.split(r";\s*(?:\r?\n|$)", text) if statement.strip()]
