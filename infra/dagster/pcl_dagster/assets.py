"""The lakehouse graph: the batch layer of a lambda architecture.

    debezium_connector ─┐
    lakehouse_tables ───┴─> orders_current ──> serving_orders

`orders_current` projects the append-only event log into the authoritative
current state, and `serving_orders` corrects the low-latency serving table
whenever batch is at the same or a newer source position.

The speed writers are not here: `pipeline-speed` appends Kafka deliveries to
`raw.order_cdc_events` and `pipeline-serving` upserts that log into
`serving.orders_current`, both continuously on Flink. The log is never updated
or deleted, which is what lets the Flink streaming reader follow it. Every
batch step is idempotent, so a retry or an overlapping tick cannot corrupt the
tables. Replay is deliberately outside the scheduled graph: it is an explicit
request with its own window.
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
from .resources import Podman, Trino
from .steps import (
    connector_status,
    expand_sql,
    run_spark,
    spark_metrics,
)

REPLAY_ID = re.compile(r"^[a-z][a-z0-9_]{0,39}$")

EXPECTED_TABLES = (
    ("raw", "order_cdc_events"),
    ("silver", "orders_current"),
    ("serving", "orders_current"),
    ("ops", "materialization_state"),
    ("ops", "cdc_dead_letter"),
)

# Serving may be ahead of silver, but never behind on a known key.
SERVING_CORRECTION_WHERE = """
       s.order_id IS NULL
    OR b.last_lsn > s.last_lsn
    OR (b.last_lsn = s.last_lsn AND coalesce(b.last_seq_no, -1) > coalesce(s.last_seq_no, -1))
    OR (
      b.last_lsn = s.last_lsn
      AND coalesce(b.last_seq_no, -1) = coalesce(s.last_seq_no, -1)
      AND (
        s.customer_id IS DISTINCT FROM b.customer_id
        OR s.status IS DISTINCT FROM b.status
        OR s.total_amount IS DISTINCT FROM b.total_amount
        OR s.currency IS DISTINCT FROM b.currency
        OR s.is_deleted IS DISTINCT FROM b.is_deleted
        OR s.last_operation IS DISTINCT FROM b.last_operation
        OR s.last_event_id IS DISTINCT FROM b.last_event_id
      )
    )
"""


@asset(group_name="source", description="Registers the Debezium connector if it is missing.")
def debezium_connector(context: AssetExecutionContext) -> dict[str, str]:
    status = connector_status()
    context.add_output_metadata(status)
    return status


@asset(
    group_name="lakehouse",
    description="Creates the Iceberg schemas and tables when missing.",
)
def lakehouse_tables(context: AssetExecutionContext, trino: Trino, podman: Podman) -> dict[str, int]:
    present = {
        (schema, table)
        for schema, table in trino.query(
            "SELECT table_schema, table_name FROM iceberg.information_schema.tables "
            "WHERE table_schema IN ('raw', 'silver', 'serving', 'ops')"
        )
    }
    missing = [table for table in EXPECTED_TABLES if table not in present]

    if missing:
        # Bootstrap costs a Spark container, so only run it when needed.
        context.log.info("Missing tables %s; running the bootstrap DDL.", missing)
        bootstrap = (WORKSPACE / "sql/spark/bootstrap_catalog.sql").read_text(encoding="utf-8")
        run_spark(podman, bootstrap, "bootstrap_catalog")
    else:
        context.log.info("All %d Iceberg tables are present.", len(EXPECTED_TABLES))

    context.add_output_metadata({"missing_tables": len(missing)})
    return {"missing_tables": len(missing)}


@asset(
    deps=[debezium_connector, lakehouse_tables],
    group_name="transform",
    description="Projects new events from the append-only log into silver.orders_current.",
)
def orders_current(context: AssetExecutionContext, trino: Trino, podman: Podman) -> dict[str, int]:
    if int(trino.scalar("SELECT count(*) FROM iceberg.ops.materialization_state")) == 0:
        context.log.info("No watermark yet; the first materialization is a full scan.")
        pending = 1
    else:
        # Strictly newer than the watermark; the SQL itself keeps `>=`.
        pending = int(
            trino.scalar(
                "SELECT count(*) FROM iceberg.raw.order_cdc_events e "
                "JOIN iceberg.ops.materialization_state s "
                "  ON s.target_table = 'silver.orders_current' "
                "WHERE e.ingested_at > s.last_ingested_at"
            )
        )

    if pending == 0:
        # Skip a MERGE that would only rewrite files.
        context.log.info("No new events since the watermark; skipping the MERGE.")
        context.add_output_metadata({"pending_events": 0, "spark_ran": False})
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
            "pending_events": pending,
            "changed_keys": metrics.get("changed_keys", -1),
            "projected_keys": metrics.get("projected_keys", -1),
            "watermark": watermark,
            "projection_keys": keys,
            "live_keys": live,
        }
    )
    return {"pending": pending, "keys": keys, "live": live}


@asset(
    deps=[orders_current],
    group_name="serving",
    description="Corrects serving.orders_current from canonical silver, by source position.",
)
def serving_orders(context: AssetExecutionContext, trino: Trino, podman: Podman) -> dict[str, int]:
    needing = int(
        trino.scalar(
            f"""SELECT count(*)
                  FROM iceberg.silver.orders_current b
                  LEFT JOIN iceberg.serving.orders_current s
                    ON s.order_id = b.order_id
                 WHERE {SERVING_CORRECTION_WHERE}"""
        )
        or 0
    )
    if needing == 0:
        keys = int(trino.scalar("SELECT count(*) FROM iceberg.serving.orders_current") or 0)
        context.log.info("Serving already matches silver; skipping the correction MERGE.")
        context.add_output_metadata({"keys_needing_correction": 0, "serving_keys": keys})
        return {"corrected": 0, "serving_keys": keys}

    output = run_spark(
        podman,
        (WORKSPACE / "sql/spark/correct_serving.sql").read_text(encoding="utf-8"),
        "correct_serving",
    )
    metrics = spark_metrics(output)
    metrics["keys_needing_correction"] = needing
    context.add_output_metadata(metrics)
    return metrics


@asset_check(
    asset=orders_current,
    blocking=True,
    description="The projection agrees with the replayed history, key by key.",
)
def projection_matches_history(trino: Trino) -> AssetCheckResult:
    # Reuse the manual reconciliation report so both cannot drift.
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


@asset_check(
    asset=serving_orders,
    blocking=True,
    description="Every canonical silver key is present in serving at the same or a newer position.",
)
def serving_matches_silver(trino: Trino) -> AssetCheckResult:
    mismatches = int(
        trino.scalar(
            f"""SELECT count(*)
                  FROM iceberg.silver.orders_current b
                  LEFT JOIN iceberg.serving.orders_current s
                    ON s.order_id = b.order_id
                 WHERE {SERVING_CORRECTION_WHERE}"""
        )
        or 0
    )
    ahead = int(
        trino.scalar(
            """SELECT count(*)
                 FROM iceberg.serving.orders_current s
                 LEFT JOIN iceberg.silver.orders_current b
                   ON b.order_id = s.order_id
                WHERE b.order_id IS NULL"""
        )
        or 0
    )
    return AssetCheckResult(
        passed=mismatches == 0,
        metadata={"keys_needing_correction": mismatches, "keys_only_in_serving": ahead},
        description=f"{mismatches} silver keys are missing or stale in serving.",
    )


class ReplayConfig(Config):
    """Rebuild window. Omit both bounds to replay the whole history."""

    replay_id: str
    from_lsn: int | None = None
    to_lsn: int | None = None


@asset(
    group_name="replay",
    description="Rebuilds the projection from the event log into an isolated table.",
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

    # Only a full replay can be compared against the live projection.
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
