"""Dagster entrypoint: the batch layer of a lambda architecture.

The schedule materialises the batch graph every five minutes. Each run is
incremental: the ingestion step reads what is pending in Kafka with the batch
consumer group, merges those deliveries into canonical raw by event_id, the
transformation step only merges what arrived since the watermark, and the
serving step compacts speed deliveries that batch now owns and refreshes the
query-time merge. The continuous speed layer runs outside this graph.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from .assets import (
    batch_fully_accounted,
    debezium_connector,
    lakehouse_tables,
    orders_current,
    orders_current_replay,
    projection_matches_history,
    raw_cdc_events,
    raw_events_unique,
    serving_orders,
    serving_prefers_batch,
    speed_compaction,
    speed_fully_compacted,
)
from .config import env
from .resources import Kafka, Podman, Trino

incremental_job = define_asset_job(
    name="pcl_incremental",
    selection=AssetSelection.assets(
        raw_cdc_events, orders_current, speed_compaction, serving_orders
    ).upstream()
    | AssetSelection.checks(
        batch_fully_accounted,
        raw_events_unique,
        projection_matches_history,
        speed_fully_compacted,
        serving_prefers_batch,
    ),
)

replay_job = define_asset_job(
    name="pcl_replay",
    selection=AssetSelection.assets(orders_current_replay),
)

defs = Definitions(
    assets=[
        debezium_connector,
        lakehouse_tables,
        raw_cdc_events,
        orders_current,
        speed_compaction,
        serving_orders,
        orders_current_replay,
    ],
    asset_checks=[
        batch_fully_accounted,
        raw_events_unique,
        projection_matches_history,
        speed_fully_compacted,
        serving_prefers_batch,
    ],
    jobs=[incremental_job, replay_job],
    schedules=[
        ScheduleDefinition(
            name="pcl_incremental_every_five_minutes",
            job=incremental_job,
            cron_schedule="*/5 * * * *",
            execution_timezone="UTC",
            # The stack is meant to run unattended: a stopped schedule would look
            # healthy in the UI and never fire.
            default_status=DefaultScheduleStatus.RUNNING,
        )
    ],
    resources={
        "podman": Podman(
            # This is the path *inside* the Dagster container: Compose mounts the
            # host socket there. PCL_PODMAN_SOCKET is the host-side path used by
            # the bind mount, not something the container can connect to.
            socket=env("PCL_DAGSTER_PODMAN_SOCKET", "/run/podman/podman.sock"),
            host_workspace=env("PCL_HOST_WORKSPACE", ""),
            network=env("PCL_DOCKER_NETWORK", "pcl_default"),
        ),
        "trino": Trino(url=env("PCL_TRINO_URL", "http://trino:8080")),
        "kafka": Kafka(
            bootstrap=env("PCL_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic=env("PCL_CDC_TOPIC", "pcl.public.orders"),
            group_id=f"pcl-batch-{env('PCL_SOURCE_ID', 'pg-local-1')}",
        ),
    },
)
