"""Dagster entrypoint: the batch layer of a lambda architecture.

The schedule materialises the batch graph every five minutes. Each run is
incremental: the transformation step projects the events that arrived since the
watermark into canonical `silver`, and the serving step overwrites the
low-latency serving table with the canonical version. The continuous speed and
serving writers run outside this graph on Flink.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from .assets import (
    debezium_connector,
    lakehouse_tables,
    orders_current,
    orders_current_replay,
    projection_matches_history,
    serving_matches_silver,
    serving_orders,
)
from .config import env
from .resources import Podman, Trino

incremental_job = define_asset_job(
    name="pcl_incremental",
    selection=AssetSelection.assets(orders_current, serving_orders).upstream()
    | AssetSelection.checks(
        projection_matches_history,
        serving_matches_silver,
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
        orders_current,
        serving_orders,
        orders_current_replay,
    ],
    asset_checks=[
        projection_matches_history,
        serving_matches_silver,
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
    },
)
