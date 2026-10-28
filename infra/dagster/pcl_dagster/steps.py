"""The external steps, each mirroring a Rake task one-to-one.

Dagster does not reimplement the lakehouse logic: it renders the same SQL, runs
the same pinned images, and passes the same environment. `rake` stays the manual
and test path, this module is the scheduled one.
"""

import json
import re
import uuid
from pathlib import Path

import requests

from .config import WORKSPACE, env
from .resources import Podman

SPARK_CONF = ".local/spark/spark-defaults.conf"
SPARK_WORK_DIR = ".local/spark/work"
PIPELINE_ENV = ".local/dagster/pipeline.env"
BUILD_VOLUME = "pcl-build-target"
PIPELINE_JAR = "/scala-2.13/pcl-pipeline.jar"

# Placeholders in spark-defaults.conf.template and the fallback Rake uses when
# .env does not set them.
SPARK_CONF_KEYS = {
    "PCL_ICEBERG_WAREHOUSE": "pcl",
    "PCL_POLARIS_CLIENT_ID": "root",
    "PCL_POLARIS_CLIENT_SECRET": "",
    "PCL_MINIO_REGION": "",
    "PCL_MINIO_ROOT_USER": "",
    "PCL_MINIO_ROOT_PASSWORD": "",
    "PCL_SPARK_DRIVER_MEMORY": "1g",
}


def spark_image() -> str:
    """Same tag Compose builds and Rake runs: one image, no drift."""
    version = env("PCL_SPARK_VERSION", "3.5.9")
    iceberg = env("PCL_SPARK_ICEBERG_VERSION", "1.11.0")
    return f"localhost/pcl-spark:{version}-iceberg-{iceberg}"


def render_spark_conf() -> Path:
    """Render catalog and S3 settings; the output holds credentials and is ignored."""
    template = (WORKSPACE / "infra/spark/spark-defaults.conf.template").read_text(encoding="utf-8")
    for key, default in SPARK_CONF_KEYS.items():
        template = template.replace(f"__{key}__", env(key, default))

    target = WORKSPACE / SPARK_CONF
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(template, encoding="utf-8")
    target.chmod(0o600)
    return target


def expand_sql(name: str, *, target: str, raw_filter: str | None = None) -> str:
    """Splice the shared ranking and MERGE fragments into a top-level script."""
    directory = WORKSPACE / "sql" / "spark"

    def read(relative: str) -> str:
        return (directory / relative).read_text(encoding="utf-8")

    sql = read(name).replace("-- __PROJECTION_SOURCE__", read("projection_source.sql"))
    sql = sql.replace("-- __MERGE__", read("merge_orders.sql"))
    sql = sql.replace("__TARGET_TABLE__", target)
    if raw_filter is not None:
        sql = sql.replace("__RAW_FILTER__", raw_filter)
    return sql


def run_spark(podman: Podman, sql: str, name: str) -> str:
    """Run a SQL script on the one-shot Spark image, exactly as `lakehouse:*` does."""
    render_spark_conf()

    work = WORKSPACE / SPARK_WORK_DIR
    work.mkdir(parents=True, exist_ok=True)
    (work / f"{name}.sql").write_text(sql, encoding="utf-8")

    return podman.run(
        "run",
        "--rm",
        "--network",
        podman.network,
        "-v",
        f"{podman.host_path('sql')}:/opt/pcl/sql:ro",
        "-v",
        f"{podman.host_path('.local/spark')}:/opt/pcl/conf:ro",
        "-w",
        "/tmp",
        spark_image(),
        "/opt/spark/bin/spark-sql",
        "--properties-file",
        "/opt/pcl/conf/spark-defaults.conf",
        "--driver-memory",
        env("PCL_SPARK_DRIVER_MEMORY", "1g"),
        "-f",
        f"/opt/pcl/conf/work/{name}.sql",
    )


def spark_metrics(output: str) -> dict[str, int]:
    """Pick up the `metric<TAB>value` rows the SQL scripts select."""
    metrics: dict[str, int] = {}
    pattern = re.compile(
        r"^(changed_keys|projected_keys|landing_rows|landing_events|"
        r"landing_duplicates|raw_rows|raw_event_ids|landing_remaining)\s+(\d+)$"
    )
    for line in output.splitlines():
        match = pattern.match(line.strip())
        if match:
            metrics[match.group(1)] = int(match.group(2))
    return metrics


def ensure_pipeline_jar(podman: Podman) -> None:
    """The jar is a build artifact, not an operational step: fail with the fix."""
    if not podman.volume_file_exists(BUILD_VOLUME, PIPELINE_JAR):
        raise RuntimeError(
            "pcl-pipeline.jar is missing from the pcl-build-target volume. "
            "Build it once with `rake assembly`."
        )


def run_beam(podman: Podman, max_records: int) -> dict[str, int]:
    """Read at most `max_records` from Kafka and append them to the landing table.

    The bounded read stops exactly at the offset snapshot taken before the run,
    which is what lets the caller advance the consumer group afterwards.
    """
    settings = {
        "PCL_RUN_ID": str(uuid.uuid4()),
        "PCL_SOURCE_ID": env("PCL_SOURCE_ID", "pg-local-1"),
        "PCL_KAFKA_BOOTSTRAP_SERVERS": env("PCL_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "PCL_CDC_TOPIC": env("PCL_CDC_TOPIC", "pcl.public.orders"),
        "PCL_ICEBERG_CATALOG_URI": env(
            "PCL_ICEBERG_CATALOG_URI", "http://polaris:8181/api/catalog"
        ),
        "PCL_ICEBERG_WAREHOUSE": env("PCL_ICEBERG_WAREHOUSE", "pcl"),
        "PCL_POLARIS_CLIENT_ID": env("PCL_POLARIS_CLIENT_ID", "root"),
        "PCL_POLARIS_CLIENT_SECRET": env("PCL_POLARIS_CLIENT_SECRET"),
        "PCL_MINIO_ENDPOINT": env("PCL_MINIO_ENDPOINT", "http://minio:9000"),
        "PCL_MINIO_REGION": env("PCL_MINIO_REGION"),
        "PCL_MINIO_ROOT_USER": env("PCL_MINIO_ROOT_USER"),
        "PCL_MINIO_ROOT_PASSWORD": env("PCL_MINIO_ROOT_PASSWORD"),
        "PCL_PRIMARY_KEY_COLUMNS": env("PCL_PRIMARY_KEY_COLUMNS", "order_id"),
        "PCL_WRITE_FREQUENCY_SECONDS": env("PCL_WRITE_FREQUENCY_SECONDS", "10"),
        "PCL_MAX_RECORDS": str(max_records),
    }

    # Credentials travel in a file, never on the command line.
    env_file = WORKSPACE / PIPELINE_ENV
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text(
        "".join(f"{key}={value}\n" for key, value in settings.items()), encoding="utf-8"
    )
    env_file.chmod(0o600)

    output = podman.run(
        "run",
        "--rm",
        "--network",
        podman.network,
        "--env-file",
        str(env_file),
        "-v",
        f"{BUILD_VOLUME}:/workspace/target:ro",
        "-w",
        "/workspace",
        "--entrypoint",
        "java",
        env("PCL_SBT_IMAGE"),
        "-jar",
        f"/workspace/target/scala-2.13/pcl-pipeline.jar",
    )
    return beam_metrics(output)


def beam_metrics(output: str) -> dict[str, int]:
    """The pipeline prints one JSON object per metric when it finishes."""
    metrics: dict[str, int] = {}
    for line in output.splitlines():
        if not line.startswith('{"component":"CdcStreamingPipeline"'):
            continue
        payload = json.loads(line)
        if "metric" in payload:
            metrics[payload["metric"]] = int(payload["value"])
    return metrics


def connector_status() -> dict[str, str]:
    """Ensure the source connector exists and is running, and report its state.

    Registering an existing connector restarts its tasks, so the PUT only happens
    when the connector is missing or not healthy.
    """
    url = env("PCL_CONNECT_URL", "http://connect:8083")
    template = (WORKSPACE / "infra/debezium/postgres-orders-connector.json").read_text(
        encoding="utf-8"
    )
    template = template.replace("__PCL_POSTGRES_DB__", env("PCL_POSTGRES_DB"))
    template = template.replace("__PCL_POSTGRES_PASSWORD__", env("PCL_POSTGRES_PASSWORD"))
    payload = json.loads(template)
    name = payload["name"]

    status = requests.get(f"{url}/connectors/{name}/status", timeout=30)
    if status.status_code == 404:
        registered = requests.put(
            f"{url}/connectors/{name}/config", json=payload["config"], timeout=60
        )
        registered.raise_for_status()
        status = requests.get(f"{url}/connectors/{name}/status", timeout=30)

    status.raise_for_status()
    state = status.json()["connector"]["state"]
    if state != "RUNNING":
        requests.put(f"{url}/connectors/{name}/config", json=payload["config"], timeout=60).raise_for_status()
        status = requests.get(f"{url}/connectors/{name}/status", timeout=30)
        status.raise_for_status()
        state = status.json()["connector"]["state"]

    return {"connector": name, "state": state}
