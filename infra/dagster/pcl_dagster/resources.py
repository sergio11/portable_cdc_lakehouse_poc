"""Clients for the services the orchestrator drives.

Each resource wraps one external system and does what the equivalent Rake task
does. Nothing here writes Iceberg data: DDL, MERGE and replay belong to Spark.
Trino reads the lakehouse and owns exactly one object, the serving view
definition, because Iceberg views are dialect-specific.
"""

import subprocess

import requests
from confluent_kafka import (
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from dagster import ConfigurableResource


class Podman(ConfigurableResource):
    """Starts one-shot sibling containers through the host engine socket."""

    socket: str
    host_workspace: str
    network: str = "pcl_default"
    probe_image: str = "docker.io/library/alpine:3.21"

    def _argv(self, *args: str) -> list[str]:
        return ["docker", "-H", f"unix://{self.socket}", *args]

    def host_path(self, relative: str) -> str:
        """Translate a workspace-relative path into the path the daemon sees."""
        if not self.host_workspace:
            raise RuntimeError(
                "PCL_HOST_WORKSPACE is not set. Point it at the absolute host path of "
                "this repository: bind mounts are resolved by the Podman engine, not "
                "by the Dagster container."
            )
        return f"{self.host_workspace.rstrip('/')}/{relative}"

    def run(self, *args: str, timeout: int = 1800) -> str:
        argv = self._argv(*args)
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        if completed.returncode != 0:
            raise RuntimeError(
                f"podman {args[0]} failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        return completed.stdout

    def image_exists(self, tag: str) -> bool:
        argv = self._argv("image", "inspect", tag)
        return subprocess.run(argv, capture_output=True).returncode == 0

    def volume_file_exists(self, volume: str, path: str) -> bool:
        """Check a path inside a named volume without mounting it locally."""
        argv = self._argv(
            "run",
            "--rm",
            "-v",
            f"{volume}:/probe:ro",
            self.probe_image,
            "test",
            "-f",
            f"/probe{path}",
        )
        result = subprocess.run(argv, capture_output=True)
        # 1 means "no such file"; 125 means the probe container never started.
        if result.returncode == 125:
            raise RuntimeError(
                f"Cannot inspect volume {volume}: {result.stderr.decode(errors='replace')[:300]}"
            )
        return result.returncode == 0


class Trino(ConfigurableResource):
    """SQL client for the serving layer.

    Normal queries run as `pcl` and are read-only. The serving-view DDL runs as
    `pcl-provisioner`, the only identity the file-based access control allows to
    own `serving.orders_current`; Iceberg stores one SQL dialect per view, so
    Trino must be the engine that writes it.
    """

    url: str = "http://trino:8080"
    user: str = "pcl"
    provisioner_user: str = "pcl-provisioner"
    source: str = "pcl-dagster"
    catalog: str = "iceberg"

    def query(self, sql: str) -> list[list[str]]:
        return self._execute(sql, self.user)

    def provision(self, sql: str) -> list[list[str]]:
        return self._execute(sql, self.provisioner_user)

    def _execute(self, sql: str, user: str) -> list[list[str]]:
        headers = {
            "X-Trino-User": user,
            "X-Trino-Source": self.source,
            "X-Trino-Catalog": self.catalog,
            "Content-Type": "text/plain; charset=utf-8",
        }
        response = requests.post(
            f"{self.url}/v1/statement", data=sql.encode("utf-8"), headers=headers, timeout=120
        )
        rows: list[list[str]] = []
        while True:
            if response.status_code != 200:
                raise RuntimeError(f"Trino HTTP {response.status_code}: {response.text[:400]}")

            payload = response.json()
            # A failed query still answers 200 with an error payload, so the
            # status code alone would report a clean bill of health on bad SQL.
            if payload.get("error"):
                error = payload["error"]
                raise RuntimeError(
                    f"Trino query failed ({error.get('errorCode')}): {error.get('message')}"
                )

            rows.extend(payload.get("data") or [])
            next_uri = payload.get("nextUri")
            if not next_uri:
                return rows
            response = requests.get(next_uri, headers=headers, timeout=120)

    def scalar(self, sql: str) -> str | None:
        rows = self.query(sql)
        return rows[0][0] if rows else None


class Kafka(ConfigurableResource):
    """Reads the topic watermark and owns the Beam consumer-group offsets.

    Beam starts from the group's committed offsets, so advancing them after a
    successful append is what makes a scheduled run incremental instead of a
    full re-read of the topic.
    """

    bootstrap: str
    topic: str
    group_id: str

    def _admin(self) -> AdminClient:
        return AdminClient({"bootstrap.servers": self.bootstrap})

    def partitions(self) -> list[int]:
        metadata = self._admin().list_topics(self.topic, timeout=15)
        topic = metadata.topics.get(self.topic)
        # Debezium creates the topic with the first change event, so an empty
        # result is a valid "nothing to read yet", not an error.
        if topic is None or topic.error is not None:
            return []
        return sorted(topic.partitions.keys())

    def end_offsets(self) -> dict[int, int]:
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "enable.auto.commit": False,
            }
        )
        try:
            return {
                partition: consumer.get_watermark_offsets(
                    TopicPartition(self.topic, partition), timeout=15
                )[1]
                for partition in self.partitions()
            }
        finally:
            consumer.close()

    def committed_offsets(self) -> dict[int, int]:
        # Keep the admin client alive until the future resolves: a temporary
        # client can be collected before the response arrives and the future
        # would then never complete.
        admin = self._admin()
        try:
            futures = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(self.group_id)], request_timeout=15
            )
            result = futures[self.group_id].result(timeout=30)
        except KafkaException as error:
            cause = error.args[0] if error.args else None
            if getattr(cause, "code", lambda: None)() == KafkaError.GROUP_ID_NOT_FOUND:
                return {}
            raise

        # An offset of -1 means "no committed position for this partition".
        return {
            partition.partition: partition.offset
            for partition in result.topic_partitions
            if partition.offset >= 0
        }

    def lag(self) -> tuple[int, dict[int, int]]:
        """Pending records and the end offsets they were measured against."""
        end = self.end_offsets()
        committed = self.committed_offsets()
        # No committed offset means `auto.offset.reset=earliest`: the whole topic
        # is pending, which is the intended first run.
        pending = sum(max(0, end[partition] - committed.get(partition, 0)) for partition in end)
        return pending, end

    def commit(self, offsets: dict[int, int]) -> None:
        """Store the offsets as the group's committed position.

        A consumer that assigns partitions and commits explicit offsets also
        creates the group when it does not exist yet, which the admin API does
        not do. That keeps the first scheduled run working on a fresh topic.
        """
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.assign(
                [
                    TopicPartition(self.topic, partition, offset)
                    for partition, offset in offsets.items()
                ]
            )
            consumer.commit(
                offsets=[
                    TopicPartition(self.topic, partition, offset)
                    for partition, offset in offsets.items()
                ],
                asynchronous=False,
            )
        finally:
            consumer.close()
