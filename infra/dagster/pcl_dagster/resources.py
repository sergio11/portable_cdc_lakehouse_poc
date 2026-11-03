"""Clients for the services the orchestrator drives.

Each resource wraps one external system and does what the equivalent Rake task
does. Nothing here writes Iceberg data: DDL, MERGE and replay belong to Spark,
and the continuous serving upsert belongs to Flink. Trino is a read-only SQL
endpoint.
"""

import subprocess

import requests
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
    """Read-only SQL client for the serving layer.

    The file-based access control in infra/trino is the real boundary; this
    resource has no way to write because the whole architecture gives Trino no
    ownership of any table, view, or schema.
    """

    url: str = "http://trino:8080"
    user: str = "pcl"
    source: str = "pcl-dagster"
    catalog: str = "iceberg"

    def query(self, sql: str) -> list[list[str]]:
        headers = {
            "X-Trino-User": self.user,
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
