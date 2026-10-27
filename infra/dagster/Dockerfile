# Dagster is the scheduled entrypoint for the lakehouse. The image stays small:
# it needs a Python runtime plus the docker CLI, because Spark and Beam run as
# sibling containers started through the host Podman socket. No JVM here.
FROM docker.io/docker:27-cli AS cli

FROM docker.io/library/python:3.12-slim

# The CLI is a static Go binary, so it drops into a Debian base unchanged.
COPY --from=cli /usr/local/bin/docker /usr/local/bin/docker

ENV DAGSTER_HOME=/opt/dagster/home \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/dagster/app

# DAGSTER_HOME must exist. Compose mounts the Postgres-backed dagster.yaml here;
# without that mount Dagster falls back to a local instance, which is what the
# `definitions validate` check uses.
RUN mkdir -p /opt/dagster/home

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY workspace.yaml .
COPY pcl_dagster ./pcl_dagster

EXPOSE 3000
