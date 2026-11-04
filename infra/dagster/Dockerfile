# Dagster batch image: Python runtime plus the docker CLI used to start sibling
# Spark containers through the host Podman socket.
FROM docker.io/docker:27-cli AS cli

FROM docker.io/library/python:3.12-slim

COPY --from=cli /usr/local/bin/docker /usr/local/bin/docker

ENV DAGSTER_HOME=/opt/dagster/home \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/dagster/app

RUN mkdir -p /opt/dagster/home

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY workspace.yaml .
COPY pcl_dagster ./pcl_dagster

EXPOSE 3000
