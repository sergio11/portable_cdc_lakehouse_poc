# Compatibility Matrix

Every version used by the POC is pinned and verified as a set. A change to an
entry requires re-running `rake verify` and the relevant integration or
portability checks.

Validation states:

- **Verified** — exercised in this repository with evidence below.
- **Pending** — resolved and pinned, but runtime validation remains outstanding.

## 1. JVM toolchain

The toolchain runs in containers invoked by Rake. No host JVM, sbt or Scala
installation is required.

| Component | Version | Validation |
|---|---|---|
| Container runtime | Podman 5.8.2 | Verified |
| Compose provider | podman-compose 1.5.0 | Verified |
| Build image | `sbtscala/scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18` | Verified |
| Build image digest | `sha256:c848bfcb0acb4086609c9c83ac3e64743465f11b3a18ca282f69d65c15b32170` | Verified |
| JDK | Eclipse Temurin 21.0.12+8 | Verified |
| sbt | 1.13.0 | Verified |
| Scala | 2.13.18 | Verified |
| Ruby | 3.4.5 | Verified |
| Rake | 13.4.2 | Verified |

Evidence: `rake verify` compiles the sources and runs the domain and architecture
suites in the pinned image.

## 2. Data plane libraries

| Component | Version | Validation |
|---|---|---|
| Apache Beam SDK | 2.76.0 | Verified |
| Beam DirectRunner | 2.76.0 | Verified (bounded Kafka-to-Iceberg run) |
| Beam FlinkRunner | 2.76.0 (`beam-runners-flink-2.2`) | Verified (bounded classic FlinkRunner embedded run) |
| Apache Flink | 2.2.x | Verified (embedded local runtime; cluster deployment pending) |
| KafkaIO | 2.76.0 | Verified (classpath resolves) |
| IcebergIO | 2.76.0 | Verified (classpath resolves) |
| Apache Iceberg | 1.11.0 | Verified (Beam IcebergIO write to MinIO) |
| Iceberg AWS module and bundle | 1.11.0 | Verified (S3FileIO runtime) |
| ScalaTest | 3.2.19 | Verified |

`beam-sdks-java-io-kafka` references Confluent registry artifacts outside Maven
Central, so `build.sbt` declares the Confluent resolver explicitly.

IcebergIO requires JDK 17 or newer; the pinned JDK 21 satisfies that floor.

## 3. Infrastructure services

Every service uses a pinned image tag or source release. No `latest` tag is used
by the reproducible configuration.

| Service | Image / source | Validation |
|---|---|---|
| PostgreSQL | `postgres:17.2-bookworm` | Verified (connector source and fixtures) |
| Kafka (KRaft) | `apache/kafka:3.9.1` | Verified (broker and CDC topic) |
| Debezium Connect | `debezium/connect:3.2.4.Final` | Verified (connector task RUNNING) |
| Trino | `trinodb/trino:476` | Verified (REST query and catalog startup) |
| MinIO | Official source `RELEASE.2025-10-15T17-29-55Z`, built with Go 1.24.8 | Verified (source build, S3 API, bucket and Iceberg table writes) |
| Iceberg REST catalog | `apache/iceberg-rest-fixture:1.10.1` | Verified (Trino create/list tables) |
| AWS CLI bucket bootstrap | `amazon/aws-cli:2.27.41` | Verified (bucket creation and probe) |

### MinIO source build

MinIO is required by the SRS. Upstream archived its repository on 25 April 2026
and stopped publishing community container images. The final upstream security
release, `RELEASE.2025-10-15T17-29-55Z`, is built locally with a pinned Go 1.24.8
builder. The OCI image is local to this POC; it is not an upstream-maintained
container image. The version and source-build procedure are recorded in
ADR-008.

The build, S3 bucket bootstrap, Trino Iceberg table writes, and Beam IcebergIO
append have been exercised with Podman. See `docs/poc-runbook.md` for the
reproducible bounded capture and materialization flow.

### Iceberg catalog compatibility

Trino 476 does not support HadoopCatalog as an Iceberg catalog type. The stack
therefore uses the Apache Iceberg REST fixture, backed by a persistent SQLite
catalog volume. Trino 476 successfully created and listed Iceberg tables through
the fixture. Beam Iceberg 1.11.0 successfully appended raw rows through the same
catalog and MinIO warehouse; Trino read the resulting snapshot. The warehouse is
`s3://pcl-warehouse/warehouse/` in MinIO. See ADR-009.

### Kafka addressing

Kafka advertises only `kafka:9092` and is not published to the host. The Beam
application joins the `pcl_default` Compose network and uses this same address.

## 4. Flink portability scope

`rake pipeline:flink` submits the same bounded graph to Beam's classic
FlinkRunner in embedded local mode. It is useful for POC portability checks and
does not require a separate Flink cluster. Cluster deployment, checkpoint
recovery and long-running job operations remain outside this local validation.

## 5. Known Flink dependency risk

The bounded embedded run did not encounter the `lz4-java` capability conflict
between Flink's `at.yawk.lz4:lz4-java` and Kafka's `org.lz4:lz4-java`. A remote
cluster run has not been exercised.

## 6. Environment notes

- sbt build outputs and Coursier caches use Podman named volumes to avoid slow
  or incompatible Windows/WSL bind-mounted timestamp updates.
- The Podman machine used during development has 4 CPUs and about 6 GiB RAM;
  absolute timings are not a performance measure for this POC.
- `.env` and the generated Trino catalog under `.local/` contain local secrets
  and remain untracked.
