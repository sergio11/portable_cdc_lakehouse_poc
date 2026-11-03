# frozen_string_literal: true

require "fileutils"
require "securerandom"

# Toolchain and container pins for Rake tasks.
module Config
  ROOT = File.expand_path("..", __dir__)

  SBT_IMAGE_DEFAULT = "docker.io/sbtscala/scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18"

  # Native volumes: the Windows bind mount rejects sbt's timestamp writes.
  BUILD_VOLUME = "pcl-build"

  SBT_CACHE_VOLUME = "pcl-sbt-cache"
  IVY_CACHE_DIR    = "/root/.ivy2"
  SBT_CACHE_DIR    = "/root/.sbt"
  COURSIER_DIR     = "/root/.cache/coursier"

  POSTGRES_IMAGE = "docker.io/library/postgres:17.2-bookworm"
  KAFKA_IMAGE    = "docker.io/apache/kafka:3.9.1"
  DEBEZIUM_IMAGE = "quay.io/debezium/connect:3.2.4.Final"
  TRINO_IMAGE    = "docker.io/trinodb/trino:476"
  MINIO_RELEASE  = "RELEASE.2025-10-15T17-29-55Z"
  MINIO_IMAGE    = "localhost/pcl-minio:#{MINIO_RELEASE}"
  GO_BUILDER_IMAGE = "docker.io/library/golang:1.24.8-bookworm"
  AWS_CLI_IMAGE  = "docker.io/amazon/aws-cli:2.27.41"
  POLARIS_VERSION = "1.8.0"
  POLARIS_IMAGE  = "docker.io/apache/polaris:#{POLARIS_VERSION}"

  # Iceberg 1.12 is the first release with a Flink 2.2 runtime.
  FLINK_DEFAULT_VERSION = "2.2.1"
  FLINK_DEFAULT_VARIANT = "java21"
  ICEBERG_FLINK_DEFAULT_VERSION = "1.12.0"

  MINIO_ENDPOINT = "http://minio:9000"
  TRINO_CATALOG_TEMPLATE = File.join(ROOT, "infra", "trino", "etc", "catalog", "iceberg.properties.template")
  TRINO_CATALOG_OUTPUT = File.join(ROOT, ".local", "trino", "catalog", "iceberg.properties")
  FLINK_SERVING_TEMPLATE = File.join(ROOT, "sql", "flink", "serving.sql")
  FLINK_SERVING_OUTPUT = File.join(ROOT, ".local", "flink", "serving.sql")
  PIPELINE_ENV_OUTPUT = File.join(ROOT, ".local", "pipeline.env")

  # One-shot Spark CLI; runtime must match Spark 3.5 / Scala 2.12.
  SPARK_DEFAULT_VERSION = "3.5.9"
  SPARK_DEFAULT_ICEBERG_VERSION = "1.11.0"
  SPARK_DEFAULT_DRIVER_MEMORY = "1g"
  SPARK_CONF_TEMPLATE = File.join(ROOT, "infra", "spark", "spark-defaults.conf.template")
  SPARK_CONF_OUTPUT = File.join(ROOT, ".local", "spark", "spark-defaults.conf")
  SPARK_WORK_DIR = File.join(ROOT, ".local", "spark", "work")

  ENV_FILE = File.join(ROOT, ".env")

  # Parse the local .env into a Hash. Credentials stay on disk, never in Git.
  def self.load_env!
    @env ||= begin
      unless File.exist?(ENV_FILE)
        abort <<~MSG
          Missing .env file.

          Local credentials are kept out of version control.
          Create it once with:

            cp .env.example .env

          then adjust the values if needed.
        MSG
      end

      File.readlines(ENV_FILE).filter_map do |line|
        stripped = line.strip
        next if stripped.empty? || stripped.start_with?("#")

        key, value = stripped.split("=", 2)
        [key.strip, value.to_s.strip]
      end.to_h
    end
  end

  # Read a configured value, failing with the key name rather than a nil error.
  def self.pg(key)
    value = load_env![key]
    abort("Missing required setting #{key} in .env") if value.nil? || value.empty?
    value
  end

  # Render local-only credentials into an ignored Trino catalog file.
  def self.render_trino_catalog!(env = load_env!)
    template = File.read(TRINO_CATALOG_TEMPLATE)
    replacements = {
      "PCL_MINIO_ROOT_USER" => env.fetch("PCL_MINIO_ROOT_USER"),
      "PCL_MINIO_ROOT_PASSWORD" => env.fetch("PCL_MINIO_ROOT_PASSWORD"),
      "PCL_MINIO_REGION" => env.fetch("PCL_MINIO_REGION"),
      "PCL_ICEBERG_BUCKET" => env.fetch("PCL_ICEBERG_BUCKET"),
      "PCL_ICEBERG_WAREHOUSE" => env.fetch("PCL_ICEBERG_WAREHOUSE", "pcl"),
      "PCL_POLARIS_CLIENT_ID" => env.fetch("PCL_POLARIS_CLIENT_ID", "root"),
      "PCL_POLARIS_CLIENT_SECRET" => env.fetch("PCL_POLARIS_CLIENT_SECRET")
    }
    replacements.each { |key, value| template = template.gsub("__#{key}__", value) }

    FileUtils.mkdir_p(File.dirname(TRINO_CATALOG_OUTPUT))
    File.open(TRINO_CATALOG_OUTPUT, "w", 0o600) { |file| file.write(template) }
  end

  # Output under .local/ because it carries the Polaris secret.
  def self.render_flink_serving!(env = load_env!)
    template = File.read(FLINK_SERVING_TEMPLATE)
    replacements = {
      "PCL_ICEBERG_WAREHOUSE" => env.fetch("PCL_ICEBERG_WAREHOUSE", "pcl"),
      "PCL_POLARIS_CLIENT_ID" => env.fetch("PCL_POLARIS_CLIENT_ID", "root"),
      "PCL_POLARIS_CLIENT_SECRET" => env.fetch("PCL_POLARIS_CLIENT_SECRET"),
      "PCL_MINIO_REGION" => env.fetch("PCL_MINIO_REGION"),
      "PCL_MINIO_ROOT_USER" => env.fetch("PCL_MINIO_ROOT_USER"),
      "PCL_MINIO_ROOT_PASSWORD" => env.fetch("PCL_MINIO_ROOT_PASSWORD")
    }
    replacements.each { |key, value| template = template.gsub("__#{key}__", value) }

    FileUtils.mkdir_p(File.dirname(FLINK_SERVING_OUTPUT))
    File.open(FLINK_SERVING_OUTPUT, "w", 0o600) { |file| file.write(template) }
    FLINK_SERVING_OUTPUT
  end

  # One-run env file: credentials never reach argv or a tracked file.
  def self.render_pipeline_env!(env = load_env!)
    settings = {
      "PCL_RUN_ID" => env.fetch("PCL_RUN_ID", SecureRandom.uuid),
      "PCL_SOURCE_ID" => env.fetch("PCL_SOURCE_ID"),
      "PCL_KAFKA_BOOTSTRAP_SERVERS" => env.fetch("PCL_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
      "PCL_CDC_TOPIC" => env.fetch("PCL_CDC_TOPIC", "pcl.public.orders"),
      "PCL_ICEBERG_CATALOG_URI" => env.fetch("PCL_ICEBERG_CATALOG_URI", "http://polaris:8181/api/catalog"),
      "PCL_ICEBERG_WAREHOUSE" => env.fetch("PCL_ICEBERG_WAREHOUSE", "pcl"),
      "PCL_POLARIS_CLIENT_ID" => env.fetch("PCL_POLARIS_CLIENT_ID", "root"),
      "PCL_POLARIS_CLIENT_SECRET" => env.fetch("PCL_POLARIS_CLIENT_SECRET"),
      "PCL_MINIO_ENDPOINT" => env.fetch("PCL_MINIO_ENDPOINT", "http://minio:9000"),
      "PCL_MINIO_REGION" => env.fetch("PCL_MINIO_REGION"),
      "PCL_MINIO_ROOT_USER" => env.fetch("PCL_MINIO_ROOT_USER"),
      "PCL_MINIO_ROOT_PASSWORD" => env.fetch("PCL_MINIO_ROOT_PASSWORD"),
      "PCL_PRIMARY_KEY_COLUMNS" => env.fetch("PCL_PRIMARY_KEY_COLUMNS", "order_id"),
      "PCL_WRITE_FREQUENCY_SECONDS" => env.fetch("PCL_WRITE_FREQUENCY_SECONDS", "10"),
      "PCL_MAX_RECORDS" => env.fetch("PCL_MAX_RECORDS", "0"),
      # Layer picks the consumer group; the bounded run leaves offsets alone.
      "PCL_PIPELINE_LAYER" => env.fetch("PCL_PIPELINE_LAYER", "batch"),
      "PCL_CONSUMER_GROUP" => env.fetch("PCL_CONSUMER_GROUP", ""),
      "PCL_TARGET_TABLE" => env.fetch("PCL_TARGET_TABLE", "raw.order_cdc_events"),
      "PCL_COMMIT_OFFSETS" => env.fetch("PCL_COMMIT_OFFSETS", "false")
    }

    FileUtils.mkdir_p(File.dirname(PIPELINE_ENV_OUTPUT))
    File.open(PIPELINE_ENV_OUTPUT, "w", 0o600) do |file|
      settings.each { |key, value| file.puts("#{key}=#{value}") }
    end
    PIPELINE_ENV_OUTPUT
  end

  # Image tag built by Compose (`build: ./infra/spark`) and run by Rake. Keeping
  # the tag in one method prevents "built one image, ran another" drift.
  def self.spark_image(env = load_env!)
    version = env.fetch("PCL_SPARK_VERSION", SPARK_DEFAULT_VERSION)
    iceberg = env.fetch("PCL_SPARK_ICEBERG_VERSION", SPARK_DEFAULT_ICEBERG_VERSION)
    "localhost/pcl-spark:#{version}-iceberg-#{iceberg}"
  end

  # Image tag built by Compose from infra/flink and used by both Flink services.
  def self.flink_image(env = load_env!)
    version = env.fetch("PCL_FLINK_VERSION", FLINK_DEFAULT_VERSION)
    variant = env.fetch("PCL_FLINK_VARIANT", FLINK_DEFAULT_VARIANT)
    iceberg = env.fetch("PCL_ICEBERG_FLINK_VERSION", ICEBERG_FLINK_DEFAULT_VERSION)
    "localhost/pcl-flink:#{version}-#{variant}-iceberg-#{iceberg}"
  end

  # JVM used to run the assembled pipeline jar. Defaults without requiring .env,
  # because `rake verify` and `rake compile` must work on a fresh clone.
  def self.sbt_image(env = nil)
    env ||= File.exist?(ENV_FILE) ? load_env! : {}
    env.fetch("PCL_SBT_IMAGE", SBT_IMAGE_DEFAULT)
  end

  # Render the Iceberg catalog and S3 settings for the Spark session. The output
  # holds credentials and lives under the gitignored .local/ directory.
  def self.render_spark_conf!(env = load_env!)
    template = File.read(SPARK_CONF_TEMPLATE)
    replacements = {
      "PCL_ICEBERG_WAREHOUSE" => env.fetch("PCL_ICEBERG_WAREHOUSE", "pcl"),
      "PCL_POLARIS_CLIENT_ID" => env.fetch("PCL_POLARIS_CLIENT_ID", "root"),
      "PCL_POLARIS_CLIENT_SECRET" => env.fetch("PCL_POLARIS_CLIENT_SECRET"),
      "PCL_MINIO_REGION" => env.fetch("PCL_MINIO_REGION"),
      "PCL_MINIO_ROOT_USER" => env.fetch("PCL_MINIO_ROOT_USER"),
      "PCL_MINIO_ROOT_PASSWORD" => env.fetch("PCL_MINIO_ROOT_PASSWORD"),
      "PCL_SPARK_DRIVER_MEMORY" => env.fetch("PCL_SPARK_DRIVER_MEMORY", SPARK_DEFAULT_DRIVER_MEMORY)
    }
    replacements.each { |key, value| template = template.gsub("__#{key}__", value) }

    FileUtils.mkdir_p(SPARK_WORK_DIR)
    File.open(SPARK_CONF_OUTPUT, "w", 0o600) { |file| file.write(template) }
    SPARK_CONF_OUTPUT
  end
end

# Runs sbt inside the pinned container with the repo bind-mounted. The host needs
# only Podman — no local JVM, no local sbt, no Ruby gems beyond Rake itself.
module Build
  include Config

  # Suites that make up the fast local gate: domain rules and the architectural
  # boundary. Integration suites are deliberately excluded — they need services.
  UNIT_TEST_PATTERNS = %w[
    com.example.pcl.architecture.*
    com.example.pcl.domain.*
    com.example.pcl.adapters.*
  ].freeze

  # sbt commands are passed as separate argv entries (never interpolated into a
  # shell string), so paths with spaces and metacharacters stay safe.

  # Batch mode treats every argv entry as a distinct command, so a command with
  # its own arguments must travel as ONE entry. Joining happens here rather than
  # in the caller so no task can get the split wrong.
  def self.sbt_test_only(*patterns)
    "testOnly #{patterns.join(' ')}"
  end
  def self.container_sbt(*sbt_args)
    argv = [
      "podman", "run", "--rm",
      "-v", "#{Config::ROOT}:/workspace:z",
      # Shadow the build-output dirs with native volumes (see Config::BUILD_VOLUME).
      "-v", "#{Config::BUILD_VOLUME}-target:/workspace/target",
      "-v", "#{Config::BUILD_VOLUME}-project:/workspace/project/target",
      # Persist dependency caches across runs. Coursier must be a real volume too:
      # it is the primary artifact cache for sbt 1.13, and without it every run
      # re-verifies (and re-downloads) the whole Beam/Flink/Iceberg classpath.
      "-v", "#{Config::SBT_CACHE_VOLUME}:#{Config::SBT_CACHE_DIR}",
      "-v", "#{Config::SBT_CACHE_VOLUME}-ivy:#{Config::IVY_CACHE_DIR}",
      "-v", "#{Config::SBT_CACHE_VOLUME}-coursier:#{Config::COURSIER_DIR}",
      "-w", "/workspace",
      "-e", "COURSIER_CACHE=#{Config::COURSIER_DIR}",
      # Give the JVM enough heap for the large Beam/Flink/Iceberg classpath.
      "-e", "JAVA_OPTS=-Xmx3g -Xms1g",
      "--entrypoint", "sbt",
      Config.sbt_image,
      *sbt_args
    ]
    run!(*argv)
  end
end
