# frozen_string_literal: true

require "fileutils"

# Single source of truth for toolchain / container pins used by Rake tasks.
# Mirror any change here into build.sbt and docs/compatibility-matrix.md.
module Config
  ROOT = File.expand_path("..", __dir__)

  # sbt + Scala + JDK, fully pinned. Tag format: eclipse-temurin-<jdk>_<sbt>_<scala>
  SBT_IMAGE = "docker.io/sbtscala/scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18"

  # Named volumes keep sbt's build output on the container's native filesystem.
  # The Windows/WSL bind mount rejects the file-timestamp writes sbt performs in
  # its incremental caches, and native volumes are also markedly faster.
  BUILD_VOLUME = "pcl-build"

  # Local cache volumes so dependency resolution survives across runs without
  # writing anything into the host working tree beyond target/.
  SBT_CACHE_VOLUME = "pcl-sbt-cache"
  IVY_CACHE_DIR    = "/root/.ivy2"
  SBT_CACHE_DIR    = "/root/.sbt"
  COURSIER_DIR     = "/root/.cache/coursier"

  # --- Service images, pinned to exact tags (never `latest`) -----------------
  # Verified against their registries; see docs/compatibility-matrix.md.
  POSTGRES_IMAGE = "docker.io/library/postgres:17.2-bookworm"
  KAFKA_IMAGE    = "docker.io/apache/kafka:3.9.1"
  DEBEZIUM_IMAGE = "quay.io/debezium/connect:3.2.4.Final"
  TRINO_IMAGE    = "docker.io/trinodb/trino:476"
  MINIO_RELEASE  = "RELEASE.2025-10-15T17-29-55Z"
  MINIO_IMAGE    = "localhost/pcl-minio:#{MINIO_RELEASE}"
  GO_BUILDER_IMAGE = "docker.io/library/golang:1.24.8-bookworm"
  AWS_CLI_IMAGE  = "docker.io/amazon/aws-cli:2.27.41"
  ICEBERG_REST_IMAGE = "docker.io/apache/iceberg-rest-fixture:1.10.1"

  MINIO_ENDPOINT = "http://minio:9000"
  TRINO_CATALOG_TEMPLATE = File.join(ROOT, "infra", "trino", "etc", "catalog", "iceberg.properties.template")
  TRINO_CATALOG_OUTPUT = File.join(ROOT, ".local", "trino", "catalog", "iceberg.properties")

  ENV_FILE = File.join(ROOT, ".env")

  # Parse the local .env into a Hash. Credentials stay on disk, never in Git.
  def self.load_env!
    @env ||= begin
      unless File.exist?(ENV_FILE)
        abort <<~MSG
          Missing .env file.

          Local credentials are kept out of version control (SRS NFR-005).
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
      "PCL_ICEBERG_BUCKET" => env.fetch("PCL_ICEBERG_BUCKET")
    }
    replacements.each { |key, value| template = template.gsub("__#{key}__", value) }

    FileUtils.mkdir_p(File.dirname(TRINO_CATALOG_OUTPUT))
    File.open(TRINO_CATALOG_OUTPUT, "w", 0o600) { |file| file.write(template) }
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
      Config::SBT_IMAGE,
      *sbt_args
    ]
    run!(*argv)
  end
end
