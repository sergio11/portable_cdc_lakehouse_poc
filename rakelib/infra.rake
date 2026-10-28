# frozen_string_literal: true

require "json"
require "net/http"
require "open3"
require "uri"

# Service orchestration. Every external command is invoked with an argv array via
# Open3/System, never a shell string, and readiness is decided by real probes
# (HTTP endpoints, SQL round-trips, broker API) rather than fixed sleeps.
module Infra
  READY_TIMEOUT = 240
  POLL_INTERVAL = 2

  module_function

  # --- Compose plumbing ------------------------------------------------------

  def compose(*args)
    # -p keeps the project (and therefore the network name) stable so the Beam
    # application can attach to `pcl_default` without guessing.
    run!(
      "podman", "compose", "-p", "pcl",
      "-f", File.join(Config::ROOT, "compose.yaml"), *args
    )
  end

  # Run a one-shot client container on the Compose network, returning stdout.
  # Host tooling requirements stay at zero: no psql or kafka-topics.sh needed.
  def capture(image, *args, environment: {})
    env_args = environment.keys.flat_map { |name| ["-e", name] }
    stdout, stderr, status = Open3.capture3(
      environment, "podman", "run", "--rm", "--network", "pcl_default",
      *env_args, image, *args
    )
    raise "podman run failed: #{stderr.strip}" unless status.success?
    stdout
  end

  def postgres_ready?
    capture(
      Config::POSTGRES_IMAGE,
      "psql", "-h", "postgres", "-U", Config.pg("PCL_POSTGRES_USER"),
      "-d", Config.pg("PCL_POSTGRES_DB"), "-tAc", "SELECT 1",
      environment: { "PGPASSWORD" => Config.pg("PCL_POSTGRES_PASSWORD") }
    ).strip == "1"
  end

  def kafka_ready?
    out = capture(
      Config::KAFKA_IMAGE,
      "/opt/kafka/bin/kafka-broker-api-versions.sh",
      "--bootstrap-server", "kafka:9092"
    )
    !out.strip.empty?
  end

  # Apply a local .sql file to Postgres using the pinned image as the client.
  # The file is mounted read-only; the SQL travels as a file, not via stdin, so
  # nothing depends on shell quoting.
  def psql_file(sql_path, env = Config.load_env!)
    raise "SQL file not found: #{sql_path}" unless File.exist?(sql_path)

    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "-e", "PGPASSWORD",
      "-v", "#{sql_path}:/sql/input.sql:ro",
      Config::POSTGRES_IMAGE,
      "psql", "-v", "ON_ERROR_STOP=1",
      "-h", "postgres",
      "-U", env.fetch("PCL_POSTGRES_USER"),
      "-d", env.fetch("PCL_POSTGRES_DB"),
      "-f", "/sql/input.sql",
      environment: { "PGPASSWORD" => env.fetch("PCL_POSTGRES_PASSWORD") }
    )
  end

  # --- Readiness -------------------------------------------------------------

  # Blocks until the block returns truthy, then returns. On timeout, abort with
  # the last observed error so the message is diagnosable rather than generic.
  def wait_until!(description)
    deadline = Time.now + READY_TIMEOUT
    last_error = "no error recorded"

    while Time.now < deadline
      begin
        return true if yield
      rescue StandardError => e
        last_error = "#{e.class}: #{e.message}"
      end
      sleep POLL_INTERVAL
    end

    abort(<<~MSG)
      Timed out after #{READY_TIMEOUT}s waiting for #{description}.
      Last observed error: #{last_error}
      Inspect the service with: rake infra:logs
    MSG
  end

  # GET a URL, requiring a 2xx status.
  def http_ok?(url)
    response = Net::HTTP.get_response(URI(url))
    response.is_a?(Net::HTTPSuccess)
  rescue StandardError
    false
  end

  # --- Polaris ---------------------------------------------------------------
  #
  # Polaris exposes the catalog API on 8181 and a Quarkus management/health API
  # on 8182. The catalog itself is created by the one-shot `polaris-setup`
  # container, so readiness is not just "the process is up": we authenticate and
  # confirm the `pcl` catalog exists before any Iceberg client connects.

  def polaris_token(env = Config.load_env!)
    port = env.fetch("PCL_ICEBERG_CATALOG_PORT")
    uri = URI("http://localhost:#{port}/api/catalog/v1/oauth/tokens")
    request = Net::HTTP::Post.new(uri)
    request.basic_auth(env.fetch("PCL_POLARIS_CLIENT_ID"), env.fetch("PCL_POLARIS_CLIENT_SECRET"))
    request["Polaris-Realm"] = "POLARIS"
    request.set_form_data("grant_type" => "client_credentials", "scope" => "PRINCIPAL_ROLE:ALL")

    response = Net::HTTP.start(uri.hostname, uri.port, read_timeout: 10) do |http|
      http.request(request)
    end
    return nil unless response.is_a?(Net::HTTPSuccess)

    JSON.parse(response.body)["access_token"]
  rescue StandardError
    nil
  end

  # Create the Polaris database/schema when upgrading an existing Postgres
  # volume. The init script only runs on a brand-new volume, and Polaris refuses
  # to start if its schema is missing.
  # Create a Postgres database if it does not exist yet. The init scripts only
  # run on a brand-new volume, so an existing stack is upgraded in place.
  def ensure_database!(name, env = Config.load_env!)
    password = env.fetch("PCL_POSTGRES_PASSWORD")
    user = env.fetch("PCL_POSTGRES_USER")
    database = env.fetch("PCL_POSTGRES_DB")

    exists = capture(
      Config::POSTGRES_IMAGE,
      "psql", "-h", "postgres", "-U", user, "-d", database, "-tAc",
      "SELECT 1 FROM pg_database WHERE datname = '#{name}'",
      environment: { "PGPASSWORD" => password }
    ).strip == "1"
    return if exists

    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "-e", "PGPASSWORD",
      Config::POSTGRES_IMAGE,
      "psql", "-v", "ON_ERROR_STOP=1",
      "-h", "postgres", "-U", user, "-d", database,
      "-c", "CREATE DATABASE #{name}",
      environment: { "PGPASSWORD" => password }
    )
    puts "Created Postgres database '#{name}'."
  end

  def ensure_polaris_database!(env = Config.load_env!)
    password = env.fetch("PCL_POSTGRES_PASSWORD")
    user = env.fetch("PCL_POSTGRES_USER")

    ensure_database!("polaris", env)

    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "-e", "PGPASSWORD",
      Config::POSTGRES_IMAGE,
      "psql", "-v", "ON_ERROR_STOP=1",
      "-h", "postgres", "-U", user, "-d", "polaris",
      "-c", "CREATE SCHEMA IF NOT EXISTS polaris_schema",
      environment: { "PGPASSWORD" => password }
    )
  end

  def polaris_catalog_ready?(env = Config.load_env!)
    token = polaris_token(env)
    return false if token.nil? || token.empty?

    port = env.fetch("PCL_ICEBERG_CATALOG_PORT")
    uri = URI("http://localhost:#{port}/api/management/v1/catalogs/pcl")
    request = Net::HTTP::Get.new(uri)
    request["Authorization"] = "Bearer #{token}"
    request["Polaris-Realm"] = "POLARIS"

    response = Net::HTTP.start(uri.hostname, uri.port, read_timeout: 10) do |http|
      http.request(request)
    end
    response.is_a?(Net::HTTPSuccess)
  rescue StandardError
    false
  end

  # Ensure the S3 bucket exists before Iceberg creates its first table. The
  # pinned AWS CLI image receives secrets through the process environment.
  def ensure_iceberg_bucket!(env = Config.load_env!)
    process_env = {
      "AWS_ACCESS_KEY_ID" => env.fetch("PCL_MINIO_ROOT_USER"),
      "AWS_SECRET_ACCESS_KEY" => env.fetch("PCL_MINIO_ROOT_PASSWORD"),
      "AWS_DEFAULT_REGION" => env.fetch("PCL_MINIO_REGION")
    }
    prefix = [
      "podman", "run", "--rm", "--network", "pcl_default",
      "-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY", "-e", "AWS_DEFAULT_REGION",
      Config::AWS_CLI_IMAGE, "--endpoint-url", Config::MINIO_ENDPOINT
    ]
    bucket = env.fetch("PCL_ICEBERG_BUCKET")
    _out, _err, status = Open3.capture3(
      process_env, *prefix, "s3api", "head-bucket", "--bucket", bucket
    )
    if status.success?
      puts "MinIO bucket '#{bucket}' is ready."
      return
    end

    out, err, create_status = Open3.capture3(
      process_env, *prefix, "s3api", "create-bucket", "--bucket", bucket
    )
    abort("Could not create MinIO bucket '#{bucket}': #{err.strip} #{out.strip}") unless create_status.success?
    puts "Created MinIO bucket '#{bucket}'."
  end
end

# --- Trino client ------------------------------------------------------------
#
# Trino is the serving layer. This client only reads: the engine's read-only
# access control is the real boundary, and the keyword allowlist below fails
# fast with a clearer message.
module Trino
  READ_ONLY_KEYWORDS = %w[SELECT WITH SHOW DESCRIBE DESC EXPLAIN VALUES TABLE].freeze

  module_function

  def execute(sql, env = Config.load_env!)
    base = "http://localhost:#{env.fetch('PCL_TRINO_PORT')}/v1/statement"
    uri = URI(base)
    results = []
    query_id = nil
    next_uri = nil

    loop do
      if next_uri.nil?
        request = Net::HTTP::Post.new(uri)
        request["X-Trino-User"] = "pcl"
        request["X-Trino-Source"] = "pcl-rake"
        request["X-Trino-Catalog"] = "iceberg"
        request["Content-Type"] = "text/plain; charset=utf-8"
        request.body = sql
      else
        request = Net::HTTP::Get.new(URI(next_uri))
        request["X-Trino-User"] = "pcl"
        request["X-Trino-Source"] = "pcl-rake"
        request["X-Trino-Catalog"] = "iceberg"
      end

      response = Net::HTTP.start(uri.hostname, uri.port, read_timeout: 120) do |http|
        http.request(request)
      end
      raise "Trino HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)

      query_id ||= response["X-Trino-Query-Id"]
      payload = JSON.parse(response.body)
      query_id ||= payload["id"]

      # A failed query still returns HTTP 200 with an `error` payload, so the
      # status code alone would silently treat failures as success. That would
      # make reconciliation report a clean bill of health on broken data.
      if payload["error"]
        detail = payload.dig("error", "message").to_s
        code = payload.dig("error", "errorCode").to_s
        raise "Trino query failed (#{code}): #{detail}"
      end

      # A statement may be split into multiple result pages.
      results << payload["data"] if payload["data"] && !payload["data"].empty?
      next_uri = payload["nextUri"]
      break if next_uri.nil?
    end

    { "query_id" => query_id, "data" => results }
  end

  def execute_file(path, env = Config.load_env!)
    results = []
    query_id = nil

    split_statements(File.read(path)).each do |sql|
      result = execute(sql, env)
      query_id = result["query_id"]
      results.concat(result["data"])
    end

    { "query_id" => query_id, "data" => results }
  end

  # First SQL keyword, ignoring leading line and block comments.
  def first_keyword(statement)
    stripped = statement.gsub(%r{/\*.*?\*/}m, " ")
    stripped = stripped.lines.reject { |line| line.strip.start_with?("--") }.join
    stripped.strip.split(/\s+/, 2).first.to_s.upcase
  end

  def assert_read_only!(statements)
    statements.each do |statement|
      keyword = first_keyword(statement)
      next if READ_ONLY_KEYWORDS.include?(keyword)

      abort(<<~MSG)
        query:file is read-only and cannot run #{keyword.empty? ? 'an empty statement' : keyword}.
        Allowed: #{READ_ONLY_KEYWORDS.join(', ')}.
        Writes belong to Spark: rake lakehouse:init, lakehouse:merge, lakehouse:materialize, lakehouse:replay.
      MSG
    end
  end

  # Human-readable label for a statement: the numbered section heading of its
  # leading comment block when there is one, otherwise its first descriptive
  # line. Separator lines (`-- ----`) are ignored. Preferring the numbered
  # heading keeps `reconciliation.sql` labels stable even though the first
  # statement also carries the file-level header comment.
  def statement_label(statement)
    lines = statement.lines.map(&:strip)
    first_code = lines.index { |line| !line.empty? && !line.start_with?("--") } || lines.length
    candidates = []

    # Walk up from the first SQL line so the label comes from the comment block
    # that immediately precedes the statement, not from a file-level header
    # separated from it by a blank line.
    (first_code - 1).downto(0) do |index|
      line = lines[index]
      next if line.empty?
      break unless line.start_with?("--")

      candidate = line.sub(/\A--\s*/, "").strip
      next if candidate.empty? || candidate.match?(/\A-+\z/)

      candidates.unshift(candidate)
    end

    numbered = candidates.reverse.find { |candidate| candidate.match?(/\A\d+\.\s/) }
    return numbered if numbered

    candidates.first || lines.reject(&:empty?).first.to_s[0, 80]
  end

  # Split SQL files without treating semicolons in comments or quoted strings
  # as statement terminators. Trino's statement endpoint accepts one statement
  # per request, while our bootstrap and reconciliation files contain several.
  def split_statements(sql)
    statements = []
    current = +""
    quote = nil
    line_comment = false
    block_comment = false
    index = 0

    while index < sql.length
      char = sql[index]
      following = sql[index + 1]

      if line_comment
        current << char
        line_comment = false if char == "\n"
      elsif block_comment
        current << char
        if char == "*" && following == "/"
          current << following
          index += 1
          block_comment = false
        end
      elsif quote
        current << char
        if char == quote
          if following == quote
            current << following
            index += 1
          else
            quote = nil
          end
        end
      elsif char == "-" && following == "-"
        current << char << following
        index += 1
        line_comment = true
      elsif char == "/" && following == "*"
        current << char << following
        index += 1
        block_comment = true
      elsif ["'", '"', "`"].include?(char)
        current << char
        quote = char
      elsif char == ";"
        statements << current.strip unless current.strip.empty?
        current.clear
      else
        current << char
      end

      index += 1
    end

    statements << current.strip unless current.strip.empty?
    statements
  end
end

# --- Spark batch transformation client ---------------------------------------
#
# Spark owns DDL, the incremental MERGE and replay. It runs as a one-shot
# `spark-sql` container on the Compose network; there is no long-lived cluster.
module Spark
  SQL_MOUNT = "/opt/pcl/sql"
  CONF_MOUNT = "/opt/pcl/conf"
  CONF_DIR = File.join(Config::ROOT, ".local", "spark")

  module_function

  def image(env = Config.load_env!)
    Config.spark_image(env)
  end

  def image_present?(env = Config.load_env!)
    _out, _err, status = Open3.capture3("podman", "image", "exists", image(env))
    status.success?
  end

  def ensure_image!(env = Config.load_env!)
    return if image_present?(env)

    puts "Building #{image(env)}..."
    Infra.compose("--profile", "tools", "build", "spark")
    abort("Spark image #{image(env)} is missing after the build.") unless image_present?(env)
  end

  def run_file(path, env = Config.load_env!)
    run_sql(File.read(path), name: File.basename(path, ".sql"), env: env)
  end

  # The rendered script is kept under .local/ so the exact SQL that ran is
  # available for debugging.
  def run_sql(sql, name:, env: Config.load_env!)
    ensure_image!(env)
    Config.render_spark_conf!(env)
    FileUtils.mkdir_p(Config::SPARK_WORK_DIR)
    File.write(File.join(Config::SPARK_WORK_DIR, "#{name}.sql"), sql)

    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "-v", "#{File.join(Config::ROOT, 'sql')}:#{SQL_MOUNT}:ro",
      "-v", "#{CONF_DIR}:#{CONF_MOUNT}:ro",
      "-w", "/tmp",
      image(env),
      "/opt/spark/bin/spark-sql",
      "--properties-file", "#{CONF_MOUNT}/spark-defaults.conf",
      "--driver-memory", env.fetch("PCL_SPARK_DRIVER_MEMORY", Config::SPARK_DEFAULT_DRIVER_MEMORY),
      "-f", "#{CONF_MOUNT}/work/#{name}.sql"
    )
  end

  # Splice the shared ranking and MERGE into a top-level script.
  def expand_template(path, target:, raw_filter: nil)
    projection = File.read(File.join(Config::ROOT, "sql", "spark", "projection_source.sql"))
    merge = File.read(File.join(Config::ROOT, "sql", "spark", "merge_orders.sql"))

    sql = File.read(path)
      .gsub("-- __PROJECTION_SOURCE__") { projection }
      .gsub("-- __MERGE__") { merge }
      .gsub("__TARGET_TABLE__") { target }
    raw_filter ? sql.gsub("__RAW_FILTER__") { raw_filter } : sql
  end
end

# --- Task definitions --------------------------------------------------------

namespace :infra do
  desc "Start the lakehouse stack and wait until every service is usable"
  task :up do
    env = Config.load_env!
    Config.render_trino_catalog!(env)

    # Two-phase start: Postgres first, because Polaris needs its database and
    # schema to exist before it can connect. The init scripts only run on a
    # brand-new volume, so this also upgrades an existing volume in place.
    Infra.compose("up", "--build", "-d", "postgres")
    Infra.wait_until!("Postgres") { Infra.postgres_ready? }
    Infra.ensure_polaris_database!(env)
    Infra.ensure_database!("dagster", env)
    Infra.compose("up", "--build", "-d")

    Infra.wait_until!("Kafka broker")       { Infra.kafka_ready? }
    Infra.wait_until!("Kafka Connect REST") { Infra.http_ok?("http://localhost:#{env.fetch('PCL_CONNECT_PORT')}/connectors") }
    Infra.wait_until!("MinIO S3 API")       { Infra.http_ok?("http://localhost:#{env.fetch('PCL_MINIO_API_PORT')}/minio/health/ready") }
    Infra.ensure_iceberg_bucket!(env)
    Infra.wait_until!("Apache Polaris health") do
      Infra.http_ok?("http://localhost:#{env.fetch('PCL_POLARIS_HEALTH_PORT', '8182')}/q/health")
    end
    Infra.wait_until!("Polaris catalog 'pcl' (polaris-setup)") { Infra.polaris_catalog_ready?(env) }
    Infra.wait_until!("Trino REST")         { Infra.http_ok?("http://localhost:#{env.fetch('PCL_TRINO_PORT')}/v1/info") }
    Infra.wait_until!("Dagster webserver")  { Infra.http_ok?("#{Dagster.ui_url(env)}/server_info") }
    # The daemon needs Kafka and Trino to be up, because the first scheduled tick
    # may fire immediately; starting it last keeps that first run green.
    Dagster.start_daemon!(env)

    puts "\nAll services ready."
    puts "  Trino    http://localhost:#{env.fetch('PCL_TRINO_PORT')}"
    puts "  Connect  http://localhost:#{env.fetch('PCL_CONNECT_PORT')}"
    puts "  MinIO    http://localhost:#{env.fetch('PCL_MINIO_CONSOLE_PORT')}"
    puts "  Polaris  http://localhost:#{env.fetch('PCL_ICEBERG_CATALOG_PORT')}/api/catalog"
    puts "  Polaris health  http://localhost:#{env.fetch('PCL_POLARIS_HEALTH_PORT', '8182')}/q/health"
    puts "  Dagster  #{Dagster.ui_url(env)}"
  end

  desc "Stop the stack, keeping volumes (the CDC history is preserved)"
  task :down do
    Config.load_env!
    # Not part of the Compose file (see Dagster.start_daemon!); stop it first so
    # no run is mid-flight when the services it talks to disappear.
    Dagster.stop_daemon!
    Infra.compose("--profile", "flink", "down")
  end

  desc "Show container status"
  task :status do
    Config.load_env!
    Infra.compose("ps")
  end

  desc "Follow logs of a service: rake infra:logs[connect]"
  task :logs, [:service] do |_t, args|
    Config.load_env!
    service = args[:service] or abort("Provide a service name: rake infra:logs[connect]")
    Infra.compose("logs", "-f", service)
  end

  desc "DESTRUCTIVE: stop the stack and delete all volumes, including CDC history"
  task :reset do
    unless ENV["CONFIRM"] == "reset"
      abort(<<~MSG)
        infra:reset destroys the Iceberg history and the Postgres data.

        Re-run with explicit confirmation if that is the intent:

          CONFIRM=reset rake infra:reset
      MSG
    end

    Config.load_env!
    Dagster.stop_daemon!
    Infra.compose("--profile", "flink", "down", "-v")
    puts "Volumes removed. `rake infra:up` rebuilds an empty environment."
  end
end

namespace :fixtures do
  desc "Apply the deterministic CDC scenarios (requires `rake infra:up`)"
  task :scenarios do
    path = File.join(Config::ROOT, "infra", "fixtures", "orders_scenarios.sql")
    puts "Applying deterministic CDC scenarios..."
    Infra.psql_file(path)
    puts "Done. Expected final state per key is documented in the SQL header."
  end
end
