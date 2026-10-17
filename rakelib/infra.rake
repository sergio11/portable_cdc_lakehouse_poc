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
  def capture(image, *args)
    stdout, stderr, status = Open3.capture3("podman", "run", "--rm",
                                           "--network", "pcl_default", image, *args)
    raise "podman run failed: #{stderr.strip}" unless status.success?
    stdout
  end

  def postgres_ready?
    capture(
      Config::POSTGRES_IMAGE,
      "psql", "-h", "postgres", "-U", Config.pg("PCL_POSTGRES_USER"),
      "-d", Config.pg("PCL_POSTGRES_DB"), "-tAc", "SELECT 1"
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
      "-v", "#{sql_path}:/sql/input.sql:ro",
      Config::POSTGRES_IMAGE,
      "psql", "-v", "ON_ERROR_STOP=1",
      "-h", "postgres",
      "-U", env.fetch("PCL_POSTGRES_USER"),
      "-d", env.fetch("PCL_POSTGRES_DB"),
      "-f", "/sql/input.sql"
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
end

# --- Trino client ------------------------------------------------------------
#
# Trino speaks a simple statement protocol over HTTP. Talking to it directly
# keeps the query layer dependency-free: no Trino CLI image, no JDBC driver, no
# gems, and the same code path from Windows, Linux and macOS.
module Trino
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
        request.set_form_data("query" => sql)
      else
        request = Net::HTTP::Get.new(URI(next_uri))
        request["X-Trino-User"] = "pcl"
        request["X-Trino-Source"] = "pcl-rake"
      end

      response = Net::HTTP.start(uri.hostname, uri.port, read_timeout: 120) do |http|
        http.request(request)
      end
      raise "Trino HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)

      query_id ||= response["X-Trino-Query-Id"]
      payload = JSON.parse(response.body)

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
      break if next_uri.nil?
    end

    { "query_id" => query_id, "data" => results }
  end

  def execute_file(path, env = Config.load_env!)
    execute(File.read(path), env)
  end
end

# --- Task definitions --------------------------------------------------------

namespace :infra do
  desc "Start the lakehouse stack and wait until every service is usable"
  task :up do
    env = Config.load_env!
    Infra.compose("up", "-d")

    Infra.wait_until!("Kafka broker")       { Infra.kafka_ready? }
    Infra.wait_until!("Postgres")           { Infra.postgres_ready? }
    Infra.wait_until!("Kafka Connect REST") { Infra.http_ok?("http://localhost:#{env.fetch('PCL_CONNECT_PORT')}/connectors") }
    Infra.wait_until!("Trino REST")         { Infra.http_ok?("http://localhost:#{env.fetch('PCL_TRINO_PORT')}/v1/info") }

    puts "\nAll services ready."
    puts "  Trino    http://localhost:#{env.fetch('PCL_TRINO_PORT')}"
    puts "  Connect  http://localhost:#{env.fetch('PCL_CONNECT_PORT')}"
  end

  desc "Stop the stack, keeping volumes (the CDC history is preserved)"
  task :down do
    Config.load_env!
    Infra.compose("down")
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
    Infra.compose("down", "-v")
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