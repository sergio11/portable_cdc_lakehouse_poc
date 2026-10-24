# frozen_string_literal: true

# Lakehouse operations: create the Iceberg schemas/tables, register the Debezium
# connector, and expose inspection queries. All of it goes through the same
# running services that the pipeline uses, so there is no parallel tooling path.

# Registers the Debezium PostgreSQL connector against the running Connect REST
# API. The connector definition is a committed template; the password placeholder
# is substituted from .env at call time so no credential is ever in Git.
module Debezium
  TEMPLATE = File.join("infra", "debezium", "postgres-orders-connector.json").freeze

  module_function

  def template_path
    File.join(Config::ROOT, TEMPLATE)
  end

  # Resolve the __PLACEHOLDER__ tokens against the local .env values.
  def rendered_template(env = Config.load_env!)
    body = File.read(template_path)
    env.each do |key, value|
      body = body.gsub("__#{key}__", value)
    end
    body
  end

  def connect_url(env = Config.load_env!)
    "http://localhost:#{env.fetch('PCL_CONNECT_PORT')}/connectors"
  end

  def register(env = Config.load_env!)
    url = connect_url(env)

    # Fail early with a clear message rather than a bare connection error.
    unless Infra.http_ok?(url)
      abort("Kafka Connect is not reachable at #{url}. Run `rake infra:up` first.")
    end

    payload = JSON.parse(rendered_template(env))
    name = payload.fetch("name")

    uri = URI("#{url}/#{name}/config")
    http = Net::HTTP.new(uri.hostname, uri.port)
    http.read_timeout = 60

    request = Net::HTTP::Put.new(uri)
    request["Content-Type"] = "application/json"
    request.body = JSON.generate(payload.fetch("config"))

    response = http.request(request)

    case response.code
    when "200", "201"
      puts "Debezium connector '#{name}' registered."
    else
      abort("Debezium registration failed (#{response.code}): #{response.body}")
    end
  end

  def status(env = Config.load_env!)
    name = JSON.parse(File.read(template_path)).fetch("name")
    response = Net::HTTP.get_response(URI("#{connect_url(env)}/#{name}/status"))
    puts response.body
  end
end

namespace :lakehouse do
  desc "Create the Iceberg schemas and tables (raw / silver / ops)"
  task :init do
    env = Config.load_env!
    Infra.ensure_iceberg_bucket!(env)
    path = File.join(Config::ROOT, "sql", "bootstrap_catalog.sql")
    result = Trino.execute_file(path, env)

    puts "Iceberg tables ready (query #{result['query_id']})."
  end

  desc "Describe the Iceberg tables and their snapshots"
  task :describe do
    env = Config.load_env!

    statements = {
      "tables" => <<~SQL,
        SELECT table_schema, table_name FROM information_schema.tables
         WHERE table_schema IN ('raw','silver','ops') ORDER BY table_schema, table_name
      SQL
      "current-state snapshots" => <<~SQL,
        SELECT snapshot_id, operation, committed_at
          FROM "silver"."orders_current$snapshots" ORDER BY committed_at DESC
      SQL
    }

    # Each probe is independent: a table that has not been materialised yet must
    # not hide the rest of the report.
    statements.each do |label, sql|
      puts "\n== #{label}"
      begin
        Trino.execute(sql, env)["data"].each { |page| page.each { |row| puts row.join(" | ") } }
      rescue StandardError => e
        puts "  unavailable: #{e.message}"
      end
    end
  end

  desc "Materialize the latest raw CDC event per key into silver.orders_current"
  task :materialize do
    env = Config.load_env!
    Infra.ensure_iceberg_bucket!(env)
    path = File.join(Config::ROOT, "sql", "materialize_orders.sql")
    sql = File.read(path)
      .gsub("__TARGET_TABLE__", "iceberg.silver.orders_current")
      .gsub("__RAW_FILTER__", "TRUE")
    result = Trino.execute(sql.sub(/;\s*\z/, ""), env)
    puts "Current-state projection refreshed (query #{result['query_id']})."
  end

  desc "Replay raw CDC history into an isolated table: PCL_REPLAY_ID=run1 [PCL_REPLAY_FROM_LSN=..] [PCL_REPLAY_TO_LSN=..]"
  task :replay do
    env = Config.load_env!
    replay_id = ENV.fetch("PCL_REPLAY_ID") { abort("Set PCL_REPLAY_ID to a short identifier using lowercase letters, digits, or underscores.") }
    abort("PCL_REPLAY_ID must match [a-z][a-z0-9_]{0,39}.") unless replay_id.match?(/\A[a-z][a-z0-9_]{0,39}\z/)

    from_lsn = ENV["PCL_REPLAY_FROM_LSN"]
    to_lsn = ENV["PCL_REPLAY_TO_LSN"]
    [from_lsn, to_lsn].compact.each do |value|
      abort("Replay LSN bounds must be non-negative integers.") unless value.match?(/\A\d+\z/)
    end
    abort("PCL_REPLAY_FROM_LSN must not exceed PCL_REPLAY_TO_LSN.") if from_lsn && to_lsn && from_lsn.to_i > to_lsn.to_i

    target = "iceberg.silver.orders_current_replay_#{replay_id}"
    Infra.ensure_iceberg_bucket!(env)
    Trino.execute("CREATE TABLE IF NOT EXISTS #{target} WITH (format_version = 2) AS SELECT * FROM iceberg.silver.orders_current WHERE false", env)

    filter = [
      from_lsn && "lsn >= #{from_lsn}",
      to_lsn && "lsn <= #{to_lsn}"
    ].compact.join(" AND ")
    filter = "TRUE" if filter.empty?
    template = File.read(File.join(Config::ROOT, "sql", "materialize_orders.sql"))
    sql = template.gsub("__TARGET_TABLE__", target).gsub("__RAW_FILTER__", filter)
    result = Trino.execute(sql.sub(/;\s*\z/, ""), env)
    summary = Trino.execute("SELECT count(*), count_if(is_deleted) FROM #{target}", env)["data"].flatten
    puts "Replay #{replay_id} materialized to #{target}: #{summary[0]} keys, #{summary[1]} tombstones (query #{result['query_id']})."
  end
end

namespace :debezium do
  desc "Register the Debezium PostgreSQL connector for public.orders"
  task :register do
    Debezium.register
  end

  desc "Show the Debezium connector status"
  task :status do
    Debezium.status
  end
end

namespace :query do
  desc "Run a SQL file against Trino: rake query:file[sql/reconciliation.sql]"
  task :file, [:path] do |_t, args|
    env = Config.load_env!
    relative = args[:path] or abort("Provide a SQL file: rake query:file[sql/reconciliation.sql]")
    full = File.join(Config::ROOT, relative)
    abort("SQL file not found: #{relative}") unless File.exist?(full)

    sql = File.read(full)
    result = Trino.execute_file(full, env)

    puts sql.strip
    puts "\n-- result"
    result["data"].each { |page| page.each { |row| puts row.join(" | ") } }
    puts "\n(query #{result['query_id']})"
  end
end
