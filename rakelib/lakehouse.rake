# frozen_string_literal: true

# Lakehouse operations through the running services.

# Registers the Debezium connector; the password placeholder comes from .env.
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
  SPARK_SQL_DIR = File.join(Config::ROOT, "sql", "spark")

  desc "Create the Iceberg schemas and tables (raw / silver / serving / ops)"
  task :init do
    env = Config.load_env!
    Infra.ensure_iceberg_bucket!(env)
    Spark.run_file(File.join(SPARK_SQL_DIR, "bootstrap_catalog.sql"), env)
    puts "Iceberg tables are ready (raw, silver, serving, ops)."
  end

  desc "Describe the Iceberg tables, snapshots, and incremental watermark"
  task :describe do
    env = Config.load_env!

    statements = {
      "tables" => <<~SQL,
        SELECT table_schema, table_name FROM information_schema.tables
         WHERE table_schema IN ('raw','silver','serving','ops')
         ORDER BY table_schema, table_name
      SQL
      "current-state snapshots" => <<~SQL,
        SELECT snapshot_id, operation, committed_at
          FROM "silver"."orders_current$snapshots" ORDER BY committed_at DESC
      SQL
      "materialization watermark" => <<~SQL,
        SELECT target_table, last_ingested_at, updated_at
          FROM ops.materialization_state ORDER BY target_table
      SQL
      "raw invariants" => <<~SQL,
        SELECT 'raw_deliveries' AS metric, count(*) AS value FROM raw.order_cdc_events
        UNION ALL
        SELECT 'raw_events', count(DISTINCT event_id) FROM raw.order_cdc_events
        UNION ALL
        SELECT 'raw_duplicate_deliveries',
               count(*) - count(DISTINCT event_id) FROM raw.order_cdc_events
      SQL
      "lambda layers" => <<~SQL
        SELECT 'silver_keys' AS metric, count(*) AS value FROM silver.orders_current
        UNION ALL
        SELECT 'serving_keys', count(*) FROM serving.orders_current
        UNION ALL
        SELECT 'serving_behind_silver',
               count(*) FROM serving.orders_current s
                JOIN silver.orders_current b ON b.order_id = s.order_id
               WHERE b.last_lsn > s.last_lsn
      SQL
    }

    statements.each do |label, sql|
      puts "\n== #{label}"
      begin
        Trino.execute(sql, env)["data"].each { |page| page.each { |row| puts row.join(" | ") } }
      rescue StandardError => e
        puts "  unavailable: #{e.message}"
      end
    end
  end

  desc "Incrementally project the event log into silver.orders_current and correct serving"
  task :materialize do
    env = Config.load_env!
    Infra.ensure_iceberg_bucket!(env)
    sql = Spark.expand_template(
      File.join(SPARK_SQL_DIR, "materialize_orders.sql"),
      target: "pcl.silver.orders_current"
    )
    Spark.run_sql(sql, name: "materialize_orders", env: env)
    Spark.run_file(File.join(SPARK_SQL_DIR, "correct_serving.sql"), env)
    puts "silver.orders_current refreshed and serving.orders_current corrected."
  end

  desc "Overwrite serving rows whose canonical silver position is newer or equal"
  task :"correct-serving" do
    env = Config.load_env!
    Infra.ensure_iceberg_bucket!(env)
    Spark.run_file(File.join(SPARK_SQL_DIR, "correct_serving.sql"), env)
    puts "serving.orders_current corrected from canonical silver."
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

    target = "pcl.silver.orders_current_replay_#{replay_id}"
    trino_target = "iceberg.silver.orders_current_replay_#{replay_id}"
    Infra.ensure_iceberg_bucket!(env)

    filter = [
      from_lsn && "lsn >= #{from_lsn}",
      to_lsn && "lsn <= #{to_lsn}"
    ].compact.join(" AND ")
    filter = "TRUE" if filter.empty?

    sql = Spark.expand_template(
      File.join(SPARK_SQL_DIR, "replay_orders.sql"),
      target: target,
      raw_filter: filter
    )
    Spark.run_sql(sql, name: "replay_#{replay_id}", env: env)

    summary = Trino.execute("SELECT count(*), count_if(is_deleted) FROM #{trino_target}", env)["data"].flatten
    puts "Replay #{replay_id} materialized to #{target}: #{summary[0]} keys, #{summary[1]} tombstones."

    if from_lsn.nil? && to_lsn.nil?
      mismatches = Trino.execute(<<~SQL, env)["data"].flatten.first
        SELECT count(*)
          FROM (
            SELECT coalesce(r.order_id, c.order_id) AS order_id
              FROM #{trino_target} r
              FULL OUTER JOIN iceberg.silver.orders_current c
                ON r.order_id = c.order_id
             WHERE r.order_id IS NULL
                OR c.order_id IS NULL
                OR r.is_deleted IS DISTINCT FROM c.is_deleted
                OR r.status IS DISTINCT FROM c.status
                OR r.total_amount IS DISTINCT FROM c.total_amount
                OR r.customer_id IS DISTINCT FROM c.customer_id
                OR r.currency IS DISTINCT FROM c.currency
                OR r.last_lsn IS DISTINCT FROM c.last_lsn
                OR r.last_event_id IS DISTINCT FROM c.last_event_id
          ) mismatches
      SQL
      abort("Full replay #{replay_id} differs from silver.orders_current in #{mismatches} keys.") if mismatches.to_i.positive?
      puts "Full replay #{replay_id} matches silver.orders_current exactly."
    end
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
  desc "Run a read-only SQL file against Trino: rake query:file[sql/trino/reconciliation.sql]"
  task :file, [:path] do |_t, args|
    env = Config.load_env!
    relative = args[:path] or abort("Provide a SQL file: rake query:file[sql/trino/reconciliation.sql]")
    full = File.join(Config::ROOT, relative)
    abort("SQL file not found: #{relative}") unless File.exist?(full)

    statements = Trino.split_statements(File.read(full))
    Trino.assert_read_only!(statements)
    query_id = nil

    # One labelled block per statement.
    statements.each do |statement|
      result = Trino.execute(statement, env)
      query_id = result["query_id"]
      puts "\n-- #{Trino.statement_label(statement)}"
      result["data"].each { |page| page.each { |row| puts row.join(" | ") } }
    end

    puts "\n(query #{query_id})"
  end
end
