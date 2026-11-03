# frozen_string_literal: true

namespace :pipeline do
  # Bounded DirectRunner portability check; it appends and leaves offsets alone.
  def run_bounded(runner_args)
    env = Config.load_env!

    # Must terminate, unlike the unbounded speed job.
    max_records = ENV.fetch("PCL_MAX_RECORDS", "")
    unless max_records.match?(/\A\d+\z/) && max_records.to_i.positive?
      abort("Set PCL_MAX_RECORDS to a positive record count for the bounded DirectRunner run.")
    end

    Build.container_sbt("assembly")

    source_id = env.fetch("PCL_SOURCE_ID")
    runtime_env = env.merge(
      "PCL_PIPELINE_LAYER" => "batch",
      "PCL_CONSUMER_GROUP" => "pcl-batch-#{source_id}",
      "PCL_TARGET_TABLE" => "raw.order_cdc_events",
      "PCL_COMMIT_OFFSETS" => "false",
      "PCL_MAX_RECORDS" => max_records
    )
    env_file = Config.render_pipeline_env!(runtime_env)
    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "--env-file", env_file,
      "-v", "#{Config::BUILD_VOLUME}-target:/workspace/target:ro",
      "-w", "/workspace",
      "--entrypoint", "java",
      Config.sbt_image(env),
      "-jar", "/workspace/target/scala-2.13/pcl-pipeline.jar",
      *runner_args,
      *ENV.fetch("PCL_BEAM_ARGS", "").split
    )
  end

  desc "Build and run the bounded graph with DirectRunner (manual portability check)"
  task :direct do
    run_bounded([])
  end

  desc "Start the speed writers as Compose services (Flink + Beam speed + SQL serving)"
  task "speed:up" do
    env = Config.load_env!
    Build.container_sbt("assembly")
    Config.render_flink_serving!(env)

    # Bring the cluster up before any submitter: the SQL client fails fast.
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "build", "flink-jobmanager"
    )
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      # `--no-deps`: infra:up already owns the shared stack.
      "up", "-d", "--no-deps", "flink-jobmanager", "flink-taskmanager"
    )
    web_port = env.fetch("PCL_FLINK_WEB_PORT", "8081")
    Infra.wait_until!("Flink JobManager REST API at localhost:#{web_port}") do
      Infra.http_ok?("http://localhost:#{web_port}/overview")
    end

    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "up", "-d", "--no-deps", "pipeline-speed"
    )

    # The SQL submitter exits after handing the job to the session cluster, so
    # only start it when the job is not already running. Without this guard a
    # second `speed:up` would submit a duplicate writer.
    if Flink.running?("pcl-serving", env)
      puts "Flink SQL serving job is already running; not resubmitting."
    else
      Infra.compose(
        "--profile", "flink", "--profile", "speed",
        "up", "-d", "--no-deps", "pipeline-serving"
      )
    end

    puts "Speed layer running: pcl-speed-#{env.fetch('PCL_SOURCE_ID')} -> raw.order_cdc_events"
    puts "Serving writer running: raw.order_cdc_events -> serving.orders_current"
    puts "Flink dashboard: http://localhost:#{web_port}"
  end

  desc "Stop the speed writers and their Flink cluster (checkpoints are kept)"
  task "speed:down" do
    Config.load_env!
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "stop", "pipeline-serving", "pipeline-speed", "flink-taskmanager", "flink-jobmanager"
    )
  end

  desc "Show the speed writers and Flink cluster status"
  task "speed:status" do
    env = Config.load_env!
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "ps", "pipeline-serving", "pipeline-speed", "flink-jobmanager", "flink-taskmanager"
    )
    jobs = Flink.jobs(env)
    jobs.each { |job| puts format("  %-10s %s", job["state"], job["name"]) }
  end

  desc "Stop the local Flink JobManager and TaskManager"
  task "flink:stop" do
    Infra.compose("--profile", "flink", "stop", "flink-jobmanager", "flink-taskmanager")
  end
end
