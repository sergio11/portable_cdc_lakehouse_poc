# frozen_string_literal: true

namespace :pipeline do
  # `layer` decides the consumer group and the Iceberg target. Batch runs are
  # bounded and leave offsets to Dagster; the speed layer is unbounded, owns its
  # offsets through Flink checkpoints, and writes to its own table.
  def run_pipeline(runner_args, flink_cluster: false, layer: "batch")
    env = Config.load_env!
    Build.container_sbt("assembly")

    if flink_cluster
      Infra.compose(
        "--profile", "flink", "up", "-d", "--no-deps", "flink-jobmanager", "flink-taskmanager"
      )
      web_port = env.fetch("PCL_FLINK_WEB_PORT", "8081")
      Infra.wait_until!("Flink JobManager REST API at localhost:#{web_port}") do
        Infra.http_ok?("http://localhost:#{web_port}/overview")
      end
      puts "Flink cluster ready: http://localhost:#{web_port}"
    end

    source_id = env.fetch("PCL_SOURCE_ID")
    speed = layer == "speed"
    runtime_env = env.merge(
      "PCL_PIPELINE_LAYER" => layer,
      "PCL_CONSUMER_GROUP" => speed ? "pcl-speed-#{source_id}" : "pcl-batch-#{source_id}",
      "PCL_TARGET_TABLE" => speed ? "speed.order_cdc_events" : "raw.order_cdc_events_landing",
      "PCL_COMMIT_OFFSETS" => speed ? "true" : "false",
      "PCL_MAX_RECORDS" => speed ? "0" : ENV.fetch("PCL_MAX_RECORDS", env.fetch("PCL_MAX_RECORDS", "0"))
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

  desc "Build and run the bounded batch graph with DirectRunner on pcl_default"
  task :direct do
    run_pipeline([], layer: "batch")
  end

  desc "Run the continuous speed layer on Flink in the foreground (own group, own table)"
  task :flink do
    run_pipeline(
      [
        "--runner=org.apache.beam.runners.flink.FlinkRunner",
        "--flinkMaster=flink-jobmanager:8081",
        # Checkpointing is what makes the streaming job restartable. KafkaIO
        # commits the checkpointed positions to the speed consumer group.
        "--checkpointingInterval=10000",
        "--checkpointingMode=EXACTLY_ONCE",
        "--externalizedCheckpointsEnabled=true",
        "--retainExternalizedCheckpointsOnCancellation=true"
      ],
      flink_cluster: true,
      layer: "speed"
    )
  end

  desc "Start the speed layer as a restartable Compose service (Flink + pipeline-speed)"
  task "speed:up" do
    env = Config.load_env!
    Build.container_sbt("assembly")
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      # `--no-deps` is deliberate: `infra:up` already owns the shared stack, and
      # letting Compose walk `depends_on` here makes podman-compose try to
      # recreate Kafka (and fail on the existing container name).
      "up", "-d", "--no-deps", "flink-jobmanager", "flink-taskmanager", "pipeline-speed"
    )
    web_port = env.fetch("PCL_FLINK_WEB_PORT", "8081")
    Infra.wait_until!("Flink JobManager REST API at localhost:#{web_port}") do
      Infra.http_ok?("http://localhost:#{web_port}/overview")
    end
    puts "Speed layer running: pcl-speed-#{env.fetch('PCL_SOURCE_ID')} -> speed.order_cdc_events"
    puts "Flink dashboard: http://localhost:#{web_port}"
  end

  desc "Stop the speed layer and its Flink cluster (checkpoints are kept)"
  task "speed:down" do
    Config.load_env!
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "stop", "pipeline-speed", "flink-taskmanager", "flink-jobmanager"
    )
  end

  desc "Show the speed layer and Flink cluster status"
  task "speed:status" do
    Config.load_env!
    Infra.compose(
      "--profile", "flink", "--profile", "speed",
      "ps", "pipeline-speed", "flink-jobmanager", "flink-taskmanager"
    )
  end

  desc "Stop the local Flink JobManager and TaskManager"
  task "flink:stop" do
    Infra.compose("--profile", "flink", "stop", "flink-jobmanager", "flink-taskmanager")
  end
end
