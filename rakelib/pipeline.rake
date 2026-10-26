# frozen_string_literal: true

namespace :pipeline do
  def run_pipeline(runner_args, flink_cluster: false)
    env = Config.load_env!
    Build.container_sbt("assembly")

    if flink_cluster
      Infra.compose("--profile", "flink", "up", "-d", "flink-jobmanager", "flink-taskmanager")
      web_port = env.fetch("PCL_FLINK_WEB_PORT", "8081")
      Infra.wait_until!("Flink JobManager REST API at localhost:#{web_port}") do
        Infra.http_ok?("http://localhost:#{web_port}/overview")
      end
      puts "Flink cluster ready: http://localhost:#{web_port}"
    end

    runtime_env = env.merge(
      "PCL_MAX_RECORDS" => ENV.fetch("PCL_MAX_RECORDS", env.fetch("PCL_MAX_RECORDS", "0"))
    )
    env_file = Config.render_pipeline_env!(runtime_env)
    run!(
      "podman", "run", "--rm", "--network", "pcl_default",
      "--env-file", env_file,
      "-v", "#{Config::BUILD_VOLUME}-target:/workspace/target:ro",
      "-w", "/workspace",
      "--entrypoint", "java",
      Config::SBT_IMAGE,
      "-jar", "/workspace/target/scala-2.13/pcl-pipeline.jar",
      *runner_args,
      *ENV.fetch("PCL_BEAM_ARGS", "").split
    )
  end

  desc "Build and run the Beam CDC graph with DirectRunner on pcl_default"
  task :direct do
    run_pipeline([])
  end

  desc "Build and run the Beam CDC graph on the local Flink JobManager/TaskManager cluster"
  task :flink do
    run_pipeline([
      "--runner=org.apache.beam.runners.flink.FlinkRunner",
      "--flinkMaster=flink-jobmanager:8081",
      # Checkpointing is what makes the streaming job restartable. Without it
      # Flink keeps no durable state and a restart replays from the committed
      # Kafka offsets, appending duplicate rows to the audit table.
      "--checkpointingInterval=10000",
      "--checkpointingMode=EXACTLY_ONCE",
      "--externalizedCheckpointsEnabled=true",
      "--retainExternalizedCheckpointsOnCancellation=true"
    ], flink_cluster: true)
  end

  desc "Stop the local Flink JobManager and TaskManager"
  task "flink:stop" do
    Infra.compose("--profile", "flink", "stop", "flink-jobmanager", "flink-taskmanager")
  end
end
