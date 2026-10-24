# frozen_string_literal: true

namespace :pipeline do
  def run_pipeline(runner_args)
    env = Config.load_env!
    Build.container_sbt("assembly")

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

  desc "Build and run the Beam CDC graph with the classic FlinkRunner in embedded local mode"
  task :flink do
    run_pipeline([
      "--runner=org.apache.beam.runners.flink.FlinkRunner",
      "--flinkMaster=[local]"
    ])
  end
end
