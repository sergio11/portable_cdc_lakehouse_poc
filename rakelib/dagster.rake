# frozen_string_literal: true

require "fileutils"
require "open3"
require "yaml"

# Dagster is the scheduled entrypoint: it materialises the lakehouse graph every
# five minutes by running the same containers the Rake tasks run. The tasks here
# start it, validate the code location, and trigger a run on demand; the Rake
# `lakehouse:*` tasks stay as the manual and test path.

module Dagster
  DAEMON = "pcl-dagster-daemon"

  module_function

  def image(env = Config.load_env!)
    "localhost/pcl-dagster:#{env.fetch('PCL_DAGSTER_VERSION', '1.13.27')}"
  end

  def ui_url(env = Config.load_env!)
    "http://localhost:#{env.fetch('PCL_DAGSTER_PORT', '3000')}"
  end

  # The daemon executes runs and starts Spark/Beam as sibling containers, so it
  # needs the engine socket. It is created here rather than by Compose because
  # podman-compose resolves absolute bind sources with os.path.realpath: on
  # Windows that turns /run/user/.../podman.sock into /mnt/c/run/... and mounts
  # an empty directory instead of the socket. The Compose labels keep the
  # container listed by `compose ps` and removed by `compose down`.
  def start_daemon!(env = Config.load_env!)
    stop_daemon!

    run!(
      "podman", "run", "-d",
      "--name", DAEMON,
      "--network", "pcl_default",
      "--env-file", Config::ENV_FILE,
      "-e", "DAGSTER_HOME=/opt/dagster/home",
      "-e", "DOCKER_HOST=unix:///run/podman/podman.sock",
      "-v", "#{Config::ROOT}:/workspace",
      "-v", "#{File.join(Config::ROOT, 'infra', 'dagster', 'dagster.yaml')}:/opt/dagster/home/dagster.yaml:ro",
      "-v", "pcl_dagster_home:/opt/dagster/home/storage",
      "-v", "#{env.fetch('PCL_PODMAN_SOCKET')}:/run/podman/podman.sock",
      "--label", "io.podman.compose.project=pcl",
      "--label", "com.docker.compose.project=pcl",
      "--label", "com.docker.compose.service=dagster-daemon",
      "--label", "com.docker.compose.container-number=1",
      image(env),
      "dagster-daemon", "run", "-w", "/opt/dagster/app/workspace.yaml"
    )
  end

  # Idempotent: an absent container is already the desired state.
  def stop_daemon!
    _out, _err, status = Open3.capture3("podman", "rm", "-f", DAEMON)
    puts "Stopped #{DAEMON}." if status.success?
  end

  # Runs a Dagster CLI command inside the daemon, which already holds the
  # workspace, the instance configuration and the engine socket.
  def exec(*command)
    run!("podman", "exec", DAEMON, *command)
  end
end

namespace :dagster do
  desc "Start the Dagster webserver and daemon, and wait for the UI"
  task :up do
    env = Config.load_env!
    Infra.ensure_database!("dagster", env)
    # --no-deps: the rest of the stack is already running, and letting compose
    # recreate it would interrupt the services this one reads from.
    Infra.compose("up", "--build", "-d", "--no-deps", "dagster-webserver")
    Dagster.start_daemon!(env)
    Infra.wait_until!("Dagster webserver at #{Dagster.ui_url(env)}") do
      Infra.http_ok?("#{Dagster.ui_url(env)}/server_info")
    end
    puts "Dagster UI: #{Dagster.ui_url(env)}"
  end

  desc "Stop the Dagster webserver and daemon (run history is kept)"
  task :down do
    Config.load_env!
    Dagster.stop_daemon!
    Infra.compose("stop", "dagster-webserver")
  end

  desc "Load and validate the code location"
  task :validate do
    env = Config.load_env!
    run!(
      "podman", "run", "--rm", Dagster.image(env),
      "dagster", "definitions", "validate",
      "-w", "/opt/dagster/app/workspace.yaml"
    )
  end

  desc "Trigger one incremental run now; the schedule does this every five minutes"
  task :run do
    Config.load_env!
    # `job execute` runs in this process, so the command blocks, streams the
    # step logs and exits non-zero when the run fails. `job launch` would queue
    # the run and return before it had done anything.
    Dagster.exec(
      "dagster", "job", "execute",
      "-m", "pcl_dagster.definitions",
      "-j", "pcl_incremental"
    )
  end

  desc "Replay the projection from raw history: rake dagster:replay[id,from_lsn,to_lsn]"
  task :replay, [:id, :from_lsn, :to_lsn] do |_t, args|
    Config.load_env!
    replay_id = args[:id] or abort("Provide a replay id: rake dagster:replay[readme_proof]")

    config = { "ops" => { "orders_current_replay" => { "config" => { "replay_id" => replay_id } } } }
    config["ops"]["orders_current_replay"]["config"]["from_lsn"] = args[:from_lsn].to_i if args[:from_lsn]
    config["ops"]["orders_current_replay"]["config"]["to_lsn"] = args[:to_lsn].to_i if args[:to_lsn]

    FileUtils.mkdir_p(File.join(Config::ROOT, ".local", "dagster"))
    path = File.join(Config::ROOT, ".local", "dagster", "replay_#{replay_id}.yaml")
    File.write(path, config.to_yaml)

    Dagster.exec(
      "dagster", "job", "execute",
      "-m", "pcl_dagster.definitions",
      "-j", "pcl_replay",
      "-c", "/workspace/.local/dagster/replay_#{replay_id}.yaml"
    )
  end
end
