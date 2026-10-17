# frozen_string_literal: true

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
end

# Runs sbt inside the pinned container with the repo bind-mounted. The host needs
# only Podman — no local JVM, no local sbt, no Ruby gems beyond Rake itself.
module Build
  include Config

  # sbt commands are passed as separate argv entries (never interpolated into a
  # shell string), so paths with spaces and metacharacters stay safe.
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
