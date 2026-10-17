# frozen_string_literal: true

# Portable CDC Lakehouse — operational entrypoint.
#
# Every build, test and (later) infra task is a Rake task. There are no POSIX
# shell scripts in this repository: Ruby's `system`/`sh` is cross-platform and
# this file is the single, documented index of operational commands (`rake -T`).
# `rakelib/config.rake` (loaded below via the Dir glob) defines Config and Build.
# Task bodies reference them lazily at invocation time, so definition order is safe.

# Fail fast, loudly and with a non-zero exit code. No `|| true`, no swallowing.
def run!(*cmd, chdir: Config::ROOT)
  puts "\n$ #{cmd.join(' ')}"
  success = system(*cmd, chdir: chdir)
  abort("Command failed: #{cmd.join(' ')}") unless success
end

desc "Run the full local gate: compile, format check, unit + architecture tests"
task :verify do
  Rake::Task["compile"].invoke
  Rake::Task["scalafmt:check"].invoke
  Rake::Task["test:unit"].invoke
end

desc "Compile main + test sources inside the pinned build container"
task :compile do
  Build.container_sbt("compile", "Test/compile")
end

desc "Assemble the fat-jar used by DirectRunner and FlinkRunner"
task :assembly do
  Build.container_sbt("assembly")
end

desc "Format sources in place (idempotent)"
task "scalafmt:format" do
  Build.container_sbt("scalafmtAll")
end

desc "Verify formatting without writing"
task "scalafmt:check" do
  Build.container_sbt("scalafmtCheckAll")
end

namespace :test do
  desc "Run unit + architecture tests (fast, no external services)"
  task :unit do
    Build.container_sbt("testOnly", "com.example.pcl.architecture.*", "com.example.pcl.domain.*")
  end

  desc "Run integration tests (requires `rake infra:up`)"
  task :integration do
    Build.container_sbt("testOnly", "com.example.pcl.integration.*")
  end
end

# NOTE: Rake automatically imports every `rakelib/*.rake` file, which is where
# `Config` and `Build` are defined. Do not also load them here — it redefines
# constants and emits "already initialized constant" warnings.
