# frozen_string_literal: true

# Unbuffered output. When Rake's stdout is a pipe (CI logs, a terminal capture,
# or a tool reading our stream) Ruby block-buffers it, so progress lines would
# only appear once the process exited. For long sbt invocations that reads as a
# hang. Flushing on every write makes progress visible immediately.
$stdout.sync = true
$stderr.sync = true

# Portable CDC Lakehouse — operational entrypoint.
#
# Every build, test and (later) infra task is a Rake task. There are no POSIX
# shell scripts in this repository: Ruby's `system`/`sh` is cross-platform and
# this file is the single, documented index of operational commands (`rake -T`).
# `rakelib/config.rake` (loaded below via the Dir glob) defines Config and Build.
# Task bodies reference them lazily at invocation time, so definition order is safe.

# Fail fast, loudly and with a non-zero exit code. No `|| true`, no swallowing.
def run!(*cmd, chdir: Config::ROOT, environment: {})
  puts "\n$ #{cmd.join(' ')}"
  success = system(environment, *cmd, chdir: chdir)
  abort("Command failed: #{cmd.join(' ')}") unless success
end

# Each gate runs as a *single* container invocation. Starting sbt costs a full
# JVM boot plus a project reload, so chaining tasks would pay that several times
# over for no benefit; sbt accepts multiple commands in one batch run.
desc "Run the full local gate: compile, format check, unit + architecture tests"
task :verify do
  Build.container_sbt(
    "scalafmtCheckAll",
    "compile",
    "Test/compile",
    Build.sbt_test_only(*Build::UNIT_TEST_PATTERNS)
  )
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
    Build.container_sbt(Build.sbt_test_only(*Build::UNIT_TEST_PATTERNS))
  end
end

# NOTE: Rake automatically imports every `rakelib/*.rake` file, which is where
# `Config` and `Build` are defined. Do not also load them here — it redefines
# constants and emits "already initialized constant" warnings.
