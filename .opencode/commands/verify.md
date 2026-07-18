---
description: Build and run the repository's static and unit verification checks.
agent: build
---
Inspect the worktree first. Run a full Java build with the Maven Wrapper, the focused unit tests, `actionlint` on both GitHub workflows, `shellcheck` on touched shell scripts, `hadolint` on touched Dockerfiles, and `gitleaks detect --source . --no-banner --redact`. Do not run integration or E2E tests unless Docker `docker info` succeeds. Report existing warnings separately from failures.
