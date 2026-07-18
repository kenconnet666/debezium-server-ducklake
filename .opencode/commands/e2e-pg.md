---
description: Run the PostgreSQL Docker Compose smoke test when Docker is available.
agent: build
---
First run `docker info`. If the Docker Desktop Linux engine is unavailable, stop and report that prerequisite. Otherwise use the repository's PostgreSQL stack under `.docker/postgres/`, following the README and its `.env.example`. Prefer infra-only mode for IDE debugging; use the `app` profile and `.docker/postgres/e2e-verify.sh` for a full end-to-end check. Never remove persistent data without explicit approval.
