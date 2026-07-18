---
description: Run the MySQL Docker Compose smoke test when Docker is available.
agent: build
---
First run `docker info`. If the Docker Desktop Linux engine is unavailable, stop and report that prerequisite. Otherwise use the repository's MySQL stack under `.docker/mysql/`, following the README and its `.env.example`, then run `.docker/mysql/e2e-verify.sh`. Never remove persistent data without explicit approval.
