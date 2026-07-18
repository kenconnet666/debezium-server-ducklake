---
description: Run the focused in-memory unit test suite for this project.
agent: build
---
Run the project's unit tests without Docker. Prefer the IntelliJ IDEA MCP run point for `src/test/java/org/dpdns/zerodep/ducklake/sink/DuckLakeChangeConsumerTest.java`; otherwise run `./mvnw -q -Dtest=DuckLakeChangeConsumerTest test` or the Windows equivalent `./mvnw.cmd -q -Dtest=DuckLakeChangeConsumerTest test`. Summarize pass/fail counts and any warnings.
