# Codex and OpenCode Project Guide

## Project profile

- Java 25 and Spring Boot 4.1. PostgreSQL uses pgjdbc pgoutput; MySQL uses Debezium's
  low-level binlog protocol client. The runtime does not use Debezium Engine or Kafka Connect.
- Use the Maven Wrapper: `./mvnw.cmd` on Windows or `./mvnw` in Bash. A global `mvn` executable is not required.
- Unit tests use in-memory DuckDB and do not need Docker.
- Integration tests use Testcontainers for PostgreSQL/MySQL and require the Docker Desktop Linux engine.
- The project writes local data under Docker and runtime data directories. Do not delete or reset those directories unless explicitly requested.

## Tool selection

Use the narrowest tool that answers the question:

- Use the available Java LSP for fast symbols, document symbols, diagnostics, and navigation. In Codex sessions without a native LSP tool, use the verified OpenCode LSP CLI described below.
- Use IntelliJ IDEA MCP for richer Java semantics and project operations: `idea_search_symbol`, `idea_get_symbol_info`, `idea_analyze_calls`, `idea_get_file_problems`, `idea_lint_files`, `idea_build_project`, `idea_get_run_configurations`, and `idea_execute_run_configuration`.
- Use `idea_reformat_file` only when formatting is explicitly requested. There is no project formatter contract to impose on unrelated files.
- Use `idea_get_project_dependencies` and `idea_get_project_modules` before assuming a dependency or module exists.
- Use `idea_list_database_connections`, `idea_test_database_connection`, `idea_list_database_schemas`, `idea_list_schema_objects`, `idea_get_database_object_description`, `idea_preview_table_data`, and `idea_execute_sql_query` for database investigation. Default to read-only SQL. Ask before DDL, writes, deletes, truncates, or migrations.
- Use `idea_list_debug_tools_connections` before runtime Java invocation. If no connection exists, use the configured run target and attach workflow before calling `idea_invoke_java_method` or debugger tools.
- Use `context7_resolve-library-id` before `context7_query-docs` when library documentation is needed.
- Use GitHub MCP for repository, issue, pull request, release, and check-run operations. Use `gh` for local authentication, Actions logs, and CLI-only GitHub workflows.
- Use Context7 MCP for current third-party library documentation, GitHub MCP for hosted repository state, IDEA MCP for project semantics, and Chrome DevTools MCP only for browser inspection or automation. Do not substitute shell or web search when the matching MCP is available and healthy.

### Project capability snapshot

- Read-only smoke calls completed successfully for this project with IDEA (`DucklakeApplication` symbol search and module/run-configuration discovery), Context7 (Debezium 3.6 library resolution), and GitHub (repository search).
- IDEA currently exposes the `debezium-server-ducklake` Java module and the `DucklakeApplication` and `DuckLakeChangeConsumerTest` run configurations. No Debug Tools JVM connection is attached by default.
- The verified Java LSP fallback is OpenCode-managed `jdtls`; a document-symbol request for `DucklakeApplication.java` returned the class and `main(String[])` symbols.
- Project-local plugin configuration was not found during the latest check. Follow the global agent guide for session-scoped MCP, skill, plugin, CLI, and sandbox behavior.

### Codex MCP usage

- Codex loads global MCP servers from `~/.codex/config.toml` and the IDEA server for this trusted project from `.codex/config.toml`.
- The verified IDEA transport for Codex is IntelliJ's native stdio server: `idea64.exe stdioMcpServer`, with `IJ_MCP_SERVER_PROJECT_PATH` and `IJ_MCP_SERVER_PORT` supplied through the MCP environment. Prefer this over an SSE bridge or `mcp-remote`.
- IDEA may expose concise tool names such as `search_symbol` to Codex, while other clients may expose the same operation as `idea_search_symbol`. Select the tool by its server and description rather than assuming one exact prefix.
- Before relying on an MCP server in a fresh session, inspect its registered tools or run a small read-only call. A server appearing in `codex mcp list` confirms configuration, not that every tool completed initialization.
- For a focused configuration check, use `codex exec --strict-config --ephemeral -C <project> <read-only prompt>`. Require the new process to call the target MCP directly; prohibit shell or OpenCode fallback when testing MCP registration.
- Verified smoke calls for this project are: IDEA symbol search for `DucklakeApplication`, Context7 resolution of the Debezium 3.6 documentation library, and a GitHub repository query for `debezium-server-ducklake`.
- Keep GitHub and Context7 credentials in `GITHUB_TOKEN` and `CONTEXT7_API_KEY`. Use `bearer_token_env_var` or `env_http_headers`; never copy their values into TOML, prompts, logs, or tracked files.

### IntelliJ IDEA MCP usage

- The project exposes `DucklakeApplication` and `DuckLakeChangeConsumerTest` run configurations.
- The verified Java flow is: native `lsp` for symbols/diagnostics, `idea_search_symbol` for project symbols, `idea_get_symbol_info` for declarations and docs, `idea_analyze_calls` for call graphs, then `idea_build_project` and `idea_execute_run_configuration` for validation.
- For database work, first list connections and test the selected connection. Then list schemas/objects, inspect object descriptions, and use `idea_execute_sql_query` or `idea_preview_table_data` with read-only SQL. The configured PostgreSQL data source has an introspected `public` schema.
- For runtime Java work, `idea_list_debug_tools_connections` must be checked first. An empty result means no JVM is attached; start a configured run target or attach a local JVM before using Java invocation or debugger tools.
- OpenCode LSP debugging accepts a `file://` URI for document symbols, for example `opencode debug lsp document-symbols file:///C:/path/to/File.java`. This is the verified Java LSP fallback from a Codex session when no native LSP tool is exposed. Java `jdtls` is managed by OpenCode and need not be on `PATH`.

## CLI toolbox

Installed and preferred for this repository:

- `rg`: terminal text/file search.
- `jq` and `yq`: inspect JSON/YAML, Compose files, and configuration.
- `duckdb`: direct read-only DuckDB/DuckLake queries and inspection.
- `gitleaks`: scan for credentials before commits or sharing changes.
- `shellcheck`: lint `.sh` scripts.
- `actionlint`: lint `.github/workflows/*.yml`.
- `hadolint`: lint Dockerfiles.
- `trivy`: scan images, filesystems, dependencies, and configuration.

Useful examples:

```text
rg "symbolOrConfig" src .github .docker
jq '.' package.json
yq '.' .docker/postgres/docker-compose.yml
duckdb -c "SELECT 1"
trivy fs --scanners vuln,secret,misconfig .
```

Use `docker compose` for the two repository stacks. If the Windows Docker endpoint is unavailable, verify Docker Desktop or use the working WSL Docker environment described by the global agent guide before running integration or E2E tests. Keep each Compose lifecycle within one environment so paths, networks, volumes, and cleanup target the same engine.

## Validation workflow

1. Inspect `git status` and the relevant files before editing.
2. For Java changes, run `idea_build_project` or `./mvnw.cmd -q -DskipTests package`, then the focused unit test.
3. For integration changes, verify Docker with `docker info`, then run the relevant Testcontainers or Compose workflow.
4. Run `actionlint`, `shellcheck`, and `hadolint` on touched automation files.
5. Run `gitleaks detect --source . --no-banner --redact` before committing.
6. Report pre-existing warnings separately from regressions introduced by the change.

## Safety

- Never put API keys, GitHub tokens, database passwords, or S3 credentials in tracked files or command output.
- Local agent credentials are supplied through `OPENAI_API_KEY`, `GITHUB_TOKEN`, and `CONTEXT7_API_KEY` environment variables.
- Do not use destructive Git commands or destructive SQL without explicit user approval.
- Preserve unrelated worktree changes.
