# OpenCode Project Guide

## Project profile

- Java 25 and Debezium 3.6 embedded engine.
- Use the Maven Wrapper: `./mvnw.cmd` on Windows or `./mvnw` in Bash. A global `mvn` executable is not required.
- Unit tests use in-memory DuckDB and do not need Docker.
- Integration tests use Testcontainers for PostgreSQL/MySQL and require the Docker Desktop Linux engine.
- The project writes local data under Docker and runtime data directories. Do not delete or reset those directories unless explicitly requested.

## Tool selection

Use the narrowest tool that answers the question:

- Use native `lsp` for fast Java symbols, document symbols, diagnostics, and navigation.
- Use IntelliJ IDEA MCP for richer Java semantics and project operations: `idea_search_symbol`, `idea_get_symbol_info`, `idea_analyze_calls`, `idea_get_file_problems`, `idea_lint_files`, `idea_build_project`, `idea_get_run_configurations`, and `idea_execute_run_configuration`.
- Use `idea_reformat_file` only when formatting is explicitly requested. There is no project formatter contract to impose on unrelated files.
- Use `idea_get_project_dependencies` and `idea_get_project_modules` before assuming a dependency or module exists.
- Use `idea_list_database_connections`, `idea_test_database_connection`, `idea_list_database_schemas`, `idea_list_schema_objects`, `idea_get_database_object_description`, `idea_preview_table_data`, and `idea_execute_sql_query` for database investigation. Default to read-only SQL. Ask before DDL, writes, deletes, truncates, or migrations.
- Use `idea_list_debug_tools_connections` before runtime Java invocation. If no connection exists, use the configured run target and attach workflow before calling `idea_invoke_java_method` or debugger tools.
- Use `context7_resolve-library-id` before `context7_query-docs` when library documentation is needed.
- Use GitHub MCP for repository, issue, pull request, release, and check-run operations. Use `gh` for local authentication, Actions logs, and CLI-only GitHub workflows.

### IntelliJ IDEA MCP usage

- The project exposes a `DucklakeApplication` run configuration and test run points in `DuckLakeChangeConsumerTest`.
- The verified Java flow is: native `lsp` for symbols/diagnostics, `idea_search_symbol` for project symbols, `idea_get_symbol_info` for declarations and docs, `idea_analyze_calls` for call graphs, then `idea_build_project` and `idea_execute_run_configuration` for validation.
- For database work, first list connections and test the selected connection. Then list schemas/objects, inspect object descriptions, and use `idea_execute_sql_query` or `idea_preview_table_data` with read-only SQL. The configured PostgreSQL data source has an introspected `public` schema.
- For runtime Java work, `idea_list_debug_tools_connections` must be checked first. An empty result means no JVM is attached; start a configured run target or attach a local JVM before using Java invocation or debugger tools.
- Native OpenCode LSP debugging accepts a `file://` URI for document symbols, for example `opencode debug lsp document-symbols file:///C:/path/to/File.java`. Java `jdtls` is managed by OpenCode and need not be on `PATH`.

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

Use `docker compose` for the two repository stacks. If Docker reports that the Linux engine is unavailable, start Docker Desktop and verify with `docker info` before running integration or E2E tests.

## Validation workflow

1. Inspect `git status` and the relevant files before editing.
2. For Java changes, run `idea_build_project` or `./mvnw.cmd -q -DskipTests package`, then the focused unit test.
3. For integration changes, verify Docker with `docker info`, then run the relevant Testcontainers or Compose workflow.
4. Run `actionlint`, `shellcheck`, and `hadolint` on touched automation files.
5. Run `gitleaks detect --source . --no-banner --redact` before committing.
6. Report pre-existing warnings separately from regressions introduced by the change.

## Safety

- Never put API keys, GitHub tokens, database passwords, or S3 credentials in tracked files or command output.
- OpenCode credentials are supplied through `OPENAI_API_KEY`, `GITHUB_TOKEN`, and `CONTEXT7_API_KEY` environment variables.
- Do not use destructive Git commands or destructive SQL without explicit user approval.
- Preserve unrelated worktree changes.
