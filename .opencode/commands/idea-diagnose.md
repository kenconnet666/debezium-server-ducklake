---
description: Investigate a Java issue using native LSP and IntelliJ IDEA MCP in a fixed order.
agent: build
---
Start with native LSP diagnostics and document symbols. Use IntelliJ IDEA MCP for project symbol search, symbol information, incoming/outgoing calls, file inspections, and project dependencies. For Java changes, build with `idea_build_project` and run the narrowest available test run point. For database symptoms, use the read-only IDEA database tools after testing the connection. For runtime symptoms, check DebugTools connections before attempting Java invocation. Report tool failures separately from source-code findings.
