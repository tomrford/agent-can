# agent-can

Agent-facing CAN bus session frontend for MCP clients. Primary consumer is an LLM agent over stdio MCP. Runtime access goes through `python-can`; DBC decode/encode goes through `cantools`.

Hardware support follows the installed `python-can` backend and host drivers, such as PEAK PCAN on Windows or SocketCAN on Linux.

## Build and check

Use `uv` for Python commands.

```sh
uv run ruff check .
uv run pytest
uv build --no-sources
```

Before handoff, run the full gate above unless the task is explicitly read-only or a host dependency is missing.

## Key design decisions

- **Raw-first**: runtime state is raw CAN frames. DBC is a decode/encode overlay only.
- **MCP process owns the session**: one live session per MCP process. Multi-bus means separate MCP processes on separate adapters.
- **No silent zero-fill on semantic sends**: every signal must be specified. This is intentional safety.

## Release

CI runs Ruff, pytest, and package build checks on pull requests and pushes. PyPI publishing uses GitHub trusted publishing from published GitHub releases:

```sh
gh release create v0.1.0 --target main --title v0.1.0 --generate-notes
```

Keep `pyproject.toml`, `src/agent_can/__init__.py`, and the tag version aligned.
