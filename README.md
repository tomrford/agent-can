# agent-can

Agent-first CAN bus session frontend for MCP clients. Runtime access goes through `python-can`; DBC decode/encode goes through `cantools`.

The server runs over stdio MCP and exposes tools for adapter discovery, connect/disconnect, status, DBC schema discovery, message list/read/send/stop, periodic sends, and python-can trace export.

## MCP configuration

Use `uvx agent-can` as a stdio MCP server:

```json
{
  "mcpServers": {
    "agent-can": {
      "command": "uvx",
      "args": ["agent-can"]
    }
  }
}
```

For development from this repository:

```sh
uv run agent-can
```

## Hardware mode

Real hardware uses the installed `python-can` backend and host drivers. PEAK PCAN on Windows and SocketCAN on Linux are the intended hardware paths:

```json
{ "interface": "pcan", "channel": "PCAN_USBBUS1", "bitrate": 500000 }
```

## Message operations

Raw sends use `target: "0x123"` and a hex string payload. Standard 11-bit classic CAN is the default.

```json
{ "target": "0x123", "data": "01020304" }
```

Use `extended: true` for extended 29-bit IDs and `fd: true` for CAN FD payloads.

```json
{ "target": "0x18DAF110", "data": "0102030405060708090A0B0C", "extended": true, "fd": true }
```

Semantic sends use `target: "alias.MessageName"` and a signal map:

```json
{
  "target": "vehicle.PowertrainStatus",
  "data": {
    "vehicle_speed": 12.3,
    "engine_rpm": 1200,
    "throttle": 20,
    "coolant_temp": 82
  }
}
```

Send and read targets accept raw arbitration IDs such as `0x123` or exact semantic names such as `alias.MessageName`. Schema and message-list filters also accept partial or glob-style semantic names. DBC paths and trace paths must be absolute.

Periodic sends require `periodicity_ms` of at least `1`.

## Trace export

Trace export uses `python-can` logger suffix dispatch. The trace path must be absolute and the parent directory must exist. Use `.asc` for ASCII logs; other python-can-supported suffixes select their corresponding writer.

## Development

```sh
uv run ruff check .
uv run pytest
uv build --no-sources
```
