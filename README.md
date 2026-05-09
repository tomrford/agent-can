# agent-can

Agent-first CAN bus session frontend for MCP clients. Runtime access goes through `python-can`; DBC decode/encode goes through `cantools`.

The server runs over stdio MCP and exposes tools for adapter discovery, connect/disconnect, status, DBC schema discovery, message list/read/send/stop, periodic sends, and raw ASCII trace export.

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

Raw sends use `target: "0x123"` and a hex string payload:

```json
{ "target": "0x123", "data": "01020304" }
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

Selectors accept raw arbitration IDs such as `0x123` or semantic names such as `alias.MessageName`. DBC paths and trace paths must be absolute.

## Development

```sh
uv run ruff check .
uv run pytest
uv build --no-sources
```
