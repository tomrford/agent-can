# agent-can

Python CAN MCP server spike built around `python-can` and `cantools`.

No web monitor is included in this spike.

## Run

```sh
uv run agent-can
```

Demo mode works without hardware:

```json
{
  "interface": "demo",
  "channel": "demo",
  "bitrate": 500000,
  "fd": false,
  "dbcs": [
    { "alias": "demo", "path": "/absolute/path/to/examples/demo.dbc" }
  ]
}
```

Real hardware uses `python-can` directly:

```json
{ "interface": "pcan", "channel": "PCAN_USBBUS1", "bitrate": 500000 }
```

Raw sends use `target: "0x123"` and a hex string payload. Semantic sends use `target: "alias.MessageName"` and a signal map.
