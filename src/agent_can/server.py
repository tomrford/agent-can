from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agent_can.backend import available_buses
from agent_can.protocol import ConnectRequest
from agent_can.protocol import DbcSpec
from agent_can.protocol import MessageListRequest
from agent_can.protocol import MessageReadRequest
from agent_can.protocol import MessageSendRequest
from agent_can.protocol import MessageStopRequest
from agent_can.protocol import SchemaRequest
from agent_can.protocol import TraceStartRequest
from agent_can.session import SessionManager

mcp = FastMCP("agent-can")
sessions = SessionManager()


@mcp.tool()
def buses_list() -> dict:
    """List CAN buses that can be passed to `connect` as interface/channel pairs."""
    return {"buses": [bus.model_dump() for bus in available_buses()]}


@mcp.tool()
async def connect(
    interface: str,
    channel: str | int,
    bitrate: int,
    bitrate_data: int | None = None,
    fd: bool = False,
    dbcs: list[DbcSpec] | None = None,
) -> dict:
    """Start the one live CAN session for this MCP process."""
    request = ConnectRequest(
        interface=interface,
        channel=channel,
        bitrate=bitrate,
        bitrate_data=bitrate_data,
        fd=fd,
        dbcs=dbcs or [],
    )
    return (await sessions.connect(request)).model_dump()


@mcp.tool()
async def disconnect() -> dict:
    """Stop periodic sends, finalize trace export, and tear down the current session."""
    return {"disconnected": await sessions.disconnect()}


@mcp.tool()
async def status() -> dict:
    """Show the detailed operational status for the live session."""
    return (await sessions.engine()).status().model_dump()


@mcp.tool()
async def schema(filter: str | None = None) -> dict:
    """Semantic discovery for the connect-time DBC set."""
    engine = await sessions.engine()
    request = SchemaRequest(filter=filter)
    return {"messages": [message.model_dump() for message in engine.schema(request)]}


@mcp.tool()
async def message_list(
    filter: str | None = None,
    allow_raw: bool = False,
) -> dict:
    """Observed-traffic inventory. Returns compact message entries, not decoded signal values."""
    engine = await sessions.engine()
    request = MessageListRequest(filter=filter, allow_raw=allow_raw)
    return {"messages": [message.model_dump() for message in engine.message_list(request)]}


@mcp.tool()
async def message_read(
    select: str,
    count: int | None = None,
) -> dict:
    """Detailed inspection for one selector."""
    engine = await sessions.engine()
    request = MessageReadRequest(select=select, count=count)
    return engine.message_read(request).model_dump()


@mcp.tool()
async def message_send(
    target: str,
    data: str | dict[str, float],
    periodicity_ms: int | None = None,
) -> dict:
    """Send one message by target shape."""
    engine = await sessions.engine()
    request = MessageSendRequest(target=target, data=data, periodicity_ms=periodicity_ms)
    return engine.message_send(request).model_dump()


@mcp.tool()
async def message_stop(target: str) -> dict:
    """Stop the periodic schedule for a raw or semantic target identity."""
    engine = await sessions.engine()
    request = MessageStopRequest(target=target)
    return {"target": request.target, "stopped": engine.message_stop(request)}


@mcp.tool()
async def trace_start(path: str) -> dict:
    """Start one raw ASCII trace export for the active session."""
    engine = await sessions.engine()
    request = TraceStartRequest(path=path)
    return {"path": engine.start_trace(request)}


@mcp.tool()
async def trace_stop() -> dict:
    """Stop the current raw trace export."""
    engine = await sessions.engine()
    return {"path": engine.stop_trace()}


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
