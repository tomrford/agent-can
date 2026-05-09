from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from agent_can_py.backend import available_buses
from agent_can_py.protocol import (
    ConnectRequest,
    MessageListRequest,
    MessageReadRequest,
    MessageSendRequest,
    MessageStopRequest,
    SchemaRequest,
    TraceStartRequest,
)
from agent_can_py.session import SessionManager

mcp = FastMCP("agent-can-py")
sessions = SessionManager()


@mcp.tool()
def buses_list() -> dict:
    """List CAN buses that can be passed to `connect` as interface/channel pairs."""
    return {"buses": [bus.model_dump() for bus in available_buses()]}


@mcp.tool()
async def connect(request: ConnectRequest) -> dict:
    """Start the one live CAN session for this MCP process."""
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
async def schema(request: SchemaRequest) -> dict:
    """Semantic discovery for the connect-time DBC set."""
    engine = await sessions.engine()
    return {"messages": [message.model_dump() for message in engine.schema(request)]}


@mcp.tool()
async def message_list(request: MessageListRequest) -> dict:
    """Observed-traffic inventory. Returns compact message entries, not decoded signal values."""
    engine = await sessions.engine()
    return {"messages": [message.model_dump() for message in engine.message_list(request)]}


@mcp.tool()
async def message_read(request: MessageReadRequest) -> dict:
    """Detailed inspection for one selector."""
    engine = await sessions.engine()
    return engine.message_read(request).model_dump()


@mcp.tool()
async def message_send(request: MessageSendRequest) -> dict:
    """Send one message by target shape."""
    engine = await sessions.engine()
    return engine.message_send(request).model_dump()


@mcp.tool()
async def message_stop(request: MessageStopRequest) -> dict:
    """Stop the periodic schedule for a raw or semantic target identity."""
    engine = await sessions.engine()
    return {"target": request.target, "stopped": engine.message_stop(request)}


@mcp.tool()
async def trace_start(request: TraceStartRequest) -> dict:
    """Start one raw ASCII trace export for the active session."""
    engine = await sessions.engine()
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
