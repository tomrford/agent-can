from __future__ import annotations

import asyncio
import contextlib
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import uvicorn
from datastar_py.consts import ElementPatchMode
from datastar_py.sse import ServerSentEventGenerator as SSE
from datastar_py.starlette import DatastarResponse
from fasthtml.common import FastHTML
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response

from agent_can.protocol import EventDirection
from agent_can.web_render import (
    TrafficRow,
    TrafficSignal,
    format_arb_id,
    render_page,
    render_row_group,
    row_label,
    row_group_anchor_id,
)

if TYPE_CHECKING:
    from agent_can.session import LatestObservation, ObservedEvent, SessionEngine, SessionManager

MIN_EMIT_INTERVAL = 0.010
MAX_EMIT_INTERVAL = 0.050
WEB_DIR = Path(__file__).parent


@dataclass
class WebServer:
    url: str
    server: uvicorn.Server
    task: asyncio.Task[None]
    sock: socket.socket

    async def stop(self) -> None:
        self.server.should_exit = True
        with contextlib.suppress(Exception):
            await self.task
        with contextlib.suppress(OSError):
            self.sock.close()


async def start_web_server(sessions: SessionManager) -> WebServer | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        sock.setblocking(False)
    except OSError:
        sock.close()
        return None

    host, port = sock.getsockname()
    app = create_app(sessions)
    config = uvicorn.Config(app, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    return WebServer(url=f"http://{host}:{port}/", server=server, task=task, sock=sock)


def create_app(sessions: SessionManager) -> FastHTML:
    app = FastHTML(
        default_hdrs=False,
        htmx=False,
        surreal=False,
        sess_cls=None,
        secret_key="agent-can-monitor",
    )

    @app.get("/")
    async def index(request: Request) -> HTMLResponse:
        del request
        with contextlib.suppress(ValueError):
            engine = await sessions.engine()
            return HTMLResponse(render_page(engine.status()))
        return HTMLResponse(render_page(None))

    @app.get("/events")
    async def events(request: Request) -> DatastarResponse:
        return DatastarResponse(event_stream(request, sessions))

    @app.get("/style.css")
    async def style(request: Request) -> Response:
        del request
        return Response((WEB_DIR / "style.css").read_text(), media_type="text/css")

    @app.get("/datastar.js")
    async def datastar(request: Request) -> Response:
        del request
        return Response((WEB_DIR / "datastar.js").read_text(), media_type="text/javascript")

    return app


async def event_stream(request: Request, sessions: SessionManager):
    previous_keys: dict[str, set[str]] = {"rx": set(), "tx": set()}
    revision = 0
    last_emit = 0.0
    while not await request.is_disconnected():
        try:
            next_revision = await sessions.wait_for_dashboard_change(
                revision, timeout=MAX_EMIT_INTERVAL
            )
        except TimeoutError:
            next_revision = revision
        now = time.monotonic()
        if next_revision == revision:
            continue
        if now - last_emit < MIN_EMIT_INTERVAL:
            await asyncio.sleep(MIN_EMIT_INTERVAL - (now - last_emit))
        try:
            engine = await sessions.engine()
        except ValueError:
            break
        snapshot = build_snapshot(engine)
        structure_changed = False
        for event in diff_table("rx", snapshot["rx"], previous_keys["rx"]):
            structure_changed = True
            yield event
        for event in diff_table("tx", snapshot["tx"], previous_keys["tx"]):
            structure_changed = True
            yield event
        if structure_changed:
            yield SSE.patch_signals(ui_defaults(snapshot), only_if_missing=True)
        yield SSE.patch_signals(traffic_signals(engine, snapshot, previous_keys))
        previous_keys["rx"] = {row.key for row in snapshot["rx"]}
        previous_keys["tx"] = {row.key for row in snapshot["tx"]}
        revision = engine.dashboard_revision
        last_emit = time.monotonic()


def build_snapshot(engine: SessionEngine) -> dict[str, list[TrafficRow]]:
    rows = {
        "rx": rows_for_direction(engine, EventDirection.RX),
        "tx": rows_for_direction(engine, EventDirection.TX),
    }
    rows["tx"].extend(schedule_rows(engine, {row.key for row in rows["tx"]}))
    return rows


def rows_for_direction(engine: SessionEngine, direction: EventDirection) -> list[TrafficRow]:
    rows: list[TrafficRow] = []
    latest = latest_observations(engine, direction)
    for observation in latest:
        event = observation.latest_rx
        matches = engine.dbcs.matches_for_frame(
            event.message.arbitration_id, event.message.is_extended_id
        )
        if matches:
            for message in matches:
                rows.append(
                    semantic_row(
                        engine,
                        event,
                        message.qualified_name,
                        cycle_time=cycle_time_label(observation.cycle_time_ms),
                    )
                )
        else:
            rows.append(raw_row(event, cycle_time_label(observation.cycle_time_ms)))
    return sorted(rows, key=lambda row: (row.arb_id, row.extended, row.message))


def latest_observations(
    engine: SessionEngine, direction: EventDirection
) -> list[LatestObservation]:
    return [
        latest
        for (_arb_id, _extended, observed_direction), latest in engine.latest.items()
        if observed_direction == direction
    ]


def raw_row(event: ObservedEvent, cycle_time: str) -> TrafficRow:
    key = frame_row_key(event.message.is_extended_id, event.message.arbitration_id)
    return TrafficRow(
        key=key,
        message=f"0x{event.message.arbitration_id:X}",
        arb_id=event.message.arbitration_id,
        extended=event.message.is_extended_id,
        length=event.message.dlc,
        last_seen=age_label(event.unix_ms),
        cycle_time=cycle_time,
        signals=[],
    )


def semantic_row(
    engine: SessionEngine, event: ObservedEvent, qualified_name: str, cycle_time: str
) -> TrafficRow:
    signals = [
        TrafficSignal(
            name=signal.name,
            value=format_number(signal.value),
            unit=signal.unit,
            value_description=signal.value_description,
        )
        for signal in engine.dbcs.decode(qualified_name, bytes(event.message.data))
    ]
    return TrafficRow(
        key=semantic_row_key(qualified_name),
        message=qualified_name,
        arb_id=event.message.arbitration_id,
        extended=event.message.is_extended_id,
        length=event.message.dlc,
        last_seen=age_label(event.unix_ms),
        cycle_time=cycle_time,
        signals=signals,
    )


def schedule_rows(engine: SessionEngine, existing_keys: set[str]) -> list[TrafficRow]:
    rows = []
    for schedule in engine.status().periodic_schedules:
        key = (
            frame_row_key(schedule.extended, schedule.arb_id)
            if schedule.target.startswith("0x")
            else semantic_row_key(schedule.target)
        )
        if key in existing_keys:
            continue
        rows.append(
            TrafficRow(
                key=key,
                message=schedule.target,
                arb_id=schedule.arb_id,
                extended=schedule.extended,
                length=schedule.len,
                last_seen="-",
                cycle_time=f"{schedule.periodicity_ms} ms",
                signals=[],
            )
        )
    return rows


def diff_table(table: str, rows: list[TrafficRow], previous: set[str]):
    current = {row.key for row in rows}
    for removed in previous - current:
        yield SSE.remove_elements(f".{table}-key-{removed}")

    present = previous & current
    for index, row in enumerate(rows):
        if row.key in previous:
            continue
        anchor = next((candidate for candidate in reversed(rows[:index]) if candidate.key in present), None)
        if anchor:
            selector = f"#{row_group_anchor_id(table, anchor)}"
            mode = ElementPatchMode.AFTER
        else:
            selector = f"#{table}-body"
            mode = ElementPatchMode.PREPEND
        yield SSE.patch_elements(render_row_group(table, row), selector=selector, mode=mode)
        present.add(row.key)


def ui_defaults(snapshot: dict[str, list[TrafficRow]]) -> dict[str, Any]:
    return {
        "ui": {
            "rx": {row.key: False for row in snapshot["rx"]},
            "tx": {row.key: False for row in snapshot["tx"]},
        }
    }


def traffic_signals(
    engine: SessionEngine,
    snapshot: dict[str, list[TrafficRow]],
    previous_keys: dict[str, set[str]],
) -> dict[str, Any]:
    status = engine.status()
    return {
        "can": {
            "rxCount": len(snapshot["rx"]),
            "txCount": len(snapshot["tx"]),
            "error": status.backend_error or "",
            "errorCode": error_code(status.backend_error or ""),
            "rx": row_signal_map(snapshot["rx"], previous_keys["rx"]),
            "tx": row_signal_map(snapshot["tx"], previous_keys["tx"]),
        }
    }


def row_signal_map(rows: list[TrafficRow], previous: set[str]) -> dict[str, Any]:
    current = {row.key for row in rows}
    values: dict[str, Any] = {removed: None for removed in previous - current}
    for row in rows:
        values[row.key] = {
            "label": row_label(row),
            "message": row.message,
            "arbId": format_arb_id(row),
            "len": row.length,
            "lastSeen": row.last_seen,
            "cycleTime": row.cycle_time,
            "payload": "",
            "signals": {
                f"s{index}": {
                    "value": signal.value,
                    "unit": signal.unit,
                    "description": signal.value_description,
                }
                for index, signal in enumerate(row.signals)
            },
        }
    return values


def age_label(unix_ms: int) -> str:
    age = max(0, int(time.time() * 1000) - unix_ms)
    if age < 1000:
        return f"{age} ms ago"
    return f"{age / 1000:.1f} s ago"


def format_number(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def cycle_time_label(value: float | None) -> str:
    if value is None:
        return "-"
    if value < 10:
        return f"{value:.1f} ms"
    return f"{value:.0f} ms"


def frame_row_key(extended: bool, arb_id: int) -> str:
    return f"{'e' if extended else 's'}{arb_id:08X}"


def semantic_row_key(label: str) -> str:
    return "m" + label.encode().hex().upper()


def error_code(error: str) -> str:
    if not error:
        return ""
    if "PCAN_ERROR_" in error:
        code = error.split("PCAN_ERROR_", 1)[1]
        return "".join(ch for ch in code if ch.isalnum() or ch == "_")
    if "UNKNOWN:" in error:
        return "UNKNOWN"
    return "error"
