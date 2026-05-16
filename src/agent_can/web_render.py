from __future__ import annotations

from dataclasses import dataclass

from fasthtml.common import (
    Body,
    Button,
    Details,
    Div,
    H1,
    H2,
    H3,
    Head,
    Hr,
    Html,
    Input,
    Link,
    Main,
    Meta,
    Nav,
    P,
    Script,
    Section,
    Span,
    Strong,
    Summary,
    Table,
    Tbody,
    Td,
    Th,
    Thead,
    Title,
    Tr,
    to_xml,
)
from agent_can.protocol import SessionStatus


@dataclass(frozen=True)
class TrafficSignal:
    name: str
    value: str
    unit: str | None
    value_description: str | None


@dataclass(frozen=True)
class TrafficRow:
    key: str
    message: str
    arb_id: int
    extended: bool
    length: int
    last_seen: str
    cycle_time: str
    signals: list[TrafficSignal]


def render_page(status: SessionStatus | None) -> str:
    return to_xml(
        Html(
            Head(
                Meta(charset="utf-8"),
                Meta(name="viewport", content="width=device-width, initial-scale=1"),
                Title("agent-can monitor"),
                Link(rel="stylesheet", href="/style.css"),
                Script(src="/datastar.js", type="module"),
            ),
            Body(
                Main(render_topbar(status), render_traffic(), id="dashboard"),
                **{
                    "data-signals": '{"can":{"rxCount":0,"txCount":0,"rx":{},"tx":{},"error":"","errorCode":""},"ui":{"rx":{},"tx":{}}}',
                    "data-init": "@get('/events', {openWhenHidden: true, retry: 'always'})",
                },
            ),
            lang="en",
        ),
    )


def render_topbar(status: SessionStatus | None):
    if status is None:
        state = "unavailable"
        dbcs = Span("no active session", cls="menu-muted")
        settings = Span("no active session", cls="menu-muted")
        trace = "not recording"
    else:
        state = "connected"
        dbcs = (
            tuple(
                Div(
                    Span(dbc.alias, cls="tag"),
                    Span(dbc.path, cls="mono soft-wrap"),
                    cls="menu-row",
                )
                for dbc in status.dbcs
            )
            or (Span("raw-only session", cls="menu-muted"),)
        )
        settings = (
            Span("Adapter"),
            Strong(status.interface),
            Span("Channel"),
            Strong(str(status.channel)),
            Span("Bitrate"),
            Strong(str(status.bitrate)),
            Span("FD"),
            Strong("enabled" if status.fd else "classic"),
        )
        trace = status.trace_path or "not recording"

    return Section(
        Div(
            H1("agent-can"),
            Nav(
                menu("File", Button("Add DBC...", type="button", disabled=True), Button("Reconnect session...", type="button", disabled=True), Hr(), Div(Span("Loaded DBCs", cls="menu-label"), *as_tuple(dbcs), cls="menu-section")),
                menu("Trace", Button("Start trace...", type="button", disabled=True), Button("Stop trace", type="button", disabled=True), Hr(), Div(Span("Current trace", cls="menu-label"), P(trace, cls="mono soft-wrap menu-muted"), cls="menu-section"), narrow=True),
                menu("Settings", Div(*as_tuple(settings), cls="setting-grid"), narrow=True),
                cls="menu-bar",
                **{"aria-label": "monitor actions"},
            ),
            cls="brand-block",
        ),
        Div(
            Span(cls="status-dot"),
            Span(state, **{"data-text": f"$can.errorCode ? $can.errorCode : '{state}'"}),
            cls="state-badge state-ok",
            **{"data-class-state-error": "$can.error", "data-class-state-ok": "!$can.error"},
        ),
        cls="topbar",
    )


def menu(label: str, *children, narrow: bool = False):
    return Details(
        Summary(label),
        Div(*children, cls=f"menu-popover{' menu-popover-narrow' if narrow else ''}"),
        cls="menu",
        name="monitor-menu",
    )


def render_traffic():
    return Section(
        traffic_pane("rx", "RX messages", "last seen"),
        traffic_pane("tx", "TX messages", "cycle time"),
        id="traffic-layout",
        cls="traffic-layout",
    )


def traffic_pane(table: str, title: str, fourth_column: str):
    return Div(
        Div(H2(title), Span("0", **{"data-text": f"$can.{table}Count"}), cls="panel-head"),
        traffic_table(table, fourth_column),
        cls="traffic-pane",
    )


def traffic_table(table: str, fourth_column: str):
    headings = (
        ("message", "cycle time", "last seen")
        if table == "rx"
        else ("message", fourth_column)
    )
    return Div(
        Table(
            Thead(Tr(*(Th(heading) for heading in headings))),
            Tbody(id=f"{table}-body"),
            cls=f"{table}-table",
        ),
        Div(
            H3("waiting for RX traffic" if table == "rx" else "No TX messages"),
            P("Received frames will appear here." if table == "rx" else "Transmitted frames will appear here."),
            cls="empty-state",
            **{"data-show": f"$can.{table}Count == 0"},
        ),
        cls="table-shell",
    )


def render_row_group(table: str, row: TrafficRow) -> str:
    signal_colspan = "3" if table == "rx" else "2"
    return to_xml(
        render_row(table, row),
        *(render_signal_row(table, row, signal, index, signal_colspan) for index, signal in enumerate(row.signals)),
        indent=False,
    )


def render_row(table: str, row: TrafficRow):
    row_id = f"{table}-row-{row.key}"
    anchor_attrs = {"id": row_group_anchor_id(table, row)} if not row.signals else {"id": row_id}
    path = f"$can.{table}.{row.key}"
    cells = (
        (
            message_cell(table, row, path),
            Td(row.cycle_time, **{"data-text": f"{path}.cycleTime"}),
            Td(row.last_seen, **{"data-text": f"{path}.lastSeen"}),
        )
        if table == "rx"
        else (
            message_cell(table, row, path),
            Td(row.cycle_time, **{"data-text": f"{path}.cycleTime"}),
        )
    )
    return Tr(
        *cells,
        cls=f"traffic-row {table}-key-{row.key}",
        **anchor_attrs,
    )


def message_cell(table: str, row: TrafficRow, path: str):
    toggle = (
        Input(type="checkbox", cls="row-toggle", disabled=True)
        if not row.signals
        else Input(
            type="checkbox",
            cls="row-toggle",
            **{
                "aria-label": "toggle decoded signals",
                "data-bind": f"ui.{table}.{row.key}",
            },
        )
    )
    return Td(
        Div(
            toggle,
            Span(row_label(row), cls="mono strong", **{"data-text": f"{path}.label"}),
            cls="message-cell-content",
        ),
        cls="message-cell",
    )


def render_signal_row(
    table: str, row: TrafficRow, signal: TrafficSignal, index: int, colspan: str
):
    path = f"$can.{table}.{row.key}"
    attrs = (
        {"id": row_group_anchor_id(table, row)}
        if index + 1 == len(row.signals)
        else {}
    )
    return Tr(
        Td(
            Div(
                Span(signal.name, cls="signal-name"),
                Span(
                    Span(
                        signal.value_description or "",
                        cls="signal-description",
                        **{"data-text": f"{path}.signals.s{index}.description ?? ''"},
                    ),
                    Span(
                        signal.value,
                        cls="signal-number mono",
                        **{"data-text": f"{path}.signals.s{index}.value"},
                    ),
                    Span(
                        signal.unit or "",
                        cls="signal-unit",
                        **{"data-text": f"{path}.signals.s{index}.unit ?? ''"},
                    ),
                    cls="signal-value",
                ),
                cls="signal-line",
            ),
            colspan=colspan,
        ),
        cls=f"signal-row {table}-key-{row.key}",
        **{"data-show": f"$ui.{table}.{row.key}", **attrs},
    )


def row_group_anchor_id(table: str, row: TrafficRow) -> str:
    if not row.signals:
        return f"{table}-row-{row.key}"
    return f"{table}-row-end-{row.key}"


def format_arb_id(row: TrafficRow) -> str:
    return f"0x{row.arb_id:X}{' ext' if row.extended else ''}"


def row_label(row: TrafficRow) -> str:
    if row.message == format_arb_id(row):
        return f"({format_arb_id(row)})"
    return f"{row.message} ({format_arb_id(row)})"


def as_tuple(value) -> tuple:
    if isinstance(value, tuple):
        return value
    return (value,)
