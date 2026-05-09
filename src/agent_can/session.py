from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass

import can

from agent_can.backend import Backend, open_backend
from agent_can.dbc import DbcRegistry
from agent_can.protocol import (
    ConnectRequest,
    ConnectResult,
    EventDirection,
    LoadedDbc,
    MessageEntryKind,
    MessageListEntry,
    MessageListRequest,
    MessageObservation,
    MessageReadRequest,
    MessageReadResult,
    MessageSendRequest,
    MessageSendResult,
    MessageStopRequest,
    PeriodicSchedule,
    RawObservation,
    SchemaMessage,
    SchemaRequest,
    SemanticObservation,
    SessionStatus,
    TraceStartRequest,
)
from agent_can.selectors import Selector, parse_payload_hex, payload_to_hex

RETENTION_WINDOW_SECS = 60
RETENTION_EVENT_CAP = 4096
POLL_INTERVAL_SECS = 0.001


@dataclass
class ObservedEvent:
    seq: int
    unix_ms: int
    monotonic: float
    direction: EventDirection
    message: can.Message


@dataclass
class LatestObservation:
    latest_rx: ObservedEvent
    observed_count: int = 1
    cycle_time_ms: float | None = None


@dataclass
class PeriodicScheduleState:
    target: str
    message: can.Message
    periodicity_ms: int
    next_due: float


class SessionEngine:
    def __init__(self, connect: ConnectRequest, dbcs: DbcRegistry, backend: Backend) -> None:
        self.connect = connect
        self.dbcs = dbcs
        self.backend = backend
        self.events: deque[ObservedEvent] = deque()
        self.latest: dict[tuple[int, bool], LatestObservation] = {}
        self.schedules: dict[str, PeriodicScheduleState] = {}
        self.trace: can.Listener | None = None
        self.trace_path: str | None = None
        self.backend_error: str | None = None
        self.next_seq = 1
        self.shutdown = False

    def status(self) -> SessionStatus:
        return SessionStatus(
            connection_state="connected",
            interface=self.connect.interface,
            channel=self.connect.channel,
            bitrate=self.connect.bitrate,
            bitrate_data=self.connect.bitrate_data,
            fd=self.connect.fd,
            dbcs=[LoadedDbc(alias=dbc.alias, path=dbc.path) for dbc in self.connect.dbcs],
            trace_path=self.trace_path,
            periodic_schedules=[
                PeriodicSchedule(
                    target=schedule.target,
                    arb_id=schedule.message.arbitration_id,
                    extended=schedule.message.is_extended_id,
                    fd=schedule.message.is_fd,
                    len=schedule.message.dlc,
                    periodicity_ms=schedule.periodicity_ms,
                )
                for schedule in self.schedules.values()
            ],
            backend_error=self.backend_error,
            retention_window_secs=RETENTION_WINDOW_SECS,
            retention_event_cap=RETENTION_EVENT_CAP,
        )

    def tick(self) -> None:
        try:
            for message in self.backend.recv_all():
                self.record_event(message)
        except Exception as err:
            self.backend_error = str(err)
        self.tick_schedules()
        self.trim_events()

    def disconnect(self) -> None:
        self.schedules.clear()
        self.stop_trace()
        self.backend.close()
        self.shutdown = True

    def schema(self, request: SchemaRequest) -> list[SchemaMessage]:
        selector = Selector.parse(request.filter) if request.filter else None
        return self.dbcs.schema(selector)

    def message_list(self, request: MessageListRequest) -> list[MessageListEntry]:
        selector = Selector.parse(request.filter) if request.filter else None
        out: list[MessageListEntry] = []
        for latest in self.latest.values():
            event = latest.latest_rx
            matches = self.dbcs.matches_for_frame(
                event.message.arbitration_id, event.message.is_extended_id
            )
            if not matches:
                if (self.dbcs.is_empty or request.allow_raw) and (
                    selector is None or selector.matches_arb_id(event.message.arbitration_id)
                ):
                    out.append(
                        MessageListEntry(
                            label=f"0x{event.message.arbitration_id:X}",
                            kind=MessageEntryKind.RAW,
                            arb_id=event.message.arbitration_id,
                            extended=event.message.is_extended_id,
                            fd=event.message.is_fd,
                            len=event.message.dlc,
                            observed_count=latest.observed_count,
                            cycle_time_ms=latest.cycle_time_ms,
                            has_rx=True,
                            has_tx=False,
                        )
                    )
                continue
            for message in matches:
                if selector and not self.dbcs.matches_message_filter(selector, message):
                    continue
                out.append(
                    MessageListEntry(
                        label=message.qualified_name,
                        kind=MessageEntryKind.SEMANTIC,
                        arb_id=message.arb_id,
                        extended=message.extended,
                        fd=message.fd,
                        len=event.message.dlc,
                        observed_count=latest.observed_count,
                        cycle_time_ms=latest.cycle_time_ms,
                        has_rx=True,
                        has_tx=False,
                    )
                )
        return sorted(out, key=lambda item: item.label)

    def message_read(self, request: MessageReadRequest) -> MessageReadResult:
        selector = Selector.parse(request.select)
        count = request.count or 1
        observations: list[MessageObservation] = []
        if selector.raw_arb_id is not None:
            for event in reversed(self.events):
                if len(observations) >= count:
                    break
                if event.message.arbitration_id == selector.raw_arb_id:
                    observations.append(self._raw_observation(event))
        else:
            message = self.dbcs.resolve_selector(selector)
            for event in reversed(self.events):
                if len(observations) >= count:
                    break
                if (
                    event.message.arbitration_id == message.arb_id
                    and event.message.is_extended_id == message.extended
                ):
                    observations.append(
                        SemanticObservation(
                            **self._raw_observation(event).model_dump(exclude={"kind"}),
                            qualified_name=message.qualified_name,
                            signals=self.dbcs.decode(
                                message.qualified_name, bytes(event.message.data)
                            ),
                        )
                    )
        if not observations:
            raise ValueError(f"selector '{request.select}' matched no observed traffic")
        return MessageReadResult(
            selector=request.select, count=len(observations), observations=observations
        )

    def message_send(self, request: MessageSendRequest) -> MessageSendResult:
        selector = Selector.parse(request.target)
        if selector.raw_arb_id is not None:
            if not isinstance(request.data, str):
                raise ValueError("raw target requires hex string payload")
            data = parse_payload_hex(request.data)
            message = can.Message(
                arbitration_id=selector.raw_arb_id,
                is_extended_id=False,
                is_fd=len(data) > 8,
                data=data,
                timestamp=time.time(),
            )
        else:
            if not isinstance(request.data, dict):
                raise ValueError("semantic target requires signal map payload")
            message_def = self.dbcs.resolve_selector(selector)
            message = can.Message(
                arbitration_id=message_def.arb_id,
                data=self.dbcs.encode(message_def.qualified_name, request.data),
                is_extended_id=message_def.extended,
                is_fd=message_def.fd,
                timestamp=time.time(),
            )
        self.backend.send(message)
        self.trace_message(EventDirection.TX, message)
        if request.periodicity_ms is not None:
            self.schedules[request.target] = PeriodicScheduleState(
                target=request.target,
                message=message,
                periodicity_ms=request.periodicity_ms,
                next_due=time.monotonic() + (request.periodicity_ms / 1000),
            )
        return MessageSendResult(
            target=request.target,
            arb_id=message.arbitration_id,
            extended=message.is_extended_id,
            fd=message.is_fd,
            len=message.dlc,
            periodicity_ms=request.periodicity_ms,
        )

    def message_stop(self, request: MessageStopRequest) -> bool:
        return self.schedules.pop(request.target, None) is not None

    def start_trace(self, request: TraceStartRequest) -> str:
        self.stop_trace()
        self.trace = can.Logger(request.path)
        self.trace_path = request.path
        return request.path

    def stop_trace(self) -> str | None:
        if self.trace is None:
            return None
        path = self.trace_path
        self.trace.stop()
        self.trace = None
        self.trace_path = None
        return path

    def record_event(self, message: can.Message) -> None:
        received_at = time.time()
        message.timestamp = received_at
        message.is_rx = True
        event = ObservedEvent(
            seq=self.next_seq,
            unix_ms=int(received_at * 1000),
            monotonic=time.monotonic(),
            direction=EventDirection.RX,
            message=message,
        )
        self.next_seq += 1
        self.events.append(event)
        identity = (message.arbitration_id, message.is_extended_id)
        latest = self.latest.get(identity)
        if latest is None:
            latest = LatestObservation(latest_rx=event)
            self.latest[identity] = latest
        else:
            latest.observed_count += 1
            latest.cycle_time_ms = (event.monotonic - latest.latest_rx.monotonic) * 1000
        latest.latest_rx = event
        self.trace_message(EventDirection.RX, message)

    def trace_message(self, direction: EventDirection, message: can.Message) -> None:
        if self.trace:
            message.is_rx = direction == EventDirection.RX
            self.trace.on_message_received(message)

    def tick_schedules(self) -> None:
        now = time.monotonic()
        for schedule in self.schedules.values():
            if now < schedule.next_due:
                continue
            schedule.message.timestamp = time.time()
            self.backend.send(schedule.message)
            self.trace_message(EventDirection.TX, schedule.message)
            period = schedule.periodicity_ms / 1000
            while schedule.next_due <= now:
                schedule.next_due += period

    def trim_events(self) -> None:
        cutoff = time.monotonic() - RETENTION_WINDOW_SECS
        while len(self.events) > RETENTION_EVENT_CAP or (
            self.events and self.events[0].monotonic < cutoff
        ):
            self.events.popleft()

    def _raw_observation(self, event: ObservedEvent) -> RawObservation:
        return RawObservation(
            seq=event.seq,
            direction=event.direction,
            unix_ms=event.unix_ms,
            arb_id=event.message.arbitration_id,
            extended=event.message.is_extended_id,
            fd=event.message.is_fd,
            len=event.message.dlc,
            payload_hex=payload_to_hex(bytes(event.message.data)),
        )


class SessionManager:
    def __init__(self) -> None:
        self._engine: SessionEngine | None = None
        self._connect: ConnectRequest | None = None
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def connect(self, request: ConnectRequest) -> ConnectResult:
        async with self._lock:
            if self._engine is not None:
                if self._connect == request:
                    return ConnectResult(
                        created=False,
                        already_connected=True,
                        status=self._engine.status(),
                    )
                raise ValueError("session already connected; disconnect first")
            dbcs = DbcRegistry(request.dbcs)
            backend = open_backend(request)
            self._engine = SessionEngine(request, dbcs, backend)
            self._connect = request
            self._task = asyncio.create_task(self._run())
            return ConnectResult(
                created=True, already_connected=False, status=self._engine.status()
            )

    async def disconnect(self) -> bool:
        async with self._lock:
            if self._engine is None:
                raise ValueError("no active session; connect first")
            self._engine.disconnect()
            self._engine = None
            self._connect = None
            task = self._task
            self._task = None
        if task:
            await task
        return True

    async def engine(self) -> SessionEngine:
        if self._engine is None:
            raise ValueError("no active session; connect first")
        return self._engine

    async def _run(self) -> None:
        while self._engine is not None and not self._engine.shutdown:
            self._engine.tick()
            await asyncio.sleep(POLL_INTERVAL_SECS)
