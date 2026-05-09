from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator


class DbcSpec(BaseModel):
    alias: str
    path: str

    @field_validator("path")
    @classmethod
    def path_must_be_absolute(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            raise ValueError(f"DBC path '{value}' must be absolute")
        if not path.exists():
            raise ValueError(f"DBC path '{value}' does not exist")
        return str(path.resolve())


class ConnectRequest(BaseModel):
    interface: str
    channel: str | int
    bitrate: int
    bitrate_data: int | None = None
    fd: bool = False
    dbcs: list[DbcSpec] = Field(default_factory=list)


class SchemaRequest(BaseModel):
    filter: str | None = None


class MessageListRequest(BaseModel):
    filter: str | None = None
    allow_raw: bool = False
    include_tx: bool = False


class MessageReadRequest(BaseModel):
    select: str
    count: int | None = None
    include_tx: bool = False


class MessageSendRequest(BaseModel):
    target: str
    data: str | dict[str, float]
    periodicity_ms: int | None = None


class MessageStopRequest(BaseModel):
    target: str


class TraceStartRequest(BaseModel):
    path: str

    @field_validator("path")
    @classmethod
    def path_must_be_absolute(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            raise ValueError(f"trace path '{value}' must be absolute")
        parent = path.parent
        if not parent.exists():
            raise ValueError(f"trace parent '{parent}' does not exist")
        return str(parent.resolve() / path.name)


class BusInfo(BaseModel):
    interface: str
    channel: str | int
    name: str | None = None
    device_name: str | None = None


class LoadedDbc(BaseModel):
    alias: str
    path: str


class PeriodicSchedule(BaseModel):
    target: str
    arb_id: int
    extended: bool
    fd: bool
    len: int
    periodicity_ms: int


class SessionStatus(BaseModel):
    connection_state: str
    interface: str
    channel: str | int
    bitrate: int
    bitrate_data: int | None
    fd: bool
    dbcs: list[LoadedDbc]
    trace_path: str | None
    periodic_schedules: list[PeriodicSchedule]
    backend_error: str | None
    retention_window_secs: int
    retention_event_cap: int


class ConnectResult(BaseModel):
    created: bool
    already_connected: bool
    status: SessionStatus


class SchemaSignal(BaseModel):
    name: str
    value_type: str
    unit: str | None
    value_descriptions: dict[int, str] = Field(default_factory=dict)
    min: float | None
    max: float | None
    factor: float
    offset: float
    start_bit: int
    bit_len: int


class SchemaMessage(BaseModel):
    qualified_name: str
    alias: str
    message: str
    arb_id: int
    extended: bool
    fd: bool
    len: int
    signals: list[SchemaSignal]


class MessageEntryKind(StrEnum):
    RAW = "raw"
    SEMANTIC = "semantic"


class MessageListEntry(BaseModel):
    label: str
    kind: MessageEntryKind
    arb_id: int
    extended: bool
    fd: bool
    len: int
    last_seen_unix_ms: int
    has_rx: bool
    has_tx: bool


class EventDirection(StrEnum):
    RX = "rx"
    TX = "tx"


class DecodedSignalValue(BaseModel):
    name: str
    value: float
    unit: str | None
    value_description: str | None = None


class RawObservation(BaseModel):
    kind: Literal["raw"] = "raw"
    seq: int
    direction: EventDirection
    unix_ms: int
    arb_id: int
    extended: bool
    fd: bool
    len: int
    payload_hex: str


class SemanticObservation(BaseModel):
    kind: Literal["semantic"] = "semantic"
    seq: int
    direction: EventDirection
    unix_ms: int
    qualified_name: str
    arb_id: int
    extended: bool
    fd: bool
    len: int
    payload_hex: str
    signals: list[DecodedSignalValue]


MessageObservation = Annotated[RawObservation | SemanticObservation, Field(discriminator="kind")]


class MessageReadResult(BaseModel):
    selector: str
    count: int
    observations: list[MessageObservation]


class MessageSendResult(BaseModel):
    target: str
    arb_id: int
    extended: bool
    fd: bool
    len: int
    periodicity_ms: int | None
