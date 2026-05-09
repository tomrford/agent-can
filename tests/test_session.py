from __future__ import annotations

from pathlib import Path
import time

import can
import pytest

from agent_can.dbc import DbcRegistry
from agent_can.protocol import (
    ConnectRequest,
    DbcSpec,
    MessageListRequest,
    MessageReadRequest,
    MessageSendRequest,
    SchemaRequest,
)
from agent_can.session import SessionEngine
from agent_can import server


class FakeBackend:
    def __init__(self) -> None:
        self.sent: list[can.Message] = []
        self.fail_send = False

    def recv_all(self) -> list[can.Message]:
        return []

    def send(self, message: can.Message) -> None:
        if self.fail_send:
            raise RuntimeError("send failed")
        self.sent.append(message)

    def close(self) -> None:
        return


def demo_dbc_path() -> str:
    return str((Path(__file__).parents[1] / "examples" / "demo.dbc").resolve())


def choice_dbc_path(tmp_path: Path) -> str:
    path = tmp_path / "choices.dbc"
    path.write_text(
        """VERSION ""
NS_ :
  VAL_
BS_:
BU_: Agent Dashboard

BO_ 512 ModeStatus: 1 Agent
 SG_ mode : 0|8@1+ (1,0) [0|2] "" Dashboard

VAL_ 512 mode 0 "Off" 1 "On";
"""
    )
    return str(path)


def make_engine() -> tuple[SessionEngine, FakeBackend]:
    request = ConnectRequest(
        interface="virtual",
        channel="agent-can",
        bitrate=500_000,
        fd=False,
        dbcs=[DbcSpec(alias="demo", path=demo_dbc_path())],
    )
    backend = FakeBackend()
    return SessionEngine(request, DbcRegistry(request.dbcs), backend), backend


def powertrain_payload() -> dict[str, float]:
    return {
        "vehicle_speed": 12.3,
        "engine_rpm": 1500,
        "throttle": 20,
        "coolant_temp": 90,
    }


def record_powertrain_rx(engine: SessionEngine) -> None:
    sent = engine.message_send(
        MessageSendRequest(target="demo.PowertrainStatus", data=powertrain_payload())
    )
    engine.record_event(
        can.Message(
            arbitration_id=sent.arb_id,
            is_extended_id=sent.extended,
            is_fd=sent.fd,
            data=engine.backend.sent[-1].data,
        )
    )


def test_schema_serializes_named_value_descriptions(tmp_path: Path) -> None:
    request = ConnectRequest(
        interface="virtual",
        channel="agent-can",
        bitrate=500_000,
        dbcs=[DbcSpec(alias="choices", path=choice_dbc_path(tmp_path))],
    )
    engine = SessionEngine(request, DbcRegistry(request.dbcs), FakeBackend())

    messages = engine.schema(SchemaRequest())

    assert messages[0].signals[0].value_descriptions == {0: "Off", 1: "On"}


def test_semantic_read_serializes_named_value_description(tmp_path: Path) -> None:
    request = ConnectRequest(
        interface="virtual",
        channel="agent-can",
        bitrate=500_000,
        dbcs=[DbcSpec(alias="choices", path=choice_dbc_path(tmp_path))],
    )
    engine = SessionEngine(request, DbcRegistry(request.dbcs), FakeBackend())
    engine.record_event(
        can.Message(arbitration_id=0x200, is_extended_id=False, data=bytearray([1]))
    )

    read = engine.message_read(MessageReadRequest(select="choices.ModeStatus"))

    assert read.observations[0].signals[0].value_description == "On"


def test_schema_and_raw_send() -> None:
    engine, backend = make_engine()

    messages = engine.schema(SchemaRequest())
    assert [message.qualified_name for message in messages] == [
        "demo.BodyStatus",
        "demo.Heartbeat",
        "demo.PowertrainStatus",
    ]

    sent = engine.message_send(MessageSendRequest(target="0x123", data="DE AD BE EF"))
    assert sent.arb_id == 0x123
    assert sent.len == 4
    assert backend.sent[-1].data == bytearray.fromhex("DE AD BE EF")


def test_extended_fd_raw_send() -> None:
    engine, backend = make_engine()
    payload = " ".join(["AA"] * 12)

    sent = engine.message_send(
        MessageSendRequest(target="0x18DAF110", data=payload, extended=True, fd=True)
    )

    assert sent.arb_id == 0x18DAF110
    assert sent.extended is True
    assert sent.fd is True
    assert sent.len == 12
    assert backend.sent[-1].is_extended_id is True
    assert backend.sent[-1].is_fd is True
    assert backend.sent[-1].data == bytearray.fromhex(payload)


def test_raw_send_validates_id_range_and_payload_len() -> None:
    engine, _backend = make_engine()

    with pytest.raises(ValueError, match="standard arbitration ID out of range"):
        engine.message_send(MessageSendRequest(target="0x800", data="00"))

    with pytest.raises(ValueError, match="extended arbitration ID out of range"):
        engine.message_send(MessageSendRequest(target="0x20000000", data="00", extended=True))

    with pytest.raises(ValueError, match="classic CAN payload"):
        engine.message_send(MessageSendRequest(target="0x123", data=" ".join(["00"] * 9)))

    with pytest.raises(ValueError, match="CAN FD payload"):
        engine.message_send(
            MessageSendRequest(
                target="0x123",
                data=" ".join(["00"] * 65),
                fd=True,
            )
        )


def test_semantic_send_rejects_raw_options() -> None:
    engine, _backend = make_engine()

    with pytest.raises(ValueError, match="only valid for raw targets"):
        engine.message_send(
            MessageSendRequest(
                target="demo.PowertrainStatus",
                data=powertrain_payload(),
                extended=True,
            )
        )


def test_semantic_read_decodes_rx_only() -> None:
    engine, _backend = make_engine()
    record_powertrain_rx(engine)

    read = engine.message_read(MessageReadRequest(select="demo.PowertrainStatus"))
    values = {signal.name: signal.value for signal in read.observations[0].signals}
    assert values["engine_rpm"] == 1500
    assert values["coolant_temp"] == 90


def test_semantic_read_requires_exact_target_name() -> None:
    engine, _backend = make_engine()
    record_powertrain_rx(engine)

    with pytest.raises(ValueError, match="selector matched no DBC messages"):
        engine.message_read(MessageReadRequest(select="PowerTrain"))


def test_message_list_filter_matches_partial_names_case_insensitively() -> None:
    engine, _backend = make_engine()
    record_powertrain_rx(engine)

    labels_by_filter = {}
    for filter_value in ("PowerTrain", "powertrain", "demo", "0x120"):
        listed = engine.message_list(MessageListRequest(filter=filter_value))
        labels_by_filter[filter_value] = [message.label for message in listed]

    assert labels_by_filter == {
        "PowerTrain": ["demo.PowertrainStatus"],
        "powertrain": ["demo.PowertrainStatus"],
        "demo": ["demo.PowertrainStatus"],
        "0x120": ["demo.PowertrainStatus"],
    }


def test_periodic_send_requires_positive_periodicity() -> None:
    with pytest.raises(ValueError):
        MessageSendRequest(target="0x123", data="00", periodicity_ms=0)

    with pytest.raises(ValueError):
        MessageSendRequest(target="0x123", data="00", periodicity_ms=-1)


def test_message_read_requires_positive_count() -> None:
    with pytest.raises(ValueError):
        MessageReadRequest(select="0x123", count=0)


def test_periodic_send_failure_is_reported_and_schedule_removed() -> None:
    engine, backend = make_engine()
    engine.message_send(MessageSendRequest(target="0x123", data="00", periodicity_ms=1))
    schedule = engine.schedules["0x123"]
    schedule.next_due = 0
    backend.fail_send = True

    engine.tick()

    assert engine.backend_error == "send failed"
    assert engine.schedules == {}


def test_trim_events_prunes_latest_observations() -> None:
    engine, _backend = make_engine()
    old_message = can.Message(arbitration_id=0x100, is_extended_id=False, data=[0])
    new_message = can.Message(arbitration_id=0x101, is_extended_id=False, data=[1])
    engine.record_event(old_message)
    engine.record_event(new_message)

    engine.events[0].monotonic = time.monotonic() - 120
    engine.trim_events()

    assert [event.message.arbitration_id for event in engine.events] == [0x101]
    assert list(engine.latest) == [(0x101, False)]


@pytest.mark.anyio
async def test_mcp_message_send_forwards_raw_frame_options(
    monkeypatch: pytest.MonkeyPatch, anyio_backend: str
) -> None:
    assert anyio_backend == "asyncio"
    engine, backend = make_engine()

    class FakeSessions:
        async def engine(self) -> SessionEngine:
            return engine

    monkeypatch.setattr(server, "sessions", FakeSessions())

    result = await server.message_send(
        target="0x18DAF110",
        data="AA BB CC DD EE FF 00 11 22",
        extended=True,
        fd=True,
    )

    assert result["arb_id"] == 0x18DAF110
    assert result["extended"] is True
    assert result["fd"] is True
    assert backend.sent[-1].is_extended_id is True
    assert backend.sent[-1].is_fd is True
