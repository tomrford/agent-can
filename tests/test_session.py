from __future__ import annotations

import asyncio
import time
from pathlib import Path

from agent_can.protocol import (
    ConnectRequest,
    DbcSpec,
    MessageListRequest,
    MessageReadRequest,
    MessageSendRequest,
    SchemaRequest,
)
from agent_can.session import SessionManager


def demo_dbc_path() -> str:
    return str((Path(__file__).parents[1] / "examples" / "demo.dbc").resolve())


def test_demo_session_schema_and_raw_send() -> None:
    async def run() -> None:
        manager = SessionManager()
        result = await manager.connect(
            ConnectRequest(
                interface="demo",
                channel="demo",
                bitrate=500_000,
                fd=False,
                dbcs=[DbcSpec(alias="demo", path=demo_dbc_path())],
            )
        )
        assert result.created is True

        engine = await manager.engine()
        messages = engine.schema(SchemaRequest())
        assert [message.qualified_name for message in messages] == [
            "demo.BodyStatus",
            "demo.Heartbeat",
            "demo.PowertrainStatus",
        ]

        sent = engine.message_send(MessageSendRequest(target="0x123", data="DE AD BE EF"))
        assert sent.arb_id == 0x123
        assert sent.len == 4

        listed = engine.message_list(MessageListRequest(allow_raw=True, include_tx=True))
        assert any(message.label == "0x123" for message in listed)

        raw_filtered = engine.message_list(
            MessageListRequest(filter="0x123", allow_raw=True, include_tx=True)
        )
        assert [message.label for message in raw_filtered] == ["0x123"]

        read = engine.message_read(MessageReadRequest(select="0x123", include_tx=True))
        assert read.observations[0].payload_hex == "DEADBEEF"
        await manager.disconnect()

    asyncio.run(run())


def test_demo_session_semantic_send_and_decode() -> None:
    async def run() -> None:
        manager = SessionManager()
        await manager.connect(
            ConnectRequest(
                interface="demo",
                channel="demo",
                bitrate=500_000,
                fd=False,
                dbcs=[DbcSpec(alias="demo", path=demo_dbc_path())],
            )
        )
        engine = await manager.engine()
        engine.message_send(
            MessageSendRequest(
                target="demo.PowertrainStatus",
                data={
                    "vehicle_speed": 12.3,
                    "engine_rpm": 1500,
                    "throttle": 20,
                    "coolant_temp": 90,
                },
            )
        )
        read = engine.message_read(
            MessageReadRequest(select="demo.PowertrainStatus", include_tx=True)
        )
        values = {signal.name: signal.value for signal in read.observations[0].signals}
        assert values["engine_rpm"] == 1500
        assert values["coolant_temp"] == 90
        await manager.disconnect()

    asyncio.run(run())


def test_message_list_filter_and_timestamps_use_wall_clock() -> None:
    async def run() -> None:
        manager = SessionManager()
        await manager.connect(
            ConnectRequest(
                interface="demo",
                channel="demo",
                bitrate=500_000,
                fd=False,
                dbcs=[DbcSpec(alias="demo", path=demo_dbc_path())],
            )
        )
        engine = await manager.engine()
        before_ms = int(time.time() * 1000)
        engine.message_send(
            MessageSendRequest(
                target="demo.PowertrainStatus",
                data={
                    "vehicle_speed": 12.3,
                    "engine_rpm": 1500,
                    "throttle": 20,
                    "coolant_temp": 90,
                },
            )
        )
        after_ms = int(time.time() * 1000)

        listed = engine.message_list(
            MessageListRequest(filter="demo.PowertrainStatus", include_tx=True)
        )
        assert [message.label for message in listed] == ["demo.PowertrainStatus"]
        assert before_ms <= listed[0].last_seen_unix_ms <= after_ms

        no_match = engine.message_list(
            MessageListRequest(filter="demo.BodyStatus", include_tx=True)
        )
        assert no_match == []
        await manager.disconnect()

    asyncio.run(run())
