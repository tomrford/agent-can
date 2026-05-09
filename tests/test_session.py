from __future__ import annotations

import asyncio
from pathlib import Path

from agent_can_py.protocol import (
    ConnectRequest,
    DbcSpec,
    MessageListRequest,
    MessageReadRequest,
    MessageSendRequest,
    SchemaRequest,
)
from agent_can_py.session import SessionManager


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
