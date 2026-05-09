from __future__ import annotations

from dataclasses import dataclass

import cantools.database
from cantools.database.can import Database, Message

from agent_can.protocol import (
    DbcSpec,
    DecodedSignalValue,
    SchemaMessage,
    SchemaSignal,
)
from agent_can.selectors import Selector


@dataclass(frozen=True)
class MessageDef:
    alias: str
    db: Database
    message: Message

    @property
    def qualified_name(self) -> str:
        return f"{self.alias}.{self.message.name}"

    @property
    def arb_id(self) -> int:
        return int(self.message.frame_id)

    @property
    def extended(self) -> bool:
        return bool(self.message.is_extended_frame)

    @property
    def fd(self) -> bool:
        return self.message.length > 8

    @property
    def len(self) -> int:
        return int(self.message.length)


class DbcRegistry:
    def __init__(self, specs: list[DbcSpec]) -> None:
        self.specs = specs
        self._messages: list[MessageDef] = []
        for spec in specs:
            db = cantools.database.load_file(spec.path)
            for message in db.messages:
                self._messages.append(MessageDef(spec.alias, db, message))

    @property
    def is_empty(self) -> bool:
        return not self._messages

    def schema(self, selector: Selector | None) -> list[SchemaMessage]:
        messages = [
            self._to_schema_message(message)
            for message in self._messages
            if selector is None
            or selector.matches_qualified_name(message.qualified_name)
            or selector.matches_arb_id(message.arb_id)
        ]
        return sorted(messages, key=lambda item: item.qualified_name)

    def matches_for_frame(self, arb_id: int, extended: bool) -> list[MessageDef]:
        return [
            message
            for message in self._messages
            if message.arb_id == arb_id and message.extended == extended
        ]

    def resolve_selector(self, selector: Selector) -> MessageDef:
        if selector.raw_arb_id is not None:
            matches = [
                message for message in self._messages if message.arb_id == selector.raw_arb_id
            ]
        else:
            matches = [
                message
                for message in self._messages
                if selector.matches_qualified_name(message.qualified_name)
            ]
        if not matches:
            raise ValueError("selector matched no DBC messages")
        if len(matches) > 1:
            names = ", ".join(message.qualified_name for message in matches)
            raise ValueError(f"selector matched multiple DBC messages: {names}")
        return matches[0]

    def encode(self, qualified_name: str, signals: dict[str, float]) -> bytes:
        message = self._by_qualified_name(qualified_name)
        return bytes(message.message.encode(signals, strict=True))

    def decode(self, qualified_name: str, data: bytes) -> list[DecodedSignalValue]:
        message = self._by_qualified_name(qualified_name)
        decoded = message.message.decode(data, decode_choices=False)
        out = []
        for signal in message.message.signals:
            value = decoded[signal.name]
            choice = None
            if signal.choices:
                choice = signal.choices.get(int(value))
            out.append(
                DecodedSignalValue(
                    name=signal.name,
                    value=float(value),
                    unit=signal.unit,
                    value_description=choice,
                )
            )
        return out

    def _by_qualified_name(self, qualified_name: str) -> MessageDef:
        for message in self._messages:
            if message.qualified_name == qualified_name:
                return message
        raise ValueError(f"unknown DBC message '{qualified_name}'")

    def _to_schema_message(self, message: MessageDef) -> SchemaMessage:
        return SchemaMessage(
            qualified_name=message.qualified_name,
            alias=message.alias,
            message=message.message.name,
            arb_id=message.arb_id,
            extended=message.extended,
            fd=message.fd,
            len=message.len,
            signals=[
                SchemaSignal(
                    name=signal.name,
                    value_type="number",
                    unit=signal.unit,
                    value_descriptions={int(k): v for k, v in (signal.choices or {}).items()},
                    min=signal.minimum,
                    max=signal.maximum,
                    factor=float(signal.conversion.scale),
                    offset=float(signal.conversion.offset),
                    start_bit=int(signal.start),
                    bit_len=int(signal.length),
                )
                for signal in message.message.signals
            ],
        )
