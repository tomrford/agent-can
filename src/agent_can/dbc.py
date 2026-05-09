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
        self._messages: list[MessageDef] = []
        self._messages_by_qualified_name: dict[str, MessageDef] = {}
        self._by_frame: dict[tuple[int, bool], list[MessageDef]] = {}
        for spec in specs:
            db = cantools.database.load_file(spec.path)
            for message in db.messages:
                message_def = MessageDef(spec.alias, db, message)
                self._messages.append(message_def)
                self._messages_by_qualified_name[message_def.qualified_name] = message_def
                self._by_frame.setdefault((message_def.arb_id, message_def.extended), []).append(
                    message_def
                )

    @property
    def is_empty(self) -> bool:
        return not self._messages

    def schema(self, selector: Selector | None) -> list[SchemaMessage]:
        messages = [
            self._to_schema_message(message)
            for message in self._messages
            if selector is None or self.matches_message_filter(selector, message)
        ]
        return sorted(messages, key=lambda item: item.qualified_name)

    def matches_message_filter(self, selector: Selector, message: MessageDef) -> bool:
        return selector.matches_arb_id(message.arb_id) or selector.matches_any_name_filter(
            message.qualified_name, message.alias, message.message.name
        )

    def matches_for_frame(self, arb_id: int, extended: bool) -> list[MessageDef]:
        return self._by_frame.get((arb_id, extended), [])

    def resolve_selector(self, selector: Selector) -> MessageDef:
        if selector.raw_arb_id is not None:
            matches = [
                message for message in self._messages if selector.matches_arb_id(message.arb_id)
            ]
        elif selector.semantic_pattern is not None:
            try:
                return self._messages_by_qualified_name[selector.semantic_pattern]
            except KeyError:
                raise ValueError("selector matched no DBC messages") from None
        else:
            matches = []
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
        try:
            return self._messages_by_qualified_name[qualified_name]
        except KeyError as err:
            raise ValueError(f"unknown DBC message '{qualified_name}'") from err

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
