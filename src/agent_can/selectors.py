from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase


@dataclass(frozen=True)
class Selector:
    raw_arb_id: int | None = None
    semantic_pattern: str | None = None
    normalized_pattern: str | None = None

    @classmethod
    def parse(cls, raw: str) -> "Selector":
        value = raw.strip()
        if not value:
            raise ValueError("selector must not be empty")
        if value.lower().startswith("0x"):
            try:
                return cls(raw_arb_id=int(value[2:], 16))
            except ValueError as err:
                raise ValueError(f"invalid raw arbitration selector '{raw}'") from err
        return cls(semantic_pattern=value, normalized_pattern=value.casefold())

    def matches_name_filter(self, value: str) -> bool:
        if self.normalized_pattern is None:
            return False
        candidate = value.casefold()
        return (
            fnmatchcase(candidate, self.normalized_pattern) or self.normalized_pattern in candidate
        )

    def matches_any_name_filter(self, *values: str) -> bool:
        return any(self.matches_name_filter(value) for value in values)

    def matches_exact_name(self, value: str) -> bool:
        return self.semantic_pattern == value

    def matches_arb_id(self, value: int) -> bool:
        return self.raw_arb_id == value


def payload_to_hex(data: bytes) -> str:
    return data.hex().upper()


def parse_payload_hex(value: str) -> bytes:
    compact = "".join(value.split())
    if len(compact) % 2:
        raise ValueError("raw hex payload must contain an even number of hex digits")
    try:
        return bytes.fromhex(compact)
    except ValueError as err:
        raise ValueError(f"invalid raw hex payload '{value}'") from err
