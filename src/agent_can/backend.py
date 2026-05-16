from __future__ import annotations

import time
from typing import Protocol

import can

from agent_can.protocol import BusInfo, ConnectRequest


class Backend(Protocol):
    def recv_all(self) -> list[can.Message]: ...
    def send(self, message: can.Message) -> None: ...
    def close(self) -> None: ...


class PythonCanBackend:
    def __init__(self, bus: can.BusABC) -> None:
        self.bus = bus

    @classmethod
    def open(cls, request: ConnectRequest) -> "PythonCanBackend":
        bus = can.Bus(
            interface=request.interface,
            channel=request.channel,
            bitrate=request.bitrate,
            fd=request.fd,
            data_bitrate=request.bitrate_data,
        )
        return cls(bus)

    def recv_all(self) -> list[can.Message]:
        messages = []
        while True:
            message = self.bus.recv(timeout=0)
            if message is None:
                break
            messages.append(message)
        return messages

    def send(self, message: can.Message) -> None:
        self.bus.send(message)

    def close(self) -> None:
        self.bus.shutdown()


class DemoBackend:
    def __init__(self) -> None:
        self._next_due = time.monotonic()
        self._tick = 0
        self._sent: list[can.Message] = []

    def recv_all(self) -> list[can.Message]:
        now = time.monotonic()
        if now < self._next_due:
            return []
        messages = []
        while self._next_due <= now and len(messages) < 32:
            self._tick += 1
            speed = self._tick % 250
            rpm = 800 + (self._tick * 40) % 5200
            throttle = (self._tick * 3) % 100
            coolant = 70 + (self._tick // 20) % 25
            speed_raw = int(speed * 10)
            throttle_raw = int(throttle / 0.5)
            coolant_raw = coolant + 40
            messages.append(
                can.Message(
                    arbitration_id=0x120,
                    is_extended_id=False,
                    data=[
                        speed_raw & 0xFF,
                        speed_raw >> 8,
                        rpm & 0xFF,
                        rpm >> 8,
                        throttle_raw,
                        coolant_raw,
                        0,
                        0,
                    ],
                    timestamp=time.time(),
                )
            )
            if self._tick % 8 == 0:
                messages.append(
                    can.Message(
                        arbitration_id=0x130,
                        is_extended_id=False,
                        data=[(self._tick // 8) % 2, (self._tick // 13) % 2, 120],
                        timestamp=time.time(),
                    )
                )
            self._next_due += 0.001
        return messages

    def send(self, message: can.Message) -> None:
        self._sent.append(message)

    def close(self) -> None:
        return


def open_backend(request: ConnectRequest) -> Backend:
    if request.interface == "demo":
        return DemoBackend()
    return PythonCanBackend.open(request)


def available_buses() -> list[BusInfo]:
    buses = [
        BusInfo(
            interface="demo",
            channel="agent-can-demo",
            name="temporary demo traffic",
            device_name="agent-can browser test backend",
        ),
        BusInfo(
            interface="virtual",
            channel="agent-can",
            name="virtual agent-can",
            device_name="python-can virtual bus",
        ),
    ]
    for interface in ("pcan", "socketcan"):
        try:
            configs = can.detect_available_configs(interfaces=[interface])
        except Exception:
            configs = []
        for config in configs:
            channel = str(config.get("channel", ""))
            if channel:
                buses.append(
                    BusInfo(
                        interface=interface,
                        channel=channel,
                        name=f"{interface} {channel}",
                        device_name=channel,
                    )
                )
    return buses
