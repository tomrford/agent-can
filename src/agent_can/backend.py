from __future__ import annotations

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


def open_backend(request: ConnectRequest) -> Backend:
    return PythonCanBackend.open(request)


def available_buses() -> list[BusInfo]:
    buses = [
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
