import asyncio
import logging
import typing as t

from asynctelegraf.utils import LimitedLog

logger: logging.Logger = t.cast(logging.Logger, LimitedLog(logger_src=logging.getLogger(__name__)))


class TelegrafProtocol(asyncio.BaseProtocol):
    def __init__(self):
        self.connected = asyncio.Event()
        self.transport = None

    def send(self, metric: bytes):
        raise NotImplementedError()

    async def shutdown(self):
        raise NotImplementedError()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport
        self.connected.set()

    def connection_lost(self, exc: t.Optional[Exception]) -> None:
        self.connected.clear()


class TCPProtocol(TelegrafProtocol, asyncio.Protocol):
    def eof_received(self) -> None:
        logger.warning('Received EOF from telegraf server')
        self.connected.clear()

    def send(self, metric: bytes) -> None:
        self.transport.write(metric + b'\n')

    async def shutdown(self) -> None:
        if self.connected.is_set():
            self.send(b'')  # flush
            self.transport.close()
            while self.connected.is_set():
                await asyncio.sleep(0.1)


class UDPProtocol(TelegrafProtocol, asyncio.DatagramProtocol):
    def send(self, metric: bytes) -> None:
        self.transport.sendto(metric)

    async def shutdown(self) -> None:
        self.transport.close()
