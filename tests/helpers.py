import asyncio
import io
import logging
import socket
import typing

logger = logging.getLogger('telegraf_server')


class TelegrafServer(asyncio.DatagramProtocol, asyncio.Protocol):
    metrics: typing.List[bytes]

    def __init__(self, ip_protocol):
        self.server = None
        self.host = '127.0.0.1'
        self.port = 8125
        self.ip_protocol = ip_protocol
        self.connections_made = 0
        self.connections_lost = 0
        self.message_counter = 0

        self.metrics = []
        self.running = asyncio.Event()
        self.client_connected = asyncio.Semaphore(value=0)
        self.message_received = asyncio.Semaphore(value=0)
        self.transports: typing.List[asyncio.BaseTransport] = []

        self._buffer = io.BytesIO()

    async def run(self):
        logger.debug('starting server...')
        await self._reset()

        loop = asyncio.get_running_loop()
        if self.ip_protocol == socket.IPPROTO_TCP:
            server = await loop.create_server(lambda: self, self.host, self.port, reuse_port=True)
            self.server = server
            listening_sock = typing.cast(typing.List[socket.socket], server.sockets)[0]
            self.host, self.port = listening_sock.getsockname()
            self.running.set()
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                self.close()
                await server.wait_closed()
            except Exception as error:
                raise error
            finally:
                self.running.clear()

        elif self.ip_protocol == socket.IPPROTO_UDP:
            transport, protocol = await loop.create_datagram_endpoint(
                lambda: self,
                local_addr=(self.host, self.port),
                reuse_port=True)
            self.server = transport
            self.host, self.port = transport.get_extra_info('sockname')
            self.running.set()
            try:
                while not transport.is_closing():
                    await asyncio.sleep(0.1)
            finally:
                self.running.clear()

    def close(self):
        if self.server is not None:
            self.server.close()
        for connected_client in self.transports:
            connected_client.close()
        self.transports.clear()

    async def wait_running(self):
        await self.running.wait()

    async def wait_closed(self):
        while self.running.is_set():
            await asyncio.sleep(0.1)

    def connection_made(self, transport: asyncio.BaseTransport):
        logger.debug('connection made')
        self.client_connected.release()
        self.connections_made += 1
        self.transports.append(transport)

    def connection_lost(self, exc) -> None:
        self.connections_lost += 1

    def data_received(self, data: bytes):
        logger.debug('data received')
        self._buffer.write(data)
        self._process_buffer()

    def datagram_received(self, data: bytes, _addr):
        logger.debug('datagram received')
        self._buffer.write(data + b'\n')
        self._process_buffer()

    def _process_buffer(self):
        buf = self._buffer.getvalue()
        if b'\n' in buf:
            buf_complete = buf[-1] == ord('\n')
            if not buf_complete:
                offset = buf.rfind(b'\n')
                self._buffer = io.BytesIO(buf[offset:])
                buf = buf[:offset]
            else:
                self._buffer = io.BytesIO()
                buf = buf[:-1]

            for metric in buf.split(b'\n'):
                self.metrics.append(metric)
                self.message_received.release()
                self.message_counter += 1

    async def _reset(self):
        self._buffer = io.BytesIO()
        self.connections_made = 0
        self.connections_lost = 0
        self.message_counter = 0
        self.metrics.clear()
        for transport in self.transports:
            transport.close()
        self.transports.clear()

        self.running.clear()
        await self._drain_semaphore(self.client_connected)
        await self._drain_semaphore(self.message_received)

    @staticmethod
    async def _drain_semaphore(semaphore: asyncio.Semaphore):
        while not semaphore.locked():
            try:
                await asyncio.wait_for(semaphore.acquire(), 0.1)
            except asyncio.TimeoutError:
                break


async def main():
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    telegraf_server = TelegrafServer(ip_protocol=socket.IPPROTO_UDP)
    try:
        logger.info('Starting server')
        await telegraf_server.run()
    finally:
        telegraf_server.close()
        await telegraf_server.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
