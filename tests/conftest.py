import asyncio
import socket
from contextlib import asynccontextmanager

import pytest

from asynctelegraf import TelegrafClient
from asynctelegraf.client import MetricSender
from tests import helpers

pytestmark = pytest.mark.asyncio


@pytest.fixture
def loop(event_loop):
    return event_loop


@asynccontextmanager
async def get_telegraf_server(protocol):
    telegraf_server = helpers.TelegrafServer(ip_protocol=protocol)
    asyncio.create_task(telegraf_server.run())
    await telegraf_server.wait_running()

    yield telegraf_server

    telegraf_server.close()
    await telegraf_server.wait_closed()


@asynccontextmanager
async def get_telegraf_client(telegraf_server: helpers.TelegrafServer):
    connector = TelegrafClient(telegraf_server.host, telegraf_server.port, protocol=telegraf_server.ip_protocol)
    await connector.start()
    await wait_for(telegraf_server.client_connected.acquire())

    yield connector

    await wait_for(connector.stop())


@pytest.fixture
async def telegraf_tcp_server():
    async with get_telegraf_server(protocol=socket.IPPROTO_TCP) as telegraf_server:
        yield telegraf_server


@pytest.fixture
async def telegraf_tcp(telegraf_tcp_server):
    async with get_telegraf_client(telegraf_server=telegraf_tcp_server) as connector:
        yield connector


@pytest.fixture
async def telegraf_udp_server():
    async with get_telegraf_server(protocol=socket.IPPROTO_UDP) as telegraf_server:
        yield telegraf_server


@pytest.fixture
async def telegraf_udp(telegraf_udp_server):
    async with get_telegraf_client(telegraf_server=telegraf_udp_server) as connector:
        yield connector


@pytest.fixture(params=[socket.IPPROTO_UDP, socket.IPPROTO_TCP], ids=["UDP", "TCP"])
async def telegraf_server(request):
    async with get_telegraf_server(protocol=request.param) as telegraf_server:
        yield telegraf_server


@pytest.fixture(params=[socket.IPPROTO_UDP, socket.IPPROTO_TCP], ids=["UDP", "TCP"])
async def telegraf_server_client(request):
    async with get_telegraf_server(protocol=request.param) as telegraf_server:
        async with get_telegraf_client(telegraf_server=telegraf_server) as telegraf_client:
            yield telegraf_server, telegraf_client


@pytest.fixture
async def sender_tcp(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())

    yield sender

    await sender.stop()


async def wait_for(coro):
    try:
        await asyncio.wait_for(coro, timeout=5)
    except asyncio.TimeoutError:
        pytest.fail('timeout for coro')
