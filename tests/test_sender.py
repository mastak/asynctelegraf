import asyncio
import time

import pytest

from asynctelegraf.client import MetricSender
from tests.conftest import wait_for

pytestmark = pytest.mark.asyncio


async def test_metric_sender_connects_and_disconnects(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())
    await wait_for(sender.stop())

    assert telegraf_tcp_server.connections_made == 1
    assert telegraf_tcp_server.connections_lost == 1


async def test_metric_sender_reconnecting(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())

    # Now that the server is running and the client has connected,
    # cancel the server and let it die off.
    telegraf_tcp_server.close()
    await telegraf_tcp_server.wait_closed()
    until = time.time() + 5
    while sender.connected:
        await asyncio.sleep(0.01)
        assert time.time() >= until, 'sender never disconnected'

    # Start the server on the same port and let the client reconnect.
    asyncio.create_task(telegraf_tcp_server.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())
    assert sender.connected is True

    await wait_for(sender.stop())


async def test_metric_sender_cancelling(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    task = asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())

    task.cancel()
    await wait_for(sender.stopped.wait())


async def test_shutdown_when_disconnected(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())

    telegraf_tcp_server.close()
    await telegraf_tcp_server.wait_closed()

    await wait_for(sender.stop())


async def test_socket_resets(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    asyncio.create_task(sender.run())
    await wait_for(telegraf_tcp_server.client_connected.acquire())

    telegraf_tcp_server.transports[0].close()
    await wait_for(telegraf_tcp_server.client_connected.acquire())
    await wait_for(sender.stop())


async def test_metric_sender_stopping_when_not_running_is_safe(telegraf_tcp_server):
    sender = MetricSender(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port)
    await wait_for(sender.stop())


async def test_metric_sender_starting_and_stopping_without_connecting(telegraf_tcp_server):
    host, port = telegraf_tcp_server.host, telegraf_tcp_server.port
    telegraf_tcp_server.close()
    await wait_for(telegraf_tcp_server.wait_closed())
    sender = MetricSender(host=host, port=port)
    asyncio.create_task(sender.run())
    await asyncio.sleep(0.1)
    await sender.stop()
