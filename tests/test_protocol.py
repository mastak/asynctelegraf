import asyncio
import socket

import pytest

from asynctelegraf import TelegrafClient
from tests.conftest import wait_for

pytestmark = pytest.mark.asyncio


async def test_connection_failures(telegraf_tcp_server, sender_tcp):
    # Change the port and close the transport, this will cause the
    # sender to reconnect to the new port and fail.
    sender_tcp.port = 1
    sender_tcp.protocol.transport.close()

    # Wait for the sender to be disconnected, then change the
    # port back and let the sender reconnect.
    while sender_tcp.connected:
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.2)
    sender_tcp.port = telegraf_tcp_server.port

    await wait_for(telegraf_tcp_server.client_connected.acquire())


async def test_client_handles_socket_closure_udp(telegraf_udp_server, telegraf_udp):
    telegraf_udp.sender.protocol.transport.close()
    await wait_for(asyncio.sleep(telegraf_udp.sender.CONNECTION_TIMEOUT))

    telegraf_udp.incr('should.be.recvd')
    await wait_for(telegraf_udp_server.message_received.acquire())
    assert telegraf_udp_server.metrics[0] == b'should.be.recvd:1|c'


async def test_protocol_values(telegraf_tcp_server):
    client = TelegrafClient(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port,
                            protocol=telegraf_tcp_server.ip_protocol)
    assert socket.IPPROTO_TCP == client.sender._protocol

    client = TelegrafClient(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port, protocol=socket.IPPROTO_UDP)
    assert socket.IPPROTO_UDP == client.sender._protocol

    with pytest.raises(RuntimeError):
        TelegrafClient(host=telegraf_tcp_server.host, port=telegraf_tcp_server.port, protocol=socket.IPPROTO_GRE)
