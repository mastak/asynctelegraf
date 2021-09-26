import asyncio
import socket
from unittest.mock import MagicMock, patch

import pytest

from asynctelegraf import TelegrafClient, UDPProtocol
from tests.conftest import wait_for

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize('global_tags', [['key:value'], {'key': 'value'}], ids=["global_tags-list", "global_tags-dict"])
async def test_telegraf_client(telegraf_server, global_tags):
    telegraf = TelegrafClient(host=telegraf_server.host, port=telegraf_server.port,
                              protocol=telegraf_server.ip_protocol, global_tags=global_tags)
    async with telegraf:
        with patch('random.random', return_value=0):
            telegraf.incr('counter', tags={'name': 'bob', 'flow': 'main'}, rate=0.9)
            telegraf.timing('simple.timer', 0.001, tags=['name:marlin', 'flow:custom'], rate=0.5)
            await wait_for(telegraf_server.message_received.acquire())
            await wait_for(telegraf_server.message_received.acquire())
            assert telegraf_server.metrics[0] == b'counter:1|c|@0.9|#key:value,name:bob,flow:main'
            assert telegraf_server.metrics[1] == b'simple.timer:1.0|ms|@0.5|#key:value,name:marlin,flow:custom'


async def test_adjusting_gauge(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_client.gauge('simple.gauge', 100)
    telegraf_client.gauge('simple.gauge', -10, delta=True)
    telegraf_client.gauge('simple.gauge', 10, delta=True)
    for _ in range(3):
        await wait_for(telegraf_server.message_received.acquire())

    _assert_metrics_equal(telegraf_server.metrics[0], 'simple.gauge', '100', 'g')
    _assert_metrics_equal(telegraf_server.metrics[1], 'simple.gauge', '-10', 'g')
    _assert_metrics_equal(telegraf_server.metrics[2], 'simple.gauge', '+10', 'g')


async def test_adjusting_counter(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_client.incr('simple.counter')
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[-1], 'simple.counter', 1, 'c')

    telegraf_client.increment('simple.counter')
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[-1], 'simple.counter', 1, 'c')

    telegraf_client.incr('simple.counter', 10)
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[-1], 'simple.counter', 10, 'c')

    telegraf_client.decr('simple.counter')
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[-1], 'simple.counter', -1, 'c')

    telegraf_client.decr('simple.counter', 10)
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[-1], 'simple.counter', -10, 'c')


async def test_sending_timer(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    secs = 12.34
    telegraf_client.timing('simple.timer', secs)
    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[0], 'simple.timer', 12340.0, 'ms')


async def test_timed_context(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    with patch('time.perf_counter', side_effect=[0, 9.1235]):
        with telegraf_client.timed('timed.timer'):
            await asyncio.sleep(0.001)

    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[0], 'timed.timer', 9123.5, 'ms')


async def test_timed_direct_call(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    with patch('time.perf_counter', side_effect=[0, 1.2233]):
        timed = telegraf_client.timed('timed.timer')
        timed.start()
        try:
            await asyncio.sleep(0.001)
        finally:
            timed.stop()

    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[0], 'timed.timer', 1223.3, 'ms')


async def test_timed_decorator_sync(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    with patch('time.perf_counter', side_effect=[0, 1.23456]):

        @telegraf_client.timed('decorator.timer')
        def check_func():
            return 123

        assert check_func() == 123

    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[0], 'decorator.timer', 1234.56, 'ms')


async def test_timed_decorator_async(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    with patch('time.perf_counter', side_effect=[0, 6.54321]):

        @telegraf_client.timed('decorator.timer')
        async def check_func():
            return 321

        assert await check_func() == 321

    await wait_for(telegraf_server.message_received.acquire())
    _assert_metrics_equal(telegraf_server.metrics[0], 'decorator.timer', 6543.21, 'ms')


async def test_metrics_sent_while_disconnected_are_queued(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_server.close()
    await telegraf_server.wait_closed()

    for value in range(50):
        telegraf_client.incr('counter', value)

    asyncio.create_task(telegraf_server.run())
    await wait_for(telegraf_server.client_connected.acquire())
    for value in range(50):
        await wait_for(telegraf_server.message_received.acquire())
        assert f'counter:{value}|c'.encode(), telegraf_server.metrics.pop(0)


async def test_sending_metrics_tags_and_rate(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    with patch('random.random', return_value=0):
        telegraf_client.incr('counter', tags={'name': 'bob', 'flow': 'main'}, rate=0.9)
        telegraf_client.timing('timer', 0.001, tags=['name:marlin', 'flow:custom'], rate=0.5)
        await wait_for(telegraf_server.message_received.acquire())
        await wait_for(telegraf_server.message_received.acquire())

    assert telegraf_server.metrics[0] == b'counter:1|c|@0.9|#name:bob,flow:main'
    assert telegraf_server.metrics[1] == b'timer:1.0|ms|@0.5|#name:marlin,flow:custom'


@pytest.mark.parametrize('protocol', [socket.IPPROTO_UDP, socket.IPPROTO_TCP], ids=["UDP", "TCP"])
@pytest.mark.parametrize('random', [0, 1])
async def test_rate_pass(protocol, random):
    telegraf_client = TelegrafClient(protocol=protocol)

    with patch('random.random', return_value=random):
        telegraf_client.incr('counter', tags={'name': 'bob', 'flow': 'main'}, rate=0.9)
        telegraf_client.timing('timer', 0.001, tags=['name:marlin', 'flow:custom'], rate=0.5)

    if random == 0:
        assert telegraf_client.sender.queue.qsize() == 2
        assert telegraf_client.sender.queue.get_nowait() == b'counter:1|c|@0.9|#name:bob,flow:main'
        assert telegraf_client.sender.queue.get_nowait() == b'timer:1.0|ms|@0.5|#name:marlin,flow:custom'
    elif random == 1:
        assert telegraf_client.sender.queue.qsize() == 0
    else:
        pytest.fail(f'Incorrect random value: {random}')


async def test_that_client_sends_to_new_server_udp(telegraf_udp_server, telegraf_udp):
    telegraf_udp_server.close()
    await telegraf_udp_server.wait_closed()

    telegraf_udp.incr('should.be.lost')
    await asyncio.sleep(telegraf_udp.sender.CONNECTION_TIMEOUT + 0.1)

    asyncio.create_task(telegraf_udp_server.run())
    await telegraf_udp_server.wait_running()

    telegraf_udp.incr('should.be.recvd')
    await wait_for(telegraf_udp_server.message_received.acquire())
    assert telegraf_udp_server.metrics[0] == b'should.be.recvd:1|c'


async def test_that_client_handles_socket_closure(telegraf_server_client):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_client.sender.protocol.transport.close()
    await wait_for(asyncio.sleep(telegraf_client.sender.CONNECTION_TIMEOUT))

    telegraf_client.incr('should.be.recvd')
    await wait_for(telegraf_server.message_received.acquire())
    assert telegraf_server.metrics[0] == b'should.be.recvd:1|c'


async def test_that_metrics_are_dropped_when_queue_overflows(telegraf_server):
    telegraf = TelegrafClient(host=telegraf_server.host, port=1, queue_size=10)
    async with telegraf:
        await asyncio.sleep(0.0)
        # fill up the queue with incr's
        for expected_size in range(1, telegraf.sender.queue.maxsize + 1):
            telegraf.incr('counter')
            assert telegraf.sender.queue.qsize() == expected_size

        # the following decr's should be ignored
        for _ in range(10):
            telegraf.decr('counter')
            assert telegraf.sender.queue.qsize() == 10

        # make sure that only the incr's are in the queue
        for _ in range(telegraf.sender.queue.qsize()):
            metric = await telegraf.sender.queue.get()
            assert metric == b'counter:1|c'


async def test_batch_size():
    telegraf = TelegrafClient(batch_size=10, protocol=socket.IPPROTO_UDP)

    for i in range(10):
        telegraf.incr('counter')

    with patch.object(UDPProtocol, 'send', new=MagicMock()) as send_mock:
        async with telegraf:
            await asyncio.sleep(0.1)
            send_mock.assert_called_once()
            send_mock.assert_called_with(b'counter:1|c\ncounter:1|c\ncounter:1|c\ncounter:1|c\ncounter:1|c\n'
                                         b'counter:1|c\ncounter:1|c\ncounter:1|c\ncounter:1|c\ncounter:1|c')


def _assert_metrics_equal(recvd: bytes, path, value, type_code):
    decoded = recvd.decode('utf-8')
    recvd_path, _, rest = decoded.partition(':')
    recvd_value, _, recvd_code = rest.partition('|')
    assert path == recvd_path, 'metric path mismatch'
    if type_code == 'ms':
        assert pytest.approx(float(recvd_value)) == value, 'metric value mismatch'
    else:
        assert recvd_value == str(value), 'metric value mismatch'
    assert recvd_code == type_code, 'metric type mismatch'
