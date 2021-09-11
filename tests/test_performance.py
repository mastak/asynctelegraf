import asyncio
import time

import pytest

pytestmark = pytest.mark.asyncio

timer = time.perf_counter


@pytest.fixture
async def custom_benchmark(benchmark):
    benchmark._mode = 'custom'
    benchmark._warmup = 1
    benchmark._disable_gc = True
    return benchmark


@pytest.mark.parametrize('batch_size', [0, 50, 200, 500, 1000])
async def test_perf_one_worker(custom_benchmark, telegraf_server_client, batch_size):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_client.sender._batch_size = batch_size

    rounds = 10
    loops = 999

    stats_session = custom_benchmark._make_stats(iterations=1)
    for _ in range(rounds):
        await _runner(telegraf_client, loops=loops, stats_session=stats_session)
        await asyncio.sleep(0.1)  # flush buffers


@pytest.mark.parametrize('batch_size', [0, 50, 200, 500, 1000])
async def test_perf_multiple_workers(custom_benchmark, telegraf_server_client, batch_size):
    telegraf_server, telegraf_client = telegraf_server_client
    telegraf_client.sender._batch_size = batch_size

    rounds = 10
    workers = 10

    stats_session = custom_benchmark._make_stats(iterations=1)
    for round_num in range(rounds):
        tasks = [_runner(telegraf_client, loops=99, stats_session=stats_session) for _ in range(workers)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)  # flush buffers


async def _runner(telegraf_client, loops, stats_session):
    started_at = timer()
    for i in range(loops):
        telegraf_client.timing('perf.timer', 0.003, tags=['name:marlin', 'flow:custom'])
        await asyncio.sleep(0.0)
    stats_session.update(timer() - started_at)
