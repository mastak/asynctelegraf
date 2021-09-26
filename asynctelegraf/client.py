import asyncio
import logging
import queue
import random
import socket
import time
import typing as t
from asyncio import iscoroutinefunction
from functools import wraps

from asynctelegraf.protocol import TCPProtocol, TelegrafProtocol, UDPProtocol
from asynctelegraf.types import TagsType
from asynctelegraf.utils import LimitedLog

logger = logging.getLogger(__name__)
limited_logger: logging.Logger = t.cast(logging.Logger, LimitedLog(logger_src=logging.getLogger(__name__)))


class TelegrafClient:
    def __init__(self, host: str = '127.0.0.1', port: int = 8125, prefix: str = None,
                 protocol: int = socket.IPPROTO_UDP, default_rate: float = 1, queue_size: int = 1000,
                 global_tags: TagsType = None, batch_size: t.Optional[int] = None):
        self.prefix = f'{prefix}.' if prefix else ''
        self._default_rate = default_rate
        self._global_tags_str = self._build_tags(global_tags)
        self.sender = MetricSender(host=host, port=port, protocol=protocol, queue_size=queue_size,
                                   batch_size=batch_size)
        self._sender_task: t.Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        self._sender_task = asyncio.ensure_future(self.sender.run())

    async def stop(self) -> None:
        await self.sender.stop()

    async def __aenter__(self) -> "TelegrafClient":
        await self.start()
        return self

    async def __aexit__(self, *args: t.Any) -> None:
        await self.stop()

    def increment(self, metric: str, value: int = 1, rate: t.Optional[float] = None, tags: TagsType = None) -> None:
        self.incr(metric=metric, value=value, rate=rate, tags=tags)

    def incr(self, metric: str, value: int = 1, rate: t.Optional[float] = None, tags: TagsType = None) -> None:
        self._push_metric(metric=metric, value=str(value), type_code='c', rate=rate, tags=tags)

    def decr(self, metric: str, value: int = 1, rate: t.Optional[float] = None, tags: TagsType = None) -> None:
        self._push_metric(metric=metric, value=str(-value), type_code='c', rate=rate, tags=tags)

    def gauge(self, metric: str, value: float, delta: bool = False,
              rate: t.Optional[float] = None, tags: TagsType = None) -> None:
        payload = f'{value:+d}' if delta else str(value)
        self._push_metric(metric=metric, value=payload, type_code='g', rate=rate, tags=tags)

    def timing(self, metric: str, seconds: float, rate: t.Optional[float] = None, tags: TagsType = None,
               is_ms_passed: bool = False) -> None:
        ms = seconds if is_ms_passed else seconds * 1000.0
        self._push_metric(metric=metric, value=str(ms), type_code='ms', rate=rate, tags=tags)

    def timed(self, metric: str, rate: t.Optional[float] = None,
              tags: TagsType = None, use_ms: bool = False) -> 'TimedContextManagerDecorator':
        return TimedContextManagerDecorator(telegraf=self, metric=metric, tags=tags, rate=rate, use_ms=use_ms)

    def _push_metric(self, metric: str, value: str, type_code: str, rate: t.Optional[float] = None,
                     tags: TagsType = None) -> None:
        """
        MetricName:value|type|@sample_rate|#tag1:value,tag1...
        """
        rate = rate if rate is not None else self._default_rate
        rate_part = ''
        if rate < 1:
            if random.random() > rate:
                return
            rate_part = f'|@{rate}'

        tag_part = ''
        if tags:
            tags_strs = [i for i in (self._global_tags_str, self._build_tags(tags)) if i]
            tags_str = ','.join(tags_strs)
            tag_part = f'|#{tags_str}'

        payload = f'{self.prefix}{metric}:{value}|{type_code}{rate_part}{tag_part}'
        self.sender.schedule_send(payload.encode('utf-8'))

    @staticmethod
    def _build_tags(tags: TagsType) -> str:
        if not tags:
            return ''

        if isinstance(tags, dict):
            return ','.join(f'{k}:{v}' for k, v in tags.items())
        return ','.join(tags)


class MetricSender:
    RECONNECTION_DELAY = 0.1
    CONNECTION_TIMEOUT = 0.3

    def __init__(self, host: str, port: int = 8125, protocol: int = socket.IPPROTO_TCP, queue_size: int = 1000,
                 batch_size: t.Optional[int] = None) -> None:
        self.host = host
        self.port = port
        if protocol not in {socket.IPPROTO_UDP, socket.IPPROTO_TCP}:
            raise RuntimeError(f'ip_protocol {protocol} is not supported')

        self._protocol = protocol
        self._batch_size = batch_size

        self._is_stopping = False
        self.protocol: t.Optional['TelegrafProtocol'] = None
        self.stopped: t.Union[True, asyncio.Event] = True  # type: ignore
        self._queue_size = queue_size
        # add all metrics in queue.Queue while client is not started
        # asyncio.Queue cannot be used, because asyncio loop is not available here
        self.queue: t.Union[asyncio.Queue, queue.Queue] = queue.Queue(maxsize=queue_size)

    @property
    def connected(self) -> bool:
        return self.protocol is not None and self.protocol.connected.is_set()

    def schedule_send(self, metric: bytes) -> None:
        try:
            self.queue.put_nowait(metric)
        except (asyncio.QueueFull, queue.Full):
            if self.stopped is True:
                limited_logger.error('Sending metrics while telegraf client is not started')
            else:
                limited_logger.warning('telegraf queue is full, discarding metric')

    async def run(self) -> None:
        self.queue = self._init_async_queue()
        self.stopped = asyncio.Event()
        while not self._is_stopping:
            try:
                if not self.connected:
                    await self._connect_if_necessary()

                if self.connected:
                    metric = await self._get_queued_metric_or_none()
                    if metric is None:
                        continue
                    self.protocol.send(metric)  # type: ignore
                else:
                    limited_logger.warning('No connection to telegraf agent')
                    await asyncio.sleep(self.RECONNECTION_DELAY)
            except asyncio.CancelledError:
                logger.debug('Task cancelled, exiting')
                self._is_stopping = True
            except Exception as exc:
                logger.exception(exc)

        if not self.queue.empty() and self.protocol is not None:
            for _ in range(self.queue.qsize()):
                if not self.connected:
                    break

                metric = await self._get_queued_metric_or_none(timeout=0)
                if metric is None:
                    break

                self.protocol.send(metric)

        if self.protocol is not None:
            await self.protocol.shutdown()
        self.stopped.set()

    async def stop(self) -> None:
        self._is_stopping = True
        if self.stopped is not True:
            await self.stopped.wait()

    def _init_async_queue(self):
        if isinstance(self.queue, asyncio.Queue):
            return self.queue

        async_queue = asyncio.Queue(maxsize=self._queue_size)
        if self.queue.empty():
            return async_queue

        for i in range(self.queue.qsize()):
            try:
                async_queue.put_nowait(self.queue.get_nowait())
            except (asyncio.QueueFull, queue.Empty):
                return async_queue
        return async_queue

    async def _create_transport(self) -> t.Tuple[asyncio.BaseTransport, 'TelegrafProtocol']:
        if self._protocol == socket.IPPROTO_TCP:
            transport, protocol = await asyncio.get_running_loop().create_connection(
                protocol_factory=TCPProtocol,
                host=self.host,
                port=self.port
            )
        elif self._protocol == socket.IPPROTO_UDP:
            transport, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
                protocol_factory=UDPProtocol,
                remote_addr=(self.host, self.port),
                reuse_port=True
            )
        else:
            raise RuntimeError(f'ip_protocol {self._protocol} is not supported')
        return transport, t.cast(TelegrafProtocol, protocol)

    async def _connect_if_necessary(self) -> None:
        if self.protocol is not None:
            try:
                await asyncio.wait_for(self.protocol.connected.wait(), self.CONNECTION_TIMEOUT)
            except asyncio.TimeoutError:
                await self.protocol.shutdown()
                limited_logger.warning(f'Protocol is not connected after {self.CONNECTION_TIMEOUT}')

        if not self.connected:
            try:
                transport, self.protocol = await self._create_transport()
                logger.debug('Connection established to %s', transport.get_extra_info('peername'))
            except IOError as error:
                logger.debug('Connection to %s:%s failed: %s', self.host, self.port, error)

    async def _get_queued_metric_or_none(self, timeout=0.1) -> t.Optional[bytes]:
        if not self._batch_size:
            try:
                return await asyncio.wait_for(self.queue.get(), timeout=timeout)
            except (queue.Empty, asyncio.QueueEmpty, asyncio.TimeoutError):
                return None

        buffer = []
        for _ in range(self._batch_size):
            try:
                metric = self.queue.get_nowait()
            except (queue.Empty, asyncio.QueueEmpty):
                break
            else:
                buffer.append(metric)
        if not buffer:
            await asyncio.sleep(0.001)  # give asyncio loop chance to switch to another coroutine
            return None
        return b'\n'.join(buffer)


class TimedContextManagerDecorator:
    def __init__(self, telegraf: TelegrafClient, metric: str = None, rate: t.Optional[float] = None,
                 tags: TagsType = None, use_ms: bool = False):
        self._telegraf = telegraf
        self.metric = metric
        self.tags = tags
        self.rate = rate
        self.use_ms = use_ms

    def __call__(self, func):
        # Coroutines
        if iscoroutinefunction(func):
            @wraps(func)
            async def wrapped_co(*args, **kwargs):
                start = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    self._send(start)

            return wrapped_co

        # Others
        @wraps(func)
        def wrapped(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                self._send(start)

        return wrapped

    def __enter__(self):
        if not self.metric:
            raise TypeError("Cannot used timed without a metric!")
        self._start = time.perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self._send(self._start)

    def _send(self, start):
        elapsed = time.perf_counter() - start
        elapsed = int(round(1000 * elapsed)) if self.use_ms else elapsed
        self._telegraf.timing(self.metric, elapsed, self.tags, self.rate)

    def start(self):
        self.__enter__()

    def stop(self):
        self.__exit__(None, None, None)
