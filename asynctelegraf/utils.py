import time
from functools import partial


class LimitedLog:
    def __init__(self, logger_src, limit=30, limit_time=1):
        self._logger_src = logger_src
        self._limit = limit
        self._limit_time = limit_time
        self._msg2times = {}

    def __getattr__(self, item):
        log_func = getattr(self._logger_src, item)
        return partial(self.log_wrapper, log_func)

    def log_wrapper(self, f, msg, *args, **kwargs):
        time_now = time.perf_counter()
        msg_times = self._msg2times.setdefault(msg, [])
        self._msg2times[msg] = [i for i in msg_times if i > time_now]
        if len(self._msg2times[msg]) < self._limit:
            f(msg, *args, **kwargs)
            self._msg2times[msg].append(time_now + self._limit_time)
