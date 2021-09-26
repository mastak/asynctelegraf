[![Build Status](https://github.com/mastak/asynctelegraf/workflows/default/badge.svg)](https://github.com/mastak/asynctelegraf/actions?query=workflow%3Adefault) 
[![codecov](https://codecov.io/gh/mastak/asynctelegraf/branch/master/graph/badge.svg)](https://codecov.io/gh/mastak/asynctelegraf) 
![PyPI](https://img.shields.io/pypi/v/asynctelegraf.svg?label=pypi%20version)  
![GitHub](https://img.shields.io/github/license/mastak/asynctelegraf.svg)

# asynctelegraf

Python asyncio client for [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/), compatible with AWS cloudwatch metrics (statsd)

## Install

```bash
pip install asynctelegraf
```

## Example

```python
import asyncio
from asynctelegraf import TelegrafClient

telegraf = TelegrafClient(host='127.0.0.1', port=8125, batch_size=100)


@telegraf.timed('magic.time', use_ms=True)
async def do_some_magic():
    await asyncio.sleep(3.3)
    return 'Magic'


async def main():
    async with telegraf.start():
        res = await do_some_magic()
    print("result", res)


asyncio.run(main())
```

## Contributions
If you have found a bug or have some idea for improvement feel free to create an issue or pull request.

## License
Apache 2.0
