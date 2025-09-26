import asyncio

import aiohttp

from core.api.exceptions import BinanceAPIException
from .log_kit import logger


async def async_retry_getter(func, max_times=5, **kwargs):
    sleep_seconds = 1
    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if isinstance(e, BinanceAPIException) and e.code == -1003:
                raise e
            if max_times == 0:
                raise e
            else:
                logger.warning(f'{e.__class__.__name__} occurred, %s, %d times retry left', str(e), max_times)

            await asyncio.sleep(sleep_seconds)
            max_times -= 1
            sleep_seconds *= 2


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
