import redis.asyncio as aioredis
import asyncio
from threading import Thread

from pydantic import BaseSettings


class RedisSettings(BaseSettings):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379


class Context:
    def __init__(self):
        settings = RedisSettings()
        self.rc = aioredis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=5)
        self.loop = asyncio.new_event_loop()

        def _loop_thread_target():
            self.loop.run_forever()
            self.loop.close()

        self._loop_thread = Thread(target=_loop_thread_target, daemon=True)
        self._loop_thread.start()

    def run(self, coro) -> asyncio.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)
