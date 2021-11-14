
from purse.collections import RedisHash
# from pydantic import BaseModel
import pytest
import aioredis
import asyncio
from threading import Thread


class Context:
    def __init__(self):
        self.rc = aioredis.Redis(db=5)
        self.loop = asyncio.new_event_loop()

        def _loop_thread_target():
            self.loop.run_forever()
            self.loop.close()

        self._loop_thread = Thread(target=_loop_thread_target, daemon=True)
        self._loop_thread.start()

    def run(self, coro) -> asyncio.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)


@pytest.fixture(scope="session")
def ctx() -> Context:
    ctx = Context()
    yield ctx


def test_basics(ctx):
    data = {
        'a': "ant",
        'b': 'bull',
        'c': 'cat',
        'd': 'dog'
    }

    list_key = 'trash:test_hash'

    redis_hash = RedisHash(ctx.rc, list_key, str)

    async def main():
        await redis_hash.clear()

        # update
        await redis_hash.update(data)

        assert await redis_hash.len() == 4

        # get an existing and non-existing item
        assert await redis_hash.get('b') == 'bull'
        assert await redis_hash.get('z') is None

        # set
        await redis_hash.set('z', 'zebra')

        assert await redis_hash.get('z') == 'zebra'

        assert await redis_hash.len() == 5

        # pop
        assert await redis_hash.pop('z') == 'zebra'
        assert await redis_hash.get('z') is None
        assert await redis_hash.len() == 4

        # setdefault
        assert await redis_hash.setdefault('c', 'cow') == 'cat'
        assert await redis_hash.setdefault('z', 'zebra') == 'zebra'

    ctx.run(main()).result()

