
from purse.collections import RedisKeySpace
from pydantic import BaseModel
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

    redis_hash = RedisKeySpace(ctx.rc, list_key, str)

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


def test_models(ctx):
    class Plant(BaseModel):
        name: str
        nutrition: float
        tasty: bool = False

        def __hash__(self):
            return hash(self.json())

    plants = [
        Plant(name="apples", nutrition=5, tasty=True),
        Plant(name="bananas", nutrition=3, tasty=True),
        Plant(name="spinach", nutrition=9, tasty=False),
        Plant(name="tomatoes", nutrition=8, tasty=False),
        Plant(name="broccoli", nutrition=12, tasty=True),
        Plant(name="lettuce", nutrition=4, tasty=False),
        Plant(name="mangoes", nutrition=6, tasty=True)
    ]

    red_con = aioredis.Redis()
    redis_key = 'redis_ks_model_hash'

    redis_hash = RedisKeySpace(red_con, redis_key, Plant)

    async def main():
        await redis_hash.clear()
        assert await redis_hash.len() == 0
        await redis_hash.update({p.name: p for p in plants})
        assert await redis_hash.len() == len(plants)

        await redis_hash.set("carrot", Plant(name="carrot", nutrition=5, tasty=False))

        assert await redis_hash.len() == len(plants) + 1

        p: Plant = await redis_hash.get("spinach")

        assert isinstance(p, Plant)

        return 0

    assert ctx.run(main()).result() == 0
