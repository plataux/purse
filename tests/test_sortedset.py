
import pytest
import aioredis
import asyncio
from threading import Thread
from purse.collections import RedisSortedSet
from pydantic import BaseModel


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
    plants = {
        'lettuce': 3,
        'carrots': 4,
        'apples': 5,
        'bananas': 7,
        'tomatoes': 8,
        'spinach': 9,
        'broccoli': 12,
    }

    key = 'trash:basic'

    rss = RedisSortedSet(ctx.rc, key, str)

    async def main():
        await rss.clear()
        assert await rss.len() == 0
        await rss.add(plants)
        assert await rss.len() == 7

        async for k, v in rss.values():
            assert k in plants and plants[k] == v

        k, v = list((await rss.pop_max()).items())[0]

        assert k == 'broccoli' and v == 12

        k, v = list((await rss.pop_min()).items())[0]

        assert k == 'lettuce' and v == 3

        assert await rss.len() == 5

        await rss.clear()

        return 0
    num = ctx.run(main()).result()

    assert num == 0


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
        Plant(name="carrots", nutrition=7, tasty=False),
        Plant(name="broccoli", nutrition=12, tasty=True),
        Plant(name="lettuce", nutrition=4, tasty=False),
        Plant(name="mangoes", nutrition=6, tasty=True)
    ]

    key = "trash:models"
    rss = RedisSortedSet(ctx.rc, key, Plant)

    async def main():
        await rss.clear()

        assert await rss.len() == 0

        await rss.add({p: p.nutrition for p in plants})

        assert await rss.len() == len(plants)

        p: Plant

        p, s = list((await rss.pop_max()).items())[0]
        assert p.name == "broccoli" and s == p.nutrition

        assert await rss.len() == len(plants) - 1

        mins = list((await rss.pop_min(count=2)).items())

        p, s = mins[0]
        assert p.name == "bananas" and s == p.nutrition

        p, s = mins[1]
        assert p.name == "lettuce" and s == p.nutrition

        assert await rss.len() == len(plants) - 3

        p, s = await rss.peak_max()
        assert p.name == 'spinach' and s == p.nutrition

        await rss.increment({p: 10})

        p, s = await rss.peak_max()
        assert p.name == 'spinach' and s == (p.nutrition + 10)

    ctx.run(main()).result()


def test_models_slices(ctx):
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
        Plant(name="carrots", nutrition=7, tasty=False),
        Plant(name="lettuce", nutrition=4, tasty=False),
        Plant(name="mangoes", nutrition=6, tasty=True)
    ]

    key = "trash:models"
    rss = RedisSortedSet(ctx.rc, key, Plant)

    async def main():
        await rss.clear()
        await rss.add({p: p.nutrition for p in plants})

        res = await rss.slice_by_score(min_score=7, max_score=20, descending=True)

        for p, k in zip(res.keys(), ['spinach', 'tomatoes', 'carrots']):
            assert p.name == k

        res = await rss.slice_by_score(min_score=7, max_score=20, descending=False)

        for p, k in zip(res.keys(), ['carrots', 'tomatoes', 'spinach']):
            assert p.name == k

        res = await rss.slice_by_rank(min_rank=0, max_rank=1, descending=True)  # top 2

        assert len(res) == 2

        for p, k in zip(res.keys(), ['spinach', 'tomatoes']):
            assert p.name == k

        res = await rss.slice_by_rank(min_rank=0, max_rank=1, descending=False)  # bottom 2

        assert len(res) == 2

        for p, k in zip(res.keys(), ['bananas', 'lettuce']):
            assert p.name == k

    ctx.run(main())
