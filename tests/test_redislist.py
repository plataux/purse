
from purse.collections import RedisList
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
    data = ['a', 'b', 'c', 'd', 'e', 'f']

    list_key = 'trash:test_list_basics'

    redlist = RedisList(ctx.rc, list_key, str)

    async def main():
        await redlist.clear()

        assert await redlist.len() == 0

        await redlist.extend(data)

        assert await redlist.len() == len(data)

        assert await redlist.getitem(0) == data[0]

        assert await redlist.getitem(await redlist.len() - 1) == data[len(data) - 1]

        for a, b in zip(await redlist.slice(1, 4), data[1:4]):
            assert a == b

        data.append('g')
        await redlist.append('g')

        assert len(data) == await redlist.len()

    ctx.run(main()).result()
