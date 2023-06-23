from purse.collections import RedisList
import pytest
from ctx import Context


@pytest.fixture(scope="session")
def ctx() -> Context:
    ctx = Context()
    yield ctx


def test_basics(ctx):
    data = ['a', 'b', 'c', 'd', 'e', 'f']

    list_key = 'trash:test_list_basics'

    redlist = RedisList(ctx.redis_conn, list_key, str)

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

        assert data.pop() == await redlist.pop()

        data.insert(2, 'xx')
        await redlist.insert(2, 'xx')

        data.insert(-3, 'yy')
        await redlist.insert(-3, 'yy')

        for a, b in zip(data, [x async for x in redlist]):
            assert a == b

    ctx.run(main()).result()
