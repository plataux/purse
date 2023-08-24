import asyncio

from purse.collections import (RedisHash, RedisList, RedisQueue, RedisSet,
                               RedisPriorityQueue, RedisLifoQueue, RedisSortedSet, RedisKeySpace)
from pydantic import BaseModel
import pytest
from ctx import Context


@pytest.fixture(scope="session")
def ctx() -> Context:
    ctx = Context()
    yield ctx


class Plant(BaseModel):
    name: str
    nutrition: float
    tasty: bool = False

    def __hash__(self):
        return hash(self.model_dump_json())


data = [
    Plant(name="apples", nutrition=5, tasty=True),
    Plant(name="bananas", nutrition=3, tasty=True),
    Plant(name="spinach", nutrition=9, tasty=False),
    Plant(name="tomatoes", nutrition=8, tasty=False),
    Plant(name="broccoli", nutrition=12, tasty=True),
    Plant(name="lettuce", nutrition=4, tasty=False),
    Plant(name="mangoes", nutrition=6, tasty=True)
]


def test_redis_hash(ctx):
    red_hash = {}
    plant_data = {}

    red_hash['rh_str'] = RedisHash(ctx.redis_conn, 'rh_str', str)
    red_hash['rh_dict'] = RedisHash(ctx.redis_conn, 'rh_dict', dict)
    red_hash['rh_bytes'] = RedisHash(ctx.redis_conn, 'rh_bytes', bytes)
    red_hash['rh_model'] = RedisHash(ctx.redis_conn, 'rh_model', Plant)

    plant_data['plants_str'] = {p.name: p.model_dump_json() for p in data}
    plant_data['plants_dict'] = {p.name: p.model_dump() for p in data}
    plant_data['plants_bytes'] = {p.name: p.model_dump_json().encode() for p in data}
    plant_data['plants_model'] = {p.name: p for p in data}

    rh_test_data = [
        (red_hash['rh_str'], plant_data['plants_str']),
        (red_hash['rh_dict'], plant_data['plants_dict']),
        (red_hash['rh_bytes'], plant_data['plants_bytes']),
        (red_hash['rh_model'], plant_data['plants_model'])
    ]

    async def main():
        rh: RedisHash
        for rh, dt in rh_test_data:
            await rh.clear()
            await rh.update(dt)

            assert await rh.len() == len(dt)

            async for k, v in rh.items():
                assert dt[k] == v

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_list(ctx):
    red_list = {}
    plant_data = {}

    red_list['rl_str'] = RedisList(ctx.redis_conn, 'rl_str', str)
    red_list['rl_dict'] = RedisList(ctx.redis_conn, 'rl_dict', dict)
    red_list['rl_bytes'] = RedisList(ctx.redis_conn, 'rl_bytes', bytes)
    red_list['rl_model'] = RedisList(ctx.redis_conn, 'rl_model', Plant)

    plant_data['plants_str'] = [p.model_dump_json() for p in data]
    plant_data['plants_dict'] = [p.model_dump() for p in data]
    plant_data['plants_bytes'] = [p.model_dump_json().encode() for p in data]
    plant_data['plants_model'] = data

    rl_test_data = [
        (red_list['rl_str'], plant_data['plants_str']),
        (red_list['rl_dict'], plant_data['plants_dict']),
        (red_list['rl_bytes'], plant_data['plants_bytes']),
        (red_list['rl_model'], plant_data['plants_model'])
    ]

    async def main():
        rl: RedisList
        for rl, dt in rl_test_data:
            await rl.clear()
            await rl.extend(dt)

            assert await rl.len() == len(dt)

            x = []
            async for v in rl.values():
                x.append(v)

            assert x == dt

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_set(ctx):
    red_set = {}
    plant_data = {}

    red_set['rs_str'] = RedisSet(ctx.redis_conn, 'rs_str', str)
    red_set['rs_dict'] = RedisSet(ctx.redis_conn, 'rs_dict', dict)
    red_set['rs_bytes'] = RedisSet(ctx.redis_conn, 'rs_bytes', bytes)
    red_set['rs_model'] = RedisSet(ctx.redis_conn, 'rs_model', Plant)

    plant_data['plants_str'] = [p.model_dump_json() for p in data]
    plant_data['plants_dict'] = [p.model_dump() for p in data]
    plant_data['plants_bytes'] = [p.model_dump_json().encode() for p in data]
    plant_data['plants_model'] = list(data)

    rs_test_data = [
        (red_set['rs_str'], plant_data['plants_str']),
        (red_set['rs_dict'], plant_data['plants_dict']),
        (red_set['rs_bytes'], plant_data['plants_bytes']),
        (red_set['rs_model'], plant_data['plants_model'])
    ]

    async def main():
        rs: RedisSet
        for rs, dt in rs_test_data:
            await rs.clear()
            await rs.update(*dt)

            assert await rs.len() == len(dt)

            x = []
            async for v in rs.values():
                x.append(v)

            for item in dt:
                assert item in x

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_sorted_set(ctx):
    red_sorted_set = {}
    plant_data = {}

    red_sorted_set['rss_str'] = RedisSortedSet(ctx.redis_conn, 'rss_str', str)
    red_sorted_set['rss_dict'] = RedisSortedSet(ctx.redis_conn, 'rss_dict', dict)
    red_sorted_set['rss_bytes'] = RedisSortedSet(ctx.redis_conn, 'rss_bytes', bytes)
    red_sorted_set['rss_model'] = RedisSortedSet(ctx.redis_conn, 'rss_model', Plant)

    plant_data['plants_str'] = [(p.model_dump_json(), p.nutrition) for p in data]
    plant_data['plants_dict'] = [(p.model_dump(), p.nutrition) for p in data]
    plant_data['plants_bytes'] = [(p.model_dump_json().encode(), p.nutrition) for p in data]
    plant_data['plants_model'] = [(p, p.nutrition) for p in data]


    rss_test_data = [
        (red_sorted_set['rss_str'], plant_data['plants_str']),
        (red_sorted_set['rss_dict'], plant_data['plants_dict']),
        (red_sorted_set['rss_bytes'], plant_data['plants_bytes']),
        (red_sorted_set['rss_model'], plant_data['plants_model'])
    ]

    async def main():
        rss: RedisSortedSet
        for rss, dt in rss_test_data:
            await rss.clear()
            await rss.add_multi(dt)

            assert await rss.len() == len(dt)

            x = []
            async for v in rss.values():
                x.append(v)

            for item in dt:
                assert item in x

            assert sorted(dt, key=lambda g: g[1]) == sorted(x, key=lambda g: g[1])

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_queue(ctx):
    red_queue = {}
    plant_data = {}

    red_queue['rq_str'] = RedisQueue(ctx.redis_conn, 'rq_str', str)
    red_queue['rq_dict'] = RedisQueue(ctx.redis_conn, 'rq_dict', dict)
    red_queue['rq_bytes'] = RedisQueue(ctx.redis_conn, 'rq_bytes', bytes)
    red_queue['rq_model'] = RedisQueue(ctx.redis_conn, 'rq_model', Plant)

    plant_data['plants_str'] = [p.model_dump_json() for p in data]
    plant_data['plants_dict'] = [p.model_dump() for p in data]
    plant_data['plants_bytes'] = [p.model_dump_json().encode() for p in data]
    plant_data['plants_model'] = data

    rq_test_data = [
        (red_queue['rq_str'], plant_data['plants_str']),
        (red_queue['rq_dict'], plant_data['plants_dict']),
        (red_queue['rq_bytes'], plant_data['plants_bytes']),
        (red_queue['rq_model'], plant_data['plants_model'])
    ]

    async def main():
        rq: RedisQueue
        for rq, dt in rq_test_data:

            await rq.delete_redis_key()

            for item in dt:
                await rq.put(item)

            for item in dt:
                assert await rq.get() == item

            with pytest.raises(asyncio.QueueEmpty):
                await rq.get(0.1)

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_lifo_queue(ctx):
    red_lifo_queue = {}
    plant_data = {}

    red_lifo_queue['rlq_str'] = RedisLifoQueue(ctx.redis_conn, 'rlq_str', str)
    red_lifo_queue['rlq_dict'] = RedisLifoQueue(ctx.redis_conn, 'rlq_dict', dict)
    red_lifo_queue['rlq_bytes'] = RedisLifoQueue(ctx.redis_conn, 'rlq_bytes', bytes)
    red_lifo_queue['rlq_model'] = RedisLifoQueue(ctx.redis_conn, 'rlq_model', Plant)

    plant_data['plants_str'] = [p.model_dump_json() for p in data]
    plant_data['plants_dict'] = [p.model_dump() for p in data]
    plant_data['plants_bytes'] = [p.model_dump_json().encode() for p in data]
    plant_data['plants_model'] = data

    rlq_test_data = [
        (red_lifo_queue['rlq_str'], plant_data['plants_str']),
        (red_lifo_queue['rlq_dict'], plant_data['plants_dict']),
        (red_lifo_queue['rlq_bytes'], plant_data['plants_bytes']),
        (red_lifo_queue['rlq_model'], plant_data['plants_model'])
    ]

    async def main():
        rlq: RedisLifoQueue
        for rlq, dt in rlq_test_data:

            await rlq.delete_redis_key()

            for item in dt:
                await rlq.put(item)

            for item in reversed(dt):
                assert await rlq.get() == item

            with pytest.raises(asyncio.QueueEmpty):
                await rlq.get(0.1)

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_priority_queue(ctx):
    red_priority_queue = {}
    plant_data = {}

    red_priority_queue['rpq_str'] = RedisPriorityQueue(ctx.redis_conn, 'rpq_str', str)
    red_priority_queue['rpq_dict'] = RedisPriorityQueue(ctx.redis_conn, 'rpq_dict', dict)
    red_priority_queue['rpq_bytes'] = RedisPriorityQueue(ctx.redis_conn, 'rpq_bytes', bytes)
    red_priority_queue['rpq_model'] = RedisPriorityQueue(ctx.redis_conn, 'rpq_model', Plant)

    plant_data['plants_str'] = [(p.model_dump_json(), p.nutrition) for p in data]
    plant_data['plants_dict'] = [(p.model_dump(), p.nutrition) for p in data]
    plant_data['plants_bytes'] = [(p.model_dump_json().encode(), p.nutrition) for p in data]
    plant_data['plants_model'] = [(p, p.nutrition) for p in data]

    rpq_test_data = [
        (red_priority_queue['rpq_str'], plant_data['plants_str']),
        (red_priority_queue['rpq_dict'], plant_data['plants_dict']),
        (red_priority_queue['rpq_bytes'], plant_data['plants_bytes']),
        (red_priority_queue['rpq_model'], plant_data['plants_model'])
    ]

    async def main():
        rpq: RedisPriorityQueue
        for rpq, dt in rpq_test_data:

            await rpq.delete_redis_key()

            for item in dt:
                await rpq.put(item)

            for item in sorted(dt, key=lambda x: x[1]):
                assert await rpq.get() == item

            with pytest.raises(asyncio.QueueEmpty):
                await rpq.get(0.1)

        return 0

    assert ctx.run(main()).result() == 0

def test_redis_keyspace(ctx):
    red_hash = {}
    plant_data = {}

    red_hash['rh_str'] = RedisKeySpace(ctx.redis_conn, 'rh_str_', str)
    red_hash['rh_dict'] = RedisKeySpace(ctx.redis_conn, 'rh_dict_', dict)
    red_hash['rh_bytes'] = RedisKeySpace(ctx.redis_conn, 'rh_bytes_', bytes)
    red_hash['rh_model'] = RedisKeySpace(ctx.redis_conn, 'rh_model_', Plant)

    plant_data['plants_str'] = {p.name: p.model_dump_json() for p in data}
    plant_data['plants_dict'] = {p.name: p.model_dump() for p in data}
    plant_data['plants_bytes'] = {p.name: p.model_dump_json().encode() for p in data}
    plant_data['plants_model'] = {p.name: p for p in data}

    rh_test_data = [
        (red_hash['rh_str'], plant_data['plants_str']),
        (red_hash['rh_dict'], plant_data['plants_dict']),
        (red_hash['rh_bytes'], plant_data['plants_bytes']),
        (red_hash['rh_model'], plant_data['plants_model'])
    ]

    async def main():
        rks: RedisKeySpace

        for rks, dt in rh_test_data:

            await rks.clear()
            await rks.update(dt)

            async for k, v in rks.items():
                assert v == dt[k]

            assert await rks.get('not_a_key') is None

            return 0

    assert ctx.run(main()).result() == 0