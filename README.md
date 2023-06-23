
# Purse

High-Level Async-IO Python interface for Redis 6.0.x that provides useful Pythonic abstractions to simplify 
the usage of Redis as a non-blocking Caching layer, or even as a first-class non-blocking datastore.

Influenced and Inspired by the great library [pottery](https://github.com/brainix/pottery), with
a few differences in objectives and implementation detail.

* ``purse`` is strictly an Async-IO library that utilizes the redis library.
* ``purse`` tries to adhere as much as possible to familiar APIs and idioms used with familiar python structures
  (``dict``, ``set``, ``list`` among others), but deviates from those conventions in many instances:
  * Due to the ``async/await`` nature of the API, it is difficult and sometimes impossible to use python language constructs such as ``myhash["key"] = "value"`` - 
    as of Python 3.10, the language simply doesn't provide async-io methods for those operations and idioms 
  * ``purse`` tries to expose, as much as possible, Redis rich features such as key TTL and pattern matching, among others

Optionally, collections in this library use [pydantic](https://github.com/samuelcolvin/pydantic) 
to serialize, validate, and deserialize Python Models part of all data storage and retrieval operations

# installation

with pip

```bash
pip install redis-purse
```

# Basic Usage

## RedisList

RedisList provides an API that provides most methods and features of the python ``list`` and ``deque``

```python
import asyncio
from purse.collections import RedisList
from redis.asyncio import Redis


async def main():
    local_list = ['a', 'b', 'c', 'd', 'e', 'f']

    # local Redis >= 6.0.x plain connection with default params
    red_con = Redis()
    redis_key = 'redis_list'

    # The value_type defines the class to serialize to and from
    redis_list = RedisList(redis=red_con, rkey=redis_key, value_type=str)

    # Clear the list, in case it was previously populated
    await redis_list.clear()

    # extend a Redis list with a Python list
    await redis_list.extend(local_list)

    # async list comprehension
    print([x async for x in redis_list])

    # contains
    print(await redis_list.contains('f'))  # True
    print(await redis_list.contains('g'))  # False

    # getting the index of a value
    print(await redis_list.index('c'))  # 2
    print(await redis_list.index('g'))  # None, unlike a Python list that raises a ValueError

    # slicing
    print(await redis_list.slice(2, 5))  # ['c', 'd', 'e']

    # inserting values
    await redis_list.insert(2, 'x')
    await redis_list.insert(-2, 'y')

    # getitem
    assert await redis_list.getitem(2) == 'x'
    assert await redis_list.getitem(-3) == 'y'

    # some deque methods
    await redis_list.appendleft('z')
    await redis_list.pop()
    await redis_list.popleft()

asyncio.run(main())
```

## RedisHash

Provides most of the functionality of the Python ``dict``. 

```python
import asyncio
from purse.collections import RedisHash
from redis.asyncio import Redis
from pydantic import BaseModel


async def main():
    # Pydantic Model
    class Plant(BaseModel):
        name: str
        healthiness: float
        tasty: bool

    red_con = Redis()
    redis_key = 'redis_hash'

    # This class serializes and deserializes Plant Model objects when storing and retrieving data
    redis_hash = RedisHash(red_con, redis_key, Plant)
    await redis_hash.clear()

    plants = [
        Plant(name="spinach", healthiness=9.8, tasty=False),
        Plant(name="broccoli", healthiness=12.2, tasty=True),
        Plant(name="lettuce", healthiness=3, tasty=False),
        Plant(name="avocado", healthiness=8, tasty=True),
    ]

    # update redis hash with a python dict
    await redis_hash.update({p.name: p for p in plants})

    await redis_hash.set("carrot", Plant(name="carrot", healthiness=5, tasty=False))

    print(await redis_hash.len())  # currently 5 mappings in total
    
    #  RedisHash is a generic type with supports IDE intellisense and type hints
    p: Plant = await redis_hash.get('spinach')
    
    print(p.tasty)  # False
    
    # async for syntax
    async for name, plant in redis_hash.items():
        print(name, plant)

asyncio.run(main())
```

## Redlock

Distributed, None-blocking Lock implementation according to the algorithm and logic described here
https://redis.io/topics/distlock, and closely resembling the python implementation here
https://github.com/brainix/pottery/blob/master/pottery/redlock.py.

This none-blocking implementation is particularly efficient and attractive when a real world
distributed application is using many distributed locks over many Redis Masters,
to synchronize on many Network Resources simultaneously, due to the very small overhead associated with
asyncio tasks, and any "waiting" that may need to happen to acquire locks, since all of the above
is happening efficiently on an event-queue.

This example uses 5 Redis databases on the localhost as the Redlock Masters, to synchronize on
the access of a RedisList, where multiple tasks are concurrently synchronizing getting, incrementing and appending
to the last numerical item of that Redis List, with some asyncio delay to simulate real world
latencies and data processing times.

```python
import asyncio
from purse.redlock import Redlock
from purse.collections import RedisList
from redis.asyncio import Redis
from random import random

# The main Redis Store that contains the data that need synchronization
redis_store = Redis(db=0)

# The Redis Masters for the async Redlock
# Highly Recommended to be an odd number of masters: typically 1, 3 or 5 masters
redlock_masters = [Redis(db=x) for x in range(5)]


async def do_job(n):

    rlock = Redlock("redlock:list_lock", redlock_masters)
    rlist = RedisList(redis_store, "redis_list", str)

    for x in range(n):
        async with rlock:
            cl = await rlist.len()

            if cl == 0:
                await rlist.append("0")
                current_num = 0
            else:
                current_num = int(await rlist.getitem(-1))

            # This sleep simulates the processing time of the job - up to 100ms here
            await asyncio.sleep(0.1 * random())

            # Get the job done, which is add 1 to the last number
            current_num += 1

            print(f"the task {asyncio.current_task().get_name()} working on item #: {current_num}")

            await rlist.append(str(current_num))


async def main():
    rlist = RedisList(redis_store, "redis_list", str)
    await rlist.clear()

    # run 10 async threads (or tasks) in parallel, each one to perform 10 increments
    await asyncio.gather(
        *[asyncio.create_task(do_job(10)) for _ in range(10)]
    )

    # should print 0 to 100 in order, which means synchronization has happened
    async for item in rlist:
        print(item)

    return "success"

asyncio.run(main())
```

