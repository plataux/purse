

from purse.redlock import Redlock
from purse import RedisList
import asyncio
from random import random

import pytest
from ctx import Context


@pytest.fixture(scope="session")
def ctx() -> Context:
    ctx = Context()
    yield ctx


def test_redlock(ctx):
    async def do_job(n):

        rlock = Redlock("redlock:list_lock", [ctx.redis_conn])
        rlist = RedisList(ctx.redis_conn, "redis_list", str)

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
        rlist = RedisList(ctx.redis_conn, "redis_list", str)
        await rlist.clear()

        # run 10 async threads (or tasks) in parallel, each one to perform 10 increments
        await asyncio.gather(
            *[asyncio.create_task(do_job(10)) for _ in range(10)]
        )

        assert [x async for x in rlist] == [str(x) for x in range(101)]

        return "success"

    asyncio.run(main())