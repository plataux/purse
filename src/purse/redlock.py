############################################################################
# Parts of this source file have been copied and/or adapted and/or         #
# inspired by source code from:                                            #
# https://github.com/brainix/pottery/tree/master/pottery                   #
############################################################################
# Copyright 2021 Plataux LLC                                               #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#    https://www.apache.org/licenses/LICENSE-2.0                           #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
############################################################################


from __future__ import annotations

import contextlib
from types import TracebackType

from aioredis import Redis, RedisError
from aioredis.client import Script
from typing import List, Optional, Literal, Type
from uuid import uuid4
import asyncio
import random

from time import time_ns


class Redlock:
    _acquired_script: Script = None
    _extend_script: Script = None
    _release_script: Script = None

    # some constants

    CLOCK_DRIFT_FACTOR = 0.01
    RETRY_DELAY = 200
    NUM_EXTENSIONS = 3
    AUTO_RELEASE_TIME = 10_000  # milliseconds

    ###################################

    __slots__ = ('_key',
                 'masters',
                 'raise_on_redis_errors',
                 'auto_release_time',
                 'num_extensions',
                 'context_manager_blocking',
                 'context_manager_timeout',
                 '_uuid',
                 '_extension_num',)

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str) -> None:
        self._key = f'redlock:{value}'

    def __init__(self, key, masters: List, raise_on_redis_errors=False,
                 auto_release_time: int = AUTO_RELEASE_TIME,
                 num_extensions: int = NUM_EXTENSIONS,
                 context_manager_blocking: bool = True,
                 context_manager_timeout: float = -1,
                 ):
        if not context_manager_blocking and context_manager_timeout != -1:
            raise ValueError("can't specify a timeout for a non-blocking call")

        self.key = key
        self.masters = masters
        self.raise_on_redis_errors = raise_on_redis_errors

        self.auto_release_time = auto_release_time
        self.num_extensions = num_extensions
        self.context_manager_blocking = context_manager_blocking
        self.context_manager_timeout = context_manager_timeout
        self._uuid = ''
        self._extension_num = 0

        self.__reg_scripts()

    def __reg_scripts(self):
        """
        register LOA scripts with any of the masters, and store them in class fields
        :return:
        """
        if not self.__class__._acquired_script:
            self.__class__._acquired_script = self.masters[0].register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    local pttl = redis.call('pttl', KEYS[1])
                    return (pttl > 0) and pttl or 0
                else
                    return 0
                end
                        ''')

        if not self.__class__._extend_script:
            self.__class__._extend_script = self.masters[0].register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('pexpire', KEYS[1], ARGV[2])
                else
                    return 0
                end
                        ''')

        if not self.__class__._release_script:
            self.__class__._release_script = self.masters[0].register_script('''
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    return redis.call('del', KEYS[1])
                else
                    return 0
                end
                        ''')

    async def __acquire_masters(self,
                                *,
                                raise_on_redis_errors: Optional[bool] = None,
                                ) -> bool:
        self._uuid = str(uuid4())
        self._extension_num = 0

        ###################################################################

        aws = [self.__acquire_master(m) for m in self.masters]

        num_masters_acquired, redis_errors = 0, []

        st = time_ns()

        def elapsed():
            return (time_ns() - st) / 1000_000

        for aw in asyncio.as_completed(aws):
            try:
                # the aw.result() is either true or false or raises an exception
                # in arithmatic, True acts as a 1 and False acts as a 0
                res = await aw
                num_masters_acquired += res
            except RedisError as error:
                redis_errors.append(error)
            else:
                if num_masters_acquired > len(self.masters) // 2:
                    validity_time = self.auto_release_time
                    validity_time -= round(self.__drift())
                    validity_time -= elapsed()
                    if validity_time > 0:  # pragma: no cover
                        return True

        with contextlib.suppress(RuntimeError):
            await self.release(raise_on_redis_errors=False)
        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return False

        ############################################################

    async def __acquire_master(self, master: Redis) -> bool:
        acquired = await master.set(
            self.key,
            self._uuid,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    async def __acquired_master(self, master: Redis) -> int:
        if self._uuid:
            ttl: int = await self._acquired_script(
                keys=(self.key,),
                args=(self._uuid,),
                client=master,
            )
        else:
            ttl = 0
        return ttl

    async def __extend_master(self, master: Redis) -> bool:
        extended = await self._extend_script(
            keys=(self.key,),
            args=(self._uuid, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    async def __release_master(self, master: Redis) -> bool:
        released = await self._release_script(
            keys=(self.key,),
            args=(self._uuid,),
            client=master,
        )
        return bool(released)

    def __drift(self) -> float:
        return self.auto_release_time * self.CLOCK_DRIFT_FACTOR + 2

    def _check_enough_masters_up(self,
                                 raise_on_redis_errors: Optional[bool],
                                 redis_errors: List[RedisError],
                                 ) -> None:
        if raise_on_redis_errors is None:
            raise_on_redis_errors = self.raise_on_redis_errors
        if raise_on_redis_errors and len(redis_errors) > len(self.masters) // 2:
            raise RuntimeError('not enough consensus')

    async def acquire(self,
                      *,
                      blocking: bool = True,
                      timeout: float = -1,
                      raise_on_redis_errors: Optional[bool] = None,
                      ) -> bool:

        re = raise_on_redis_errors

        st = time_ns()

        def elapsed():
            return (time_ns() - st) / 1000_000

        if blocking:
            while timeout == -1 or ((elapsed() / 1000) < timeout):
                if await self.__acquire_masters(raise_on_redis_errors=re):
                    return True
                await asyncio.sleep(random.uniform(0, self.RETRY_DELAY / 1000))
            return False

        if timeout == -1:
            return await self.__acquire_masters(raise_on_redis_errors=re)

        raise ValueError("can't specify a timeout for a non-blocking call")

    async def locked(self, *, raise_on_redis_errors: Optional[bool] = None) -> int:

        st = time_ns()

        def elapsed():
            return (time_ns() - st) / 1000_000

        ttls, redis_errors = [], []

        aws = [self.__acquired_master(m) for m in self.masters]

        for aw in asyncio.as_completed(aws):
            try:
                ttl = await aw
            except RedisError as error:
                redis_errors.append(error)
            else:
                if ttl:
                    ttls.append(ttl)
                    if len(ttls) > len(self.masters) // 2:  # pragma: no cover
                        validity_time = min(ttls)
                        validity_time -= round(self.__drift())
                        validity_time -= elapsed()
                        return max(validity_time, 0)

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return 0

    async def extend(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:

        if self._extension_num >= self.num_extensions:
            raise RuntimeError(self.key, self.masters)

        aws = [self.__extend_master(m) for m in self.masters]

        num_masters_extended, redis_errors = 0, []

        for aw in asyncio.as_completed(aws):
            try:
                res = await aw  # that's a bool
                num_masters_extended += res
            except RedisError as error:
                redis_errors.append(error)
            else:
                if num_masters_extended > len(self.masters) // 2:
                    self._extension_num += 1
                    return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise RuntimeError("Trying to extend an unlocked lock")

    async def release(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:

        num_masters_released, redis_errors = 0, []

        aws = [self.__release_master(m) for m in self.masters]

        for aw in asyncio.as_completed(aws):
            try:
                res = await aw
                num_masters_released += res
            except RedisError as error:
                redis_errors.append(error)
            else:
                if num_masters_released > len(self.masters) // 2:
                    return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise RuntimeError("ReleaseUnlockedLock")

    async def __aenter__(self) -> 'Redlock':

        acquired = await self.acquire(
            blocking=self.context_manager_blocking,
            timeout=self.context_manager_timeout,
        )
        if acquired:
            return self
        raise RuntimeError("QuorumNotAchieved")

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType],
                        ) -> Literal[False]:
        await self.release()
        return False
