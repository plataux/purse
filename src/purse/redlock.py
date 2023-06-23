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

from redis.asyncio import Redis, RedisError

from typing import List, Optional, Literal, Type
from uuid import uuid4
import asyncio
import random

from time import time_ns


class MilliTimer:
    __slots__ = ('st',)

    def __init__(self):
        self.st = time_ns()

    def elapsed(self):
        """
        elapsed time in millis

        :return:
        """
        return (time_ns() - self.st) / 1000_000


class Redlock:
    _acquired_script = None
    _extend_script = None
    _release_script = None

    # some constants

    CLOCK_DRIFT_FACTOR = 0.01
    RETRY_DELAY = 200  # milliseconds
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
                 '_current_extension_count',)

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str) -> None:
        self._key = value

    def __init__(self, key, masters: List[Redis], raise_on_redis_errors=False,
                 auto_release_time: int = AUTO_RELEASE_TIME,
                 num_extensions: int = NUM_EXTENSIONS,
                 context_manager_blocking: bool = True,
                 context_manager_timeout: float = -1,
                 ):
        """
        Create a distributed lock object

        :param key:
        :param masters: a list of redis connection objects
        :param raise_on_redis_errors:
        :param auto_release_time: milliseconds to auto-release the lock if not extended
        :param num_extensions: -1 for infinite extensions
        :param context_manager_blocking: block if locked
        :param context_manager_timeout: optional timeout, or -1 for infinite
        """
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
        self._current_extension_count = 0

        self.__reg_scripts()

    def __reg_scripts(self):
        """
        register LOA scripts with any of the master clients, and store them in class fields
        It seems that this doesn't actually do any networking, but it does assume that all masters
        will be the same redis version?

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
                                raise_on_redis_errors: Optional[bool] = None) -> bool:
        """
        attempt to acquire locks at all Registered Redis Masters

        Return True if the lock is successfully set with all Registered masters
        Return False if the lock was not successfully set with the majority of Redis Masters, or
        Raise an Exception if ``raise_on_redis_errors`` parameter is set

        :param raise_on_redis_errors:
        :return:
        """
        self._uuid = str(uuid4())
        self._current_extension_count = 0

        aws = [asyncio.create_task(self.__acquire_master(m)) for m in self.masters]

        num_masters_acquired, redis_errors = 0, []

        timer = MilliTimer()

        for aw in asyncio.as_completed(aws):
            try:
                # the aw.result() is either true or false or raises an exception
                # in Python arithmetic, True acts as a 1 and False acts as a 0
                res = await aw
                num_masters_acquired += res
            except RedisError as error:
                redis_errors.append(error)
            else:
                if num_masters_acquired > len(self.masters) // 2:
                    validity_time = self.auto_release_time
                    validity_time -= round(self.__drift())
                    validity_time -= timer.elapsed()
                    if validity_time > 0:  # pragma: no cover
                        return True

        with contextlib.suppress(RuntimeError):
            await self.release(raise_on_redis_errors=False)
        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return False

    async def __acquire_master(self, master: Redis) -> bool:
        """

        Attempt to acquire a lock at the one given Redis Master.

        Return True if the lock is successfully set with the given master.
        Return False is the lock was not set, because of NX and key already existed (lock was taken)
        Raise a RedisError otherwise

        :param master:
        :return: ``bool`` True if successful, False if not
        :raises RedisError: if there is an (network) error performing the operation
        """
        acquired = await master.set(
            self.key,
            self._uuid,
            px=self.auto_release_time,
            nx=True,
        )
        return bool(acquired)

    async def __acquired_master(self, master: Redis) -> int:
        """
        return the lock pTTL in milliseconds for a given master
        :param master:
        :return:
        """
        assert self._acquired_script is not None

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
        assert self._extend_script is not None
        extended = await self._extend_script(
            keys=(self.key,),
            args=(self._uuid, self.auto_release_time),
            client=master,
        )
        return bool(extended)

    async def __release_master(self, master: Redis) -> bool:
        assert self._release_script is not None
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
        """



        :param blocking: ``bool`` value, True to block or False to immediately abort is lock is taken
        :param timeout: ``float`` in seconds, or -1 to block indefinitely
        :param raise_on_redis_errors: raise Redis Exceptions

        :return: True for a successfully acquired lock, False otherwise
        """
        re = raise_on_redis_errors

        timer = MilliTimer()

        if blocking:
            while (timeout == -1) or ((timer.elapsed() / 1000) < timeout):
                if await self.__acquire_masters(raise_on_redis_errors=re):
                    return True
                await asyncio.sleep(random.uniform(0, self.RETRY_DELAY / 1000))
            return False

        if timeout == -1:
            return await self.__acquire_masters(raise_on_redis_errors=re)

        raise ValueError("can't specify a timeout for a non-blocking call")

    async def locked(self, *, raise_on_redis_errors: Optional[bool] = None) -> int:
        """
        Determine how many millis left for the lock auto release
        (across all masters) in milliseconds

        :param raise_on_redis_errors:
        :return:
        """

        timer = MilliTimer()

        ttls, redis_errors = [], []

        aws = [asyncio.create_task(self.__acquired_master(m)) for m in self.masters]

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
                        validity_time -= timer.elapsed()
                        return max(validity_time, 0)

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        return 0

    async def extend(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:

        if self.num_extensions <= -1:
            pass
        elif self._current_extension_count >= self.num_extensions:
            raise RuntimeError("Exceeded Number of Extensions cap")

        aws = [asyncio.create_task(self.__extend_master(m)) for m in self.masters]

        num_masters_extended, redis_errors = 0, []

        for aw in asyncio.as_completed(aws):
            try:
                res = await aw  # that's a bool
                num_masters_extended += res
            except RedisError as error:
                redis_errors.append(error)
            else:
                if num_masters_extended > len(self.masters) // 2:
                    self._current_extension_count += 1
                    return

        self._check_enough_masters_up(raise_on_redis_errors, redis_errors)
        raise RuntimeError("Trying to extend an unlocked lock")

    async def release(self, *, raise_on_redis_errors: Optional[bool] = None) -> None:

        num_masters_released, redis_errors = 0, []

        aws = [asyncio.create_task(self.__release_master(m)) for m in self.masters]

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
        raise RuntimeError("Trying to Release a Lock that wasn't Held")

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
