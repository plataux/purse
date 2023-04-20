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

import asyncio
import json
from datetime import timedelta

from redis.asyncio import Redis
from typing import Any, Dict, Type, Union, Tuple, Iterable, List
from typing import TypeVar, Generic
from collections.abc import Mapping, AsyncIterator
from pydantic import BaseModel

from uuid import uuid4

T = TypeVar('T')


def _obj_from_raw(value_type: Type[T], raw_item: str | bytes) -> T | Any:
    if issubclass(value_type, BaseModel):
        return value_type.parse_raw(raw_item)
    elif issubclass(value_type, dict):
        return json.loads(raw_item)
    elif issubclass(value_type, str):
        if isinstance(raw_item, bytes):
            return raw_item.decode()
        else:
            return raw_item
    else:
        if isinstance(raw_item, str):
            return raw_item.encode()
        else:
            return raw_item


def _list_from_raw(value_type: Type[T], raw_list: List[Any]) -> List[T]:
    obj_list: List[T] = []
    if issubclass(value_type, BaseModel):
        for raw_item in raw_list:
            obj: Any = value_type.parse_raw(raw_item)
            obj_list.append(obj)
        return obj_list
    elif issubclass(value_type, dict):
        for raw_item in raw_list:
            obj = json.loads(raw_item)
            obj_list.append(obj)
        return obj_list
    elif issubclass(value_type, str):
        for raw_item in raw_list:
            if isinstance(raw_item, bytes):
                obj = raw_item.decode()
                obj_list.append(obj)
            else:
                obj_list.append(raw_item)
        return obj_list
    else:
        for raw_item in raw_list:
            if isinstance(raw_item, str):
                obj = raw_item.encode()
                obj_list.append(obj)
            else:
                obj_list.append(raw_item)
        return obj_list


def _obj_to_raw(value_type: Type[T], value: T) -> str | bytes:
    if isinstance(value, BaseModel) and isinstance(value, value_type):
        assert isinstance(value, BaseModel)
        return value.json()
    elif isinstance(value, dict):
        return json.dumps(value)
    elif isinstance(value, (str, bytes)):
        return value
    else:
        raise ValueError(
            f"Incorrect type. Expected type: {value_type} "
            f"while give Value type {type(value)}")


def _list_to_raw(value_type: Type[T], members: Iterable[T]) -> List[str | bytes]:
    bucket: List[Union[str, bytes]] = []
    for value in members:
        bucket.append(_obj_to_raw(value_type, value))
    return bucket


def _dict_to_raw(value_type: Type[T], mapping: Mapping[Any, T]) -> Dict[str, str | bytes]:
    bucket: Dict[str, Union[str, bytes]] = {}
    for key, value in mapping.items():
        bucket[key] = _obj_to_raw(value_type, value)
    return bucket


class RedisKeySpace(Generic[T]):
    """
    Set up the key prefix for the redis key space, define the object type to serialize to and from,
    and hold on to Redis client or a connection pool

    :param redis: an ``aioredis`` client or connection pool
    :param prefix: a ``str`` defining the key space. for example ``"app:feature:"``
    :param value_type: can be ``str``, ``bytes``, ``dict`` or a Pydantic ``BaseModel`` subclass

    """
    __slots__ = ('redis', 'prefix', '_value_type')

    def __init__(self, redis: Redis, prefix: str, value_type: Type[T]):
        self.redis = redis
        self.prefix = prefix
        self._value_type: Type[T] = value_type

    async def set(self, key: str, value: T, ex: int | timedelta | None = None,
                  px: int | timedelta | None = None,
                  nx=False, xx=False, keepttl=False):
        """
        Set the given key with the given value, with optional expiration time delta in seconds or millis,
        and other Redis options found here:

        https://redis.io/commands/set

        :param key: A key in unicode or byte string
        :param value: A Value of the type defined when initializing this object
        :param ex: Expiration timedelta in seconds
        :param px: Expiration timedelta in millis
        :param nx: Only set the key if it does not already exist.
        :param xx: Only set the key if it already exists.
        :param keepttl: Retain the time to live associated with the key.
        :return: True if key was set, False if not
        :raises ValueError: if the type mismatches the Generic[T] or is None

        """
        args: Any = [self.prefix + key, _obj_to_raw(self._value_type, value)]

        kwargs: Any = {"nx": nx, "xx": xx, "keepttl": keepttl}

        if ex is not None:
            kwargs["ex"] = ex
        if px is not None:
            kwargs["px"] = px

        return bool(await self.redis.set(*args, **kwargs))

    async def setdefault(self, key: str, value: T, ex: int | timedelta | None = None,
                         px: int | timedelta | None = None,
                         nx=False, xx=False, keepttl=False) -> T:
        """
        Get a mapping if it exists, otherwise set and return the given value with the given options

        :param key: A key in unicode or byte string
        :param value: A Value of the type defined when initializing this object
        :param ex: Expiration timedelta in seconds
        :param px: Expiration timedelta in millis
        :param nx: Only set the key if it does not already exist.
        :param xx: Only set the key if it already exists.
        :param keepttl: Retain the time to live associated with the key.
        :return: the provided value ``T`` if mapping did not already exist, or the deserialized value of the existing mapping otherwise
        """
        raw_item = await self.get(key)

        if raw_item is not None:
            return raw_item

        await self.set(key, value, ex, px, nx, xx, keepttl)
        return value

    async def update(self, mapping: Mapping[str, T], if_none_exist=False) -> bool:
        """
        update current mappings with given mappings with option to abort if any of the provided
        keys already exists

        :param mapping: a dict-like mapping of ``str`` keys and ``T`` values
        :param if_none_exist: ``bool`` value to abort if any of the provided mappings already exists
        :return: True if mappings where updated, False otherwise
        """
        bucket: Dict[str, Union[str, bytes]] = _dict_to_raw(self._value_type, mapping)

        if not if_none_exist:
            return bool(await self.redis.mset({self.prefix + k: v for k, v in bucket.items()}))
        else:
            return bool(await self.redis.msetnx({self.prefix + k: v for k, v in bucket.items()}))

    async def get(self, key: str) -> T | None:
        """
        get a mapping. Unlike a Python dict, this method doesn't raise a KeyError if a mapping
        isn't found. This is useful for reducing the Networking round-trips with the server to check
        on key existence.

        :param key: the mapping key ``str`` NOT including the prefix.
        :return: object value of the generic type ``T``
        :raises ValueError: if the value couldn't be deserialized into the Generic[T]
        """
        raw_item = await self.redis.get(self.prefix + key)
        if raw_item is not None:
            return _obj_from_raw(self._value_type, raw_item)
        else:
            return None

    async def pop(self, key: str) -> T | None:
        """
        get and remove key value mapping if it exists, otherwise return None

        :param key: Redis String key
        :return: value of type ``T``
        """
        raw_item = await self.get(key)

        if raw_item is None:
            return None

        await self.delete(key)
        return raw_item

    async def delete(self, key: str):
        """
        Delete a mapping

        :param key: a Redis String key
        """
        await self.redis.unlink(self.prefix + key)

    async def clear(self) -> int:
        """
        Clear all mappings

        :return: total mappings deleted
        """

        batch = []
        batch_size = 100
        current_batch = 0

        total_deleted = 0

        async for k in self.keys(with_prefix=True):
            batch.append(k)
            current_batch += 1

            if current_batch >= batch_size:
                total_deleted += await self.redis.unlink(*batch)
                current_batch = 0
                batch.clear()
                continue

            total_deleted += await self.redis.unlink(*batch)

        return total_deleted

    async def ttl(self, key: str) -> int:
        """
        Time to Live for the given key in seconds

        :param key:
        :return: round number of seconds until key expires
        """
        res: int = await self.redis.ttl(self.prefix + key)
        return res

    async def pttl(self, key: str) -> int:
        """
        Time to Live for the given key in milliseconds

        :param key:
        :return: round number of milliseconds until key expires
        """
        res: int = await self.redis.pttl(self.prefix + key)
        return res

    async def expire(self, key, seconds: int | timedelta):
        """
        Set new expiration offset from the current time in seconds

        :param key:
        :param seconds: ``int`` or ``timedelta`` expiration offset in seconds
        """
        return await self.redis.expire(self.prefix + key, seconds)

    async def pexpire(self, key, millis: int | timedelta):
        """
        Set new expiration offset from the current time in milliseconds

        :param key:
        :param millis: ``int`` or ``timedelta`` expiration offset in milliseconds
        """
        return await self.redis.pexpire(self.prefix + key, millis)

    async def persist(self, key):
        """
        Remove the existing timeout on key, turning the key from volatile
        (a key with an expire set) to persistent (a key that will never expire as no timeout is associated).

        :param key:
        """
        return await self.redis.persist(self.prefix + key)

    async def contains(self, key: str) -> bool:
        """
        Check if the key mapping if exists

        :param key:
        :return: ``True`` if the mapping exists, ``False`` otherwise
        """
        return bool(await self.redis.exists(self.prefix + key))

    async def len(self) -> int:
        """
        This is client side counting of keys, which is more expensive than the len of a RedisHash
        If the Key Space contains tens of thousands of mappings, this method should be avoided if possible

        :return: ``int`` number of mappings in this collection
        """
        count = 0
        async for _ in self.keys(): count += 1
        return count

    def keys(self, batch_hint=None, with_prefix=False) -> AsyncIterator[str]:
        """
        Return an AsyncIterator of all keys in this collection, with the option to include
        the Redis key prefix

        :param batch_hint: a batch size hint to the Redis Client
        :param with_prefix: ``True`` to return prefix + key, ``False`` to return the keys without prefix
        :return: ``AsyncIterator[str]`` over the collections keys. Can be used with ``async for`` keyword
        """
        if not with_prefix:
            async def _key_no_prefix() -> AsyncIterator[str]:
                prefix_len = len(self.prefix)
                async for kx in self.redis.scan_iter(match=f'{self.prefix}*', count=batch_hint):
                    if isinstance(kx, bytes): kx = kx.decode()
                    yield kx[prefix_len:]

            return _key_no_prefix()
        else:
            return self.redis.scan_iter(match=f'{self.prefix}*', count=batch_hint)

    def items(self, batch_hint=None) -> AsyncIterator[Tuple[str, T]]:
        """
        AsyncIterator of (key, value) tuples of this collection

        :param batch_hint:
        :return: ``AsyncIterator`` of (key, value) tuples of this collection
        """
        async def _pair_iter():
            async for k in self.keys(batch_hint=batch_hint):
                yield k, await self.get(k)

        _iter: AsyncIterator[Tuple[str, T]] = _pair_iter()

        return _iter

    def values(self, batch_hint=None) -> AsyncIterator[T]:
        """
        Provide an AsyncIterator of the mapped values in this collection

        :param batch_hint:
        :return: ``AsyncIterator[T]`` of deserialized values of this collection
        """
        async def _val_iter():
            async for k, v in self.items(batch_hint=batch_hint):
                yield v

        _iter: AsyncIterator[T] = _val_iter()

        return _iter

    def __aiter__(self) -> AsyncIterator[Tuple[str, T]]:
        """
        This magic method Provides an AsyncIterator over the (key, value)
        tuples of the mappings of this collection, unlike a Python dict, which provides an
        ``Iterator`` over the keys. this method is called implicitly when this collection is
        directly used with the ``async for`` keyword, and explicitly with the ``aiter()`` builtin
        Python function in Python 3.10+

        :return: ``AsyncIterator`` of (key, value) tuples of this collection
        """
        return self.items()


class RedisKey:
    """
    Base class for RedisHash, RedisSet, RedisSortedSet, RedisList and all RedisQueue Classes
    """
    __slots__ = ("rkey", "redis")

    def __init__(self, redis: Redis, rkey: str):
        self.redis: Redis = redis
        self.rkey: str = rkey

    async def expire(self, seconds: int | timedelta):
        return await self.redis.expire(self.rkey, seconds)

    async def ttl(self):
        return await self.redis.ttl(self.rkey)

    async def pttl(self):
        return await self.redis.pttl(self.rkey)

    async def persist(self):
        return await self.redis.persist(self.rkey)

    async def dump(self):
        return await self.redis.dump(self.rkey)

    async def restore(self, value, ttl=0, replace=False):
        return await self.redis.restore(self.rkey, value=value, replace=replace, ttl=ttl)

    async def key_type(self):
        return await self.redis.type(self.rkey)

    async def exists(self):
        return await self.redis.exists(self.rkey)

    async def delete_redis_key(self):
        await self.redis.delete(self.rkey)


class RedisHash(Generic[T], RedisKey):
    """
    Set up and store a Redis Key to be used as a Hash key type,
    define the object type to serialize to and from,
    and hold on to Redis client or a connection pool

    :param redis: an ``aioredis`` client or connection pool
    :param rkey: a ``str`` representing the Redis Hash key
    :param value_type: can be ``str``, ``bytes``, ``dict`` or a Pydantic ``BaseModel`` subclass

    """
    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def set(self, key: str, value: T):
        """
        set the hash mapping ``key`` to ``value``

        :param key: ``str`` hash key
        :param value: hash value of the generic type of this RedisHash initialization
        :raises ValueError: if the type mismatches the Generic[T] or is None

        """
        return await self.redis.hset(
            self.rkey, key=key, value=_obj_to_raw(self._value_type, value))

    async def get(self, key: str) -> T | None:
        """
        get the mapping value of the given key

        :param key: ``str`` mapping key
        :return: the value of the generic type T, or None if mapping doesn't exist
        :raises ValueError: if the value couldn't be deserialized into the Generic[T]
        """
        raw_item = await self.redis.hget(self.rkey, key=key)
        if raw_item is not None:
            return _obj_from_raw(self._value_type, raw_item)
        else:
            return None

    async def setdefault(self, key: str, value: T) -> T:
        """
        Get a mapping if it exists, otherwise set and return the given value with the given options

        :return: the provided value ``T`` if mapping did not already exist, or the deserialized value of the existing mapping otherwise
        """
        item = await self.get(key)

        if item is not None:
            return item
        else:
            await self.set(key, value)
            return value

    async def pop(self, key: str) -> T | None:
        """
        get and remove key value mapping if it exists, otherwise return None

        :param key: Redis String key
        :return: value of type ``T``
        """

        pipe: Any
        async with self.redis.pipeline() as pipe:
            pipe.hget(self.rkey, key)
            pipe.hdel(self.rkey, key)
            raw_item = (await pipe.execute())[0]

        if raw_item is None:
            return raw_item
        else:
            return _obj_from_raw(self._value_type, raw_item)

    async def update(self, mapping: Mapping[Any, T]):
        """
        Updating the collection with mappings of another dict-like object

        :param mapping: a Python dict
        """
        return await self.redis.hset(self.rkey, mapping=_dict_to_raw(self._value_type, mapping))

    async def delete(self, key: str):
        """
        delete a hash mapping given the hash key

        :param key:
        :return:
        """
        return await self.redis.hdel(self.rkey, key)

    async def clear(self):
        return await self.redis.delete(self.rkey)

    async def contains(self, key: str) -> bool:
        return bool(await self.redis.hexists(self.rkey, key))

    async def len(self) -> int:
        d: int = await self.redis.hlen(self.rkey)
        return d

    async def dict(self) -> Dict[str, T]:
        """
        loads the entire RedisHash in a python dict.
        This may not be very suitable for a huge collection.
        For very large collections, use ``RedisHash.items()`` to iterate over the
        hash without overwhelming the server's or the client's RAM

        :return: the Redis Hash into a python dict
        """

        raw: Dict[bytes, Union[str, bytes]] = await self.redis.hgetall(self.rkey)

        bucket: Dict[str, Any] = {}

        if issubclass(self._value_type, BaseModel):
            for k, v in raw.items():
                bucket[k.decode()] = self._value_type.parse_raw(v)
        elif issubclass(self._value_type, dict):
            for k, v in raw.items():
                bucket[k.decode()] = json.loads(v)
        elif issubclass(self._value_type, str):
            for k, v in raw.items():
                if isinstance(v, bytes):
                    bucket[k.decode()] = v.decode()
                else:
                    bucket[k.decode()] = v
        else:
            for k, v in raw.items():
                if isinstance(v, str):
                    bucket[k.decode()] = v.encode()
                else:
                    bucket[k.decode()] = v

        return bucket

    def keys(self, match=None, batch_hint=None) -> AsyncIterator[str]:
        async def _key_iter() -> AsyncIterator[str]:
            async for k, _ in self.items(match, batch_hint):
                yield k

        return _key_iter()

    def values(self, match=None, batch_hint=None) -> AsyncIterator[T]:
        async def _val_iter() -> AsyncIterator[T]:
            async for _, val in self.items(match, batch_hint):
                yield val

        return _val_iter()

    def items(self, match=None, batch_hint=None) -> AsyncIterator[Tuple[str, T]]:
        """
        Usage example:
            incrementally obtain entries from the server.
            Useful for not overloading either the server or the client when the result set
            is very large

        Usage similar to normal dicts, with the async for construct

        .. code-block::

            rc = {}
            async for k, v in rd.items():
                rc[k] = v

        You can also provide a matching string for the keys of the hash

        .. code-block::

            rc = {}
            async for k, v in rd.items(match="10*"):
                rc[k] = v

        :param match: ``str`` or ``bytes`` pattern, optionally with the globbing ``*`` symbol
        :param batch_hint: ``int`` hint to the server of the minimum number of results in
            each batch
        :return: AsyncIterator to be used with ``async for`` constructs
        """

        raw_it = self.redis.hscan_iter(
            self.rkey, match=match, count=batch_hint)

        if issubclass(self._value_type, BaseModel):
            model_type = self._value_type

            async def _typed_iter():
                async for k1, v1 in raw_it:
                    yield k1.decode(), model_type.parse_raw(v1)

        elif issubclass(self._value_type, dict):
            async def _typed_iter():
                async for k2, v2 in raw_it:
                    yield k2.decode(), json.loads(v2)

        elif issubclass(self._value_type, str):

            async def _typed_iter():
                async for k3, v3 in raw_it:
                    if isinstance(v3, bytes):
                        yield k3.decode(), v3.decode()
                    else:
                        yield k3, v3

        else:

            async def _typed_iter():
                async for k4, v4 in raw_it:
                    if isinstance(v4, str):
                        yield k4.decode(), v4.encode()
                    else:
                        yield k4.decode(), v4

        _item_iter: AsyncIterator[Tuple[str, T]] = _typed_iter()

        return _item_iter

    def __aiter__(self) -> AsyncIterator[Tuple[str, T]]:
        """
        Unlike a dict, the iterator magic method
        (which can be obtained with aiter() builtin in Python 3.10) provides an iterator over the
        key, value tuples instead of the keys alone, to reduce the need to make extra network
        round-trips to get the corresponding mapped values

        usage would look like

        .. code-block:: python

            async for k, v  in redis_hash:
                print(k, v)

        which is equivalent to

        .. code-block:: python

            async for k, v  in redis_hash.items():
                print(k, v)

        :return: an AsyncIterator of tuples: ``(key, value)``
        """
        return self.items()


class RedisSet(Generic[T], RedisKey):
    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def add(self, member: T):
        return await self.update(member)

    async def update(self, *members: T):
        return await self.redis.sadd(self.rkey, *_list_to_raw(self._value_type, members))

    async def clear(self):
        return await self.redis.delete(self.rkey)

    async def remove(self, *members: T):
        return await self.redis.srem(self.rkey, *_list_to_raw(self._value_type, members))

    async def contains(self, member: T) -> bool:
        c: bool = await self.redis.sismember(self.rkey, _obj_to_raw(self._value_type, member))
        return c

    async def len(self):
        return await self.redis.scard(self.rkey)

    def values(self, match: Union[str, None] = None, batch_hint=None) -> AsyncIterator[T]:
        kwargs = {"count": batch_hint}

        if not match:
            kwargs["match"] = match

        raw_it = self.redis.sscan_iter(self.rkey, **kwargs)

        if issubclass(self._value_type, BaseModel):
            model_type = self._value_type

            async def _typed_iter():
                async for v1 in raw_it:
                    yield model_type.parse_raw(v1)

        elif issubclass(self._value_type, dict):
            async def _typed_iter():
                async for v2 in raw_it:
                    yield json.loads(v2)

        elif issubclass(self._value_type, str):

            async def _typed_iter():
                async for v3 in raw_it:
                    if isinstance(v3, bytes):
                        yield v3.decode()
                    else:
                        yield v3

        else:

            async def _typed_iter():
                async for v4 in raw_it:
                    if isinstance(v4, str):
                        yield v4.encode()
                    else:
                        yield v4

        _iter: AsyncIterator[T] = _typed_iter()

        return _iter

    def __aiter__(self) -> AsyncIterator[T]:
        return self.values()


class RedisSortedSet(Generic[T], RedisKey):
    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def add_multi(self, members: List[Tuple[T, float]], nx=False, xx=False, ch=False):
        raw_members: Dict[Any, float] = {_obj_to_raw(self._value_type, k): v for k, v in members}
        return await self.redis.zadd(self.rkey, raw_members, nx=nx, xx=xx, ch=ch)

    async def add(self, member: Tuple[T, float], nx=False, xx=False, ch=False):
        raw_members: Dict[Any, float] = {_obj_to_raw(self._value_type, member[0]): member[1]}
        return await self.redis.zadd(self.rkey, raw_members, nx=nx, xx=xx, ch=ch)

    async def increment_multi(self, members: List[Tuple[T, float]]) -> List[Tuple[T, float]]:
        cx = len(members)

        k: Any

        if cx == 1:
            k, v = members[0]
            new_score = await self.redis.zincrby(self.rkey, v, _obj_to_raw(self._value_type, k))
            return [(k, new_score)]

        elif cx > 1:
            raw_members: Dict[Any, float] = {_obj_to_raw(self._value_type, k): v for k, v in members}

            pipe: Any
            async with self.redis.pipeline(transaction=True) as pipe:
                for k, v in raw_members.items():
                    pipe = pipe.zincrby(self.rkey, v, k)
                res = await pipe.execute()
            return [(m[0], r) for m, r in zip(members, res)]

        else:
            raise ValueError("bad members argument")

    async def increment(self, member: Tuple[T, float]) -> Tuple[T, float]:
        k, v = member
        new_score = await self.redis.zincrby(self.rkey, v, _obj_to_raw(self._value_type, k))
        return k, new_score

    async def remove(self, *members: T):
        return await self.redis.zrem(self.rkey, *_list_to_raw(self._value_type, members))

    async def score_multi(self, members: List[T]) -> List[Tuple[T, float]]:
        """
        provide the score of a single SortedSet member, or multiple members at once.

        aioredis 2.0 doesn't implement the ZMSCORE command yet, so we invoking them
        in a pipeline instead

        :param members:
        :return: either a float score for a single SortedSet Member, or a Dict of scores
        """

        if len(members) == 1:
            score = await self.redis.zscore(self.rkey, _obj_to_raw(self._value_type, members[0]))
            return [(members[0], score)]

        if len(members) > 1:
            pipe: Any
            async with self.redis.pipeline(transaction=False) as pipe:
                for m in _list_to_raw(self._value_type, members):
                    pipe = pipe.zscore(self.rkey, m)
                res = await pipe.execute()
            return [(m, v) for m, v in zip(members, res)]
        else:
            raise ValueError("invalid empty members list")

    async def score(self, member: T) -> float:
        res: float = await self.redis.zscore(self.rkey, _obj_to_raw(self._value_type, member))
        return res

    async def rank(self, member: T, descending=False) -> int:
        if not descending:
            r: int = await self.redis.zrank(self.rkey, _obj_to_raw(self._value_type, member))
        else:
            r = await self.redis.zrevrank(self.rkey, _obj_to_raw(self._value_type, member))
        return r

    async def slice_by_rank(self, min_rank: int, max_rank: int,
                            descending=False) -> List[Tuple[T, float]]:

        raw_result = await self.redis.zrange(
            self.rkey, start=min_rank, end=max_rank,
            desc=descending, withscores=True)

        r1: List[Tuple[T, float]] = [(_obj_from_raw(self._value_type, k), v) for k, v in raw_result]
        return r1

    async def slice_by_score(self, min_score: float,
                             max_score: float, offset=None, count=None,
                             descending=False) -> List[Tuple[T, float]]:

        if not descending:
            raw_result = await self.redis.zrangebyscore(self.rkey, min=min_score, max=max_score,
                                                        start=offset, num=count,
                                                        withscores=True)
        else:
            raw_result = await self.redis.zrevrangebyscore(self.rkey, min=max_score,
                                                           max=min_score,
                                                           start=offset, num=count,
                                                           withscores=True)

        r1: List[Tuple[T, float]] = [(_obj_from_raw(self._value_type, k), v) for k, v in raw_result]
        return r1

    async def clear(self):
        return await self.redis.delete(self.rkey)

    async def len(self):
        return await self.redis.zcard(self.rkey)

    async def pop_max(self, count=1) -> List[Tuple[T, float]]:
        raw_result: List[Tuple[Any, Any]] = await self.redis.zpopmax(self.rkey, count=count)
        result: List[Tuple[T, float]] = []
        for k, v in raw_result:
            result.append((_obj_from_raw(self._value_type, k), v))
        return result

    async def pop_min(self, count=1) -> List[Tuple[T, float]]:
        raw_result: List[Tuple[Any, Any]] = await self.redis.zpopmin(self.rkey, count=count)
        result: List[Tuple[T, float]] = []
        for k, v in raw_result:
            result.append((_obj_from_raw(self._value_type, k), v))
        return result

    async def peak_max(self) -> Tuple[T, float]:
        return (await self.slice_by_rank(min_rank=0, max_rank=0, descending=True))[0]

    async def peak_min(self) -> Tuple[T, float]:
        return (await self.slice_by_rank(min_rank=0, max_rank=0, descending=False))[0]

    async def blocking_pop_min(self, timeout=0) -> Tuple[T, float]:
        val = await self.redis.bzpopmin(keys=[self.rkey], timeout=timeout)
        return _obj_from_raw(self._value_type, val[1]), val[2]

    async def blocking_pop_max(self, timeout=0) -> Tuple[T, float]:
        val = await self.redis.bzpopmax(keys=[self.rkey], timeout=timeout)
        return _obj_from_raw(self._value_type, val[1]), val[2]

    def values(self, match=None, batch_hint=None) -> AsyncIterator[Tuple[T, float]]:
        kwargs = {"count": batch_hint}

        if not match:
            kwargs["match"] = match

        raw_it = self.redis.zscan_iter(self.rkey, **kwargs)
        value_type: Type[T] = self._value_type

        if issubclass(value_type, BaseModel):
            model_type = value_type

            async def _typed_iter():
                async for v1, s1 in raw_it:
                    yield model_type.parse_raw(v1), s1

        elif issubclass(value_type, dict):

            async def _typed_iter():
                async for v2, s2 in raw_it:
                    yield json.loads(v2), s2

        elif issubclass(value_type, str):

            async def _typed_iter():
                async for v3, s3 in raw_it:
                    if isinstance(v3, bytes):
                        yield v3.decode(), s3
                    else:
                        yield v3, s3

        else:

            async def _typed_iter():
                async for v4, s4 in raw_it:
                    if isinstance(v4, str):
                        yield v4.encode(), s4
                    else:
                        yield v4, s4

        _iter: AsyncIterator[Tuple[T, float]] = _typed_iter()

        return _iter

    def __aiter__(self) -> AsyncIterator[Tuple[T, float]]:
        return self.values()


class RedisList(Generic[T], RedisKey):
    """
    acts as a list, and as a deque
    """

    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def clear(self):
        await self.redis.delete(self.rkey)

    async def append(self, item: T):
        return await self.redis.rpush(self.rkey, _obj_to_raw(self._value_type, item))

    async def appendleft(self, item: T):
        return await self.redis.lpush(self.rkey, _obj_to_raw(self._value_type, item))

    async def extend(self, items: Iterable[T]):
        return await self.redis.rpush(self.rkey, *_list_to_raw(self._value_type, items))

    async def extendleft(self, items: Iterable[T]):
        return await self.redis.lpush(self.rkey, *_list_to_raw(self._value_type, items))

    async def insert(self, index: int, item: T):

        raw_value = _obj_to_raw(self._value_type, item)

        if index == 0:
            await self.redis.lpush(self.rkey, raw_value)
            return

        pipe: Any
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.llen(self.rkey)
            pipe.lindex(self.rkey, index)
            list_len, pivot = await pipe.execute()

        if index >= list_len:
            await self.redis.rpush(self.rkey, raw_value)
            return

        if pivot is None:
            raise ValueError("unexpected pivot value: None")

        while index < 0:
            index += list_len

        async with self.redis.pipeline(transaction=True) as pipe:
            # pipe.multi()
            uid = str(uuid4())
            pipe.lset(self.rkey, index, uid)
            pipe.linsert(self.rkey, 'BEFORE', uid, raw_value)
            pipe.lset(self.rkey, index + 1, pivot)
            await pipe.execute()

    async def setitem(self, index: int, value: T):
        res = await self.redis.lset(self.rkey, index, _obj_to_raw(self._value_type, value))
        return res

    async def getitem(self, index: int) -> T:
        res = await self.redis.lindex(self.rkey, index)

        if res is None:
            raise IndexError("RedisList index out of range")

        return _obj_from_raw(self._value_type, res)

    async def pop(self) -> T:
        res = await self.redis.rpop(self.rkey)

        return _obj_from_raw(self._value_type, res)

    async def popleft(self) -> T:
        res = await self.redis.lpop(self.rkey)
        return _obj_from_raw(self._value_type, res)

    async def remove(self, value: T, count: int = 0):
        return self.redis.lrem(self.rkey, count, _obj_to_raw(self._value_type, value))

    async def len(self):
        return await self.redis.llen(self.rkey)

    async def contains(self, value: T) -> bool:
        idx = await self.redis.lpos(self.rkey, _obj_to_raw(self._value_type, value))
        return bool(idx)

    async def index(self, value: T) -> int | None:
        idx: int | None = await self.redis.lpos(self.rkey, _obj_to_raw(self._value_type, value))
        return idx

    async def slice(self, start: int, stop: int) -> List[T]:
        raw_res = await self.redis.lrange(self.rkey, start, stop - 1)
        return _list_from_raw(self._value_type, raw_res)

    def values(self, batch_size: Union[int, None] = 10) -> AsyncIterator[T]:

        async def _typed_iter():
            if not batch_size or (list_len := await self.len()) <= batch_size:
                items = await self.redis.lrange(self.rkey, 0, -1)
                for item in items:
                    yield _obj_from_raw(self._value_type, item)
            else:
                full_batches, remainder = divmod(list_len, batch_size)

                last_pos = 0
                for _ in range(full_batches):
                    items = await self.redis.lrange(self.rkey, last_pos, last_pos + batch_size - 1)
                    for item in items:
                        yield _obj_from_raw(self._value_type, item)
                    last_pos += batch_size

                if remainder:
                    items = await self.redis.lrange(self.rkey, last_pos, -1)
                    for item in items:
                        yield _obj_from_raw(self._value_type, item)

        _iter: AsyncIterator[T] = _typed_iter()

        return _iter

    def __aiter__(self) -> AsyncIterator[T]:
        return self.values()


class RedisQueue(Generic[T], RedisKey):
    """
    acts as a Python SimpleQueue
    """

    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def put(self, item: T):
        return await self.redis.lpush(self.rkey, _obj_to_raw(self._value_type, item))

    async def get(self, timeout: float = 0) -> T:
        t: Any = timeout
        _, res = await self.redis.brpop(self.rkey, timeout=t)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res)

    async def get_nowait(self) -> T:
        res = await self.redis.rpop(self.rkey)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res)

    async def qsize(self):
        return await self.redis.llen(self.rkey)


class RedisLifoQueue(Generic[T], RedisKey):
    """
    acts as a Python LifeQueue
    """

    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def put(self, item: T):
        return await self.redis.rpush(self.rkey, _obj_to_raw(self._value_type, item))

    async def get(self, timeout: float = 0) -> T:
        t: Any = timeout
        _, res = await self.redis.brpop(self.rkey, timeout=t)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res)

    async def get_nowait(self) -> T:
        res = await self.redis.rpop(self.rkey)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res)

    async def qsize(self):
        return await self.redis.llen(self.rkey)


class RedisPriorityQueue(Generic[T], RedisKey):
    """
    acts as a Python PriorityQueue
    """

    __slots__ = ('_value_type',)

    def __init__(self, redis: Redis, rkey: str, value_type: Type[T]):
        super().__init__(redis, rkey)
        self._value_type: Type[T] = value_type

    async def put(self, item: Tuple[T, int]):
        raw = str(_obj_to_raw(self._value_type, item[0]))
        return await self.redis.zadd(self.rkey, {f"{uuid4()}:{raw}": item[1]})

    async def get(self, timeout: float = 0) -> Tuple[T, int]:
        t: Any = timeout
        res = await self.redis.bzpopmin(self.rkey, timeout=t)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res[1][37:]), int(res[2])

    async def get_nowait(self) -> Tuple[T, int]:
        _, res = await self.redis.zpopmin(self.rkey)

        if res is None:
            raise asyncio.QueueEmpty("RedisQueue Empty")

        return _obj_from_raw(self._value_type, res[1][37:]), int(res[2])

    async def qsize(self):
        return await self.redis.zcard(self.rkey)
