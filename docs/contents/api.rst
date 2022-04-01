

API Reference
#############

All collection classes (except for RedisKey, which is a base class that needn't be used directly) require
a mandatory parameter ``value_type``, which is the data type to serialize to and from upon data
retrieval and storage from Redis.

There are two main advantages of explicit type specification when using ``purse`` collections:

* Comprehensive Type Hinting Support.
* automatic serialization and deserialization of objects (in case of ``dict`` and Pydantic Model objects)

The ``value_type`` can be one of the following classes:

* ``str``
* ``bytes``
* ``dict``
* a Pydantic ``BaseModel`` subclass

In case of ``dict`` values, they need to be JSON serializable. ``dict`` values do not need to have a any
schema. Pydantic ``BaseModel`` Objects are similar to dicts in that they are serialized and stored
in JSON strings, but differ in the following way:

* ``BaseModel`` defines a schema, and ensures that objects within a collection are valid to that schema upon serialization and deserialization.
* Auto field conversion and correction whenever possible as the object is being serialized before storage
* Extra field-validation logic can be defined in the ``BaseModel`` subclass

The following example shows an example usage of Pydantic Model in conjunction with a ``RedisSortedSet``
to fully automate the serialization and type validation and type conversion process.

.. code-block:: python

    import asyncio
    from pydantic import BaseModel
    import aioredis
    from purse import RedisSortedSet

    redis_con = aioredis.Redis()


    class Plant(BaseModel):
        name: str
        nutrition: float
        tasty: bool = False


    plants = [
        Plant(name="apples", nutrition=5.0, tasty=0),  # auto convert the number 0 to False
        Plant(name=b"bananas", nutrition=3, tasty=True),  # auto conversion 3 to 3.0
        Plant(name="spinach", nutrition="9.0", tasty=False),  # cast string "9.0" to float 9.0
        Plant(name="tomatoes", nutrition=8, tasty=1),  # auto convert int 1 to True
    ]

    purse_key = '_temp:plants'


    async def main():
        # Set up the Redis key, and the value_type to the Plant Model
        rss = RedisSortedSet(redis_con, purse_key, Plant)
        await rss.clear()

        # Store Plants scored and ranked by nutritional value
        await rss.add_multi([(p, p.nutrition) for p in plants])

        # returns a tuple of the object and its score
        most_nutritious, score = await rss.peak_max()

        print(most_nutritious.name)  # Spinach
        print(most_nutritious.tasty)  # False
        print(most_nutritious.nutrition)  # 9.0


    asyncio.run(main())


RedisKeySpace
*************

A dict-like API to a Redis key space (namespace) based on a  given key prefix,
where values are serialized and stored with Redis ``Strings`` commands.

https://redis.io/commands#string

This class does not accept ``None`` values, and will raise errors if a key is set to ``None``.

The main benefit of this Class compared to RedisHash is key expiration features. This is common
in Web Applications where certain kinds of data is known to remain static for known durations,
for example:

- JWT User Access and Refresh Tokens, that would typically have a predefined expiration
- Client IP Geographic information, would typically change on a weekly or monthly frequency

Applications must ensure that the prefix used by objects of this class does not collide with keys
or prefixes used by other objects in the same Redis database.

.. autoclass:: purse.RedisKeySpace(redis: aioredis.Redis, prefix: str, value_type: Type[T])

    .. automethod:: set

    .. automethod:: get

    .. automethod:: setdefault

    .. automethod:: update

    .. automethod:: pop

    .. automethod:: delete

    .. automethod:: clear

    .. automethod:: ttl

    .. automethod:: pttl

    .. automethod:: expire

    .. automethod:: pexpire

    .. automethod:: persist

    .. automethod:: contains

    .. automethod:: len

    .. automethod:: keys

    .. automethod:: items

    .. automethod:: values

    .. automethod:: __aiter__


RedisHash
*********

A dict-like API to a Redis Hash key type,
where values are serialized and stored with Redis ``Hashes`` commands.

https://redis.io/commands#hash

Mostly Compatible with a Python ``dict``, with the following exceptions

#. Doesn't accept ``None`` for mapping values.

#. Doesn't provide the ``popitem()`` method which relies on the fact that Python dicts are ordered. while redis hash tables are not

#. Doesn't raise ``ValueError`` when accessing unset keys, but rather returns None.

#. the __aiter()__ method provides an AsyncIterator of (key, value) tuples instead a keys iterator.

Differences in point 3 and 4 are intended to eliminate or reduce the need to make extra
Networking round-trips to check for key existence, or get a mapped values during iteration.


.. autoclass:: purse.RedisHash(redis: aioredis.Redis, rkey: str, value_type: Type[T])

    .. automethod:: set

    .. automethod:: get

    .. automethod:: setdefault

    .. automethod:: update

    .. automethod:: pop

    .. automethod:: delete

    .. automethod:: clear

    .. automethod:: contains

    .. automethod:: len

    .. automethod:: keys

    .. automethod:: items

    .. automethod:: values

    .. automethod:: __aiter__