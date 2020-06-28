# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-24 16:54'
# 3p
from redis import Redis

# project
from .macro import K_RDS_PREFIX


class RedisExtension:

    def __init__(self, config):
        """ Initialize redis client with special config
        """
        self._config = config

        # Transform all of the config key with 'REDIS_' to init params
        # e.g. REDIS_HOST -> host
        kwargs = dict()
        for key, value in config.items():
            if not key.startswith("REDIS_"):
                continue
            kwargs[key[6:]] = value.lower()

        self.redis = Redis(**kwargs)

    def _get_key(self, key):
        """ Generate operate key

        :param key: origin key
        """
        rds_prefix = self._config.get(K_RDS_PREFIX)
        if rds_prefix:
            return rds_prefix + key

        return key

    def _get_keys(self, keys):
        """ Generate multi operate keys

        :param keys: multi keys
        """
        return [self._get_key(k) for k in keys]

    def __getattr__(self, item):
        """ Forward undefined method to redis object

        :param item: method name
        """
        return getattr(self.redis, item)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        return self.redis.bitcount(self._get_key(key), start, end)

    def bitfield(self, key, default_overflow=None):
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.
        """
        return self.redis.bitfield(self._get_key(key), default_overflow)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        keys = self._get_keys(keys)
        return self.redis.bitop(operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` difines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        return self.redis.bitpos(self._get_key(key), bit, start, end)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        # An alias for ``decr()``, because it is already implemented
        # as DECRBY redis command.
        return self.redis.decr(self._get_key(name), amount)

    def decrby(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.redis.decrby(self._get_key(name), amount)

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        names = self._get_keys(names)
        return self.redis.delete(*names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return self.redis.dump(self._get_key(name))

    def exists(self, *names):
        """Returns the number of ``names`` that exist"""
        names = self._get_keys(names)
        return self.redis.exists(*names)

    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        return self.redis.expire(self._get_key(name), time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        return self.redis.expireat(self._get_key(name), when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.redis.get(self._get_key(name))

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        return self.get(name)

    def getbit(self, name, offset):
        """Returns a boolean indicating the value of ``offset`` in ``name``"""
        return self.redis.getbit(self._get_key(name), offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.redis.getrange(self._get_key(key), start, end)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.redis.getset(self._get_key(name), value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.redis.incr(self._get_key(name), amount)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.redis.incrby(self._get_key(name), amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return self.redis.incrbyfloat(self._get_key(name), amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.redis.keys(pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        # TODO: Update keys
        return self.redis.mget(keys, *args)

    def mset(self, mapping):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        """
        # TODO: Update keys
        return self.redis.mset(mapping)

    def msetnx(self, mapping):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.
        """
        # TODO: Update keys
        return self.redis.msetnx(mapping)

    def move(self, name, db):
        """Moves the key ``name`` to a different Redis database ``db``"""
        return self.redis.move(self._get_key(name), db)

    def persist(self, name):
        "Removes an expiration on ``name``"
        return self.persist(self._get_key(name))

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        return self.redis.pexpire(self._get_key(name), time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        return self.redis.pexpireat(self._get_key(name), when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        return self.redis.psetex(self._get_key(name), time_ms, value)

    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return self.redis.pttl(self._get_key(name))

    def randomkey(self):
        "Returns the name of a random key"
        # TODO: Update keys
        return self.redis.randomkey()

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.redis.rename(self._get_key(src), self._get_key(dst))

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.redis.renamenx(self._get_key(src), self._get_key(dst))

    def restore(self, name, ttl, value, replace=False):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        return self.redis.restore(self._get_key(name), ttl, value, replace)

    def set(self, name, value,
            ex=None, px=None, nx=False, xx=False, keepttl=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.

        ``keepttl`` if True, retain the time to live associated with the key.
            (Available since Redis 6.0)
        """
        return self.redis.set(self._get_key(name), value, ex, px, nx, xx, keepttl)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        return self.redis.setbit(self._get_key(name), offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return self.redis.setex(self._get_key(name), time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.redis.setnx(self._get_key(name), value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return self.redis.setrange(self._get_key(name), offset, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.redis.strlen(self._get_key(name))

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.redis.substr(self._get_key(name), start, end)

    def touch(self, *args):
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.
        """
        # TODO: Update keys
        return self.redis.touch(*args)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.redis.ttl(self._get_key(name))

    def type(self, name):
        "Returns the type of key ``name``"
        return self.redis.type(self._get_key(name))

    def unlink(self, *names):
        "Unlink one or more keys specified by ``names``"
        names = self._get_keys(names)
        return self.redis.unlink(*names)

    # LIST COMMANDS
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        # TODO: Update keys
        return self.redis.blpop(keys, timeout)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        # TODO: Update keys
        return self.redis.brpop(keys, timeout)

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        return self.redis.brpoplpush(self._get_key(src), self._get_key(dst), timeout)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.redis.lindex(self._get_key(name), index)

    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return self.redis.linsert(self._get_key(name), where, refvalue, value)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.redis.llen(self._get_key(name))

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.redis.lpop(self._get_key(name))

    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"
        return self.redis.lpush(self._get_key(name), *values)

    def lpushx(self, name, value):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return self.redis.lpushx(self._get_key(name), value)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.redis.lrange(self._get_key(name), start, end)

    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.redis.lrem(self._get_key(name), count, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.redis.lset(self._get_key(name), index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.redis.ltrim(self._get_key(name), start, end)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.redis.rpop(self._get_key(name))

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.redis.rpoplpush(self._get_key(src), self._get_key(dst))

    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"
        return self.redis.rpush(self._get_key(name), *values)

    def rpushx(self, name, value):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return self.redis.rpushx(self._get_key(name), value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        return self.redis.sort(self._get_key(name), start, num, by, get, desc, alpha, store, groups)

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None, _type=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        return self.redis.scan(cursor, match, count, _type)

    def scan_iter(self, match=None, count=None, _type=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        return self.redis.scan_iter(match, count, _type)

    def sscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        # TODO: Update keys
        return self.redis.sscan(self._get_key(name), cursor, match, count)

    def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        # TODO: Update keys
        return self.redis.sscan_iter(self._get_key(name), match, count)

    def hscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.redis.hscan(self._get_key(name), cursor, match, count)

    def hscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.redis.hscan_iter(self._get_key(name), match, count)

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self.redis.zscan(self._get_key(name), cursor, match, count, score_cast_func)

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self.redis.zscan_iter(self._get_key(name), match, count, score_cast_func)

    # SET COMMANDS
    def sadd(self, name, *values):
        "Add ``value(s)`` to set ``name``"
        return self.redis.sadd(self._get_key(name), *values)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.redis.scard(self._get_key(name))

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        # TODO: Update keys
        return self.redis.sdiff(keys, *args)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        # TODO: Update keys
        return self.redis.sdiffstore(dest, keys, *args)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        # TODO: Update keys
        return self.redis.sinter(keys, *args)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        # TODO: Update keys
        return self.redis.sinterstore(dest, keys, *args)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.redis.sismember(self._get_key(name), value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.redis.smembers(self._get_key(name))

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.redis.smove(self._get_key(src), self._get_key(dst), value)

    def spop(self, name, count=None):
        "Remove and return a random member of set ``name``"
        return self.redis.spop(self._get_key(name), count)
