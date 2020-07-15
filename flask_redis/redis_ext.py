# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-24 16:54'
# sys
import logging

# 3p
from redis import Redis

# project
from .macro import K_RDS_PREFIX, K_RDS_URL


log = logging.getLogger(__name__)


class RedisExtension:

    def __init__(self, config):
        """ Initialize redis client with special config
        """
        self._config = config
        self.rds_prefix = self._config.pop(K_RDS_PREFIX, None)

        # Transform all of the config key with 'REDIS_' to init params
        # e.g. REDIS_HOST -> host
        kwargs = dict()
        for key, value in config.items():
            if not key.startswith("REDIS_"):
                continue
            kwargs[key[6:].lower()] = value

        rds_url = config.pop(K_RDS_URL, None)
        if rds_url:
            del kwargs["url"]
            self.redis = Redis.from_url(rds_url, **kwargs)
        else:
            self.redis = Redis(**kwargs)

    def _get_key(self, key):
        """ Generate operate key

        :param key: origin key
        """
        if not key or not isinstance(key, str):
            raise ValueError("Invalid redis cache key")

        if self.rds_prefix and key.startswith("-"):
            return key[1:]

        if self.rds_prefix:
            new_key = self.rds_prefix + key
            log.debug("cache key transform to '%s'", new_key)
            return new_key
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
        new_keys = self._get_keys(keys)
        new_args = self._get_keys(args)
        return self.redis.mget(new_keys, *new_args)

    def mset(self, mapping):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        """
        items = dict()
        for key, value in mapping.items():
            items[self._get_key(key)] = value
        return self.redis.mset(items)

    def msetnx(self, mapping):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.
        """
        items = dict()
        for key, value in mapping.items():
            items[self._get_key(key)] = value
        return self.redis.msetnx(items)

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

    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        members of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        return self.redis.srandmember(self._get_key(name), number)

    def srem(self, name, *values):
        "Remove ``values`` from set ``name``"
        return self.redis.srem(self._get_key(name), *values)

    def sunion(self, keys, *args):
        "Return the union of sets specified by ``keys``"
        # TODO: Update keys
        return self.redis.sunion(keys, *args)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        # TODO: Update Keys
        return self.redis.sunionstore(dest, keys, *args)

    def xack(self, name, groupname, *ids):
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowlege.
        """
        # TODO: Update Keys
        return self.redis.xack(name, groupname, *ids)

    def xadd(self, name, fields, id='*', maxlen=None, approximate=True):
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size
        approximate: actual stream length may be slightly more than maxlen

        """
        # TODO: Update keys
        return self.redis.xadd(name, fields, id, maxlen, approximate)

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids,
               idle=None, time=None, retrycount=None, force=False,
               justid=False):
        """
        Changes the ownership of a pending message.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds
        message_ids: non-empty list or tuple of message IDs to claim
        idle: optional. Set the idle time (last time it was delivered) of the
         message in ms
        time: optional integer. This is the same as idle but instead of a
         relative amount of milliseconds, it sets the idle time to a specific
         Unix time (in milliseconds).
        retrycount: optional integer. set the retry counter to the specified
         value. This counter is incremented every time a message is delivered
         again.
        force: optional boolean, false by default. Creates the pending message
         entry in the PEL even if certain specified IDs are not already in the
         PEL assigned to a different client.
        justid: optional boolean, false by default. Return just an array of IDs
         of messages successfully claimed, without returning the actual message
        """
        return self.redis.xclaim(name, groupname, consumername, min_idle_time, message_ids, idle, time,
                                 retrycount, force, justid)

    def xdel(self, name, *ids):
        """
        Deletes one or more messages from a stream.
        name: name of the stream.
        *ids: message ids to delete.
        """
        return self.redis.xdel(self._get_key(name), *ids)

    def xgroup_create(self, name, groupname, id='$', mkstream=False):
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        # TODO: Update keys
        return self.xgroup_create(name, groupname, id, mkstream)

    def xgroup_delconsumer(self, name, groupname, consumername):
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete
        """
        # TODO: Update keys
        return self.redis.xgroup_delconsumer(name, groupname, consumername)

    def xgroup_destroy(self, name, groupname):
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        # TODO: Update keys
        return self.redis.xgroup_destroy(name, groupname)

    def xgroup_setid(self, name, groupname, id):
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        # TODO: Update keys
        return self.redis.xgroup_setid(name, groupname, id)

    def xinfo_consumers(self, name, groupname):
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        # TODO: Update keys
        return self.redis.xinfo_consumers(name, groupname)

    def xinfo_groups(self, name):
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.
        """
        # TODO: Update keys
        return self.redis.xinfo_groups(name)

    def xinfo_stream(self, name):
        """
        Returns general information about the stream.
        name: name of the stream.
        """
        # TODO: Update keys
        return self.redis.xinfo_stream(name)

    def xlen(self, name):
        """
        Returns the number of elements in a given stream.
        """
        # TODO: Update keys
        return self.redis.xlen(name)

    def xpending(self, name, groupname):
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        # TODO: Update keys
        return self.redis.xpending(name, groupname)

    def xpending_range(self, name, groupname, min, max, count,
                       consumername=None):
        """
        Returns information about pending messages, in a range.
        name: name of the stream.
        groupname: name of the consumer group.
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).
        """
        # TODO: Update keys
        return self.redis.xpending_range(name, groupname, min, max, count, consumername)

    def xrange(self, name, min='-', max='+', count=None):
        """
        Read stream values within an interval.
        name: name of the stream.
        start: first stream ID. defaults to '-',
               meaning the earliest available.
        finish: last stream ID. defaults to '+',
                meaning the latest available.
        count: if set, only return this many items, beginning with the
               earliest available.
        """
        # TODO: Update keys
        return self.redis.xrange(name, min, max, count)

    def xread(self, streams, count=None, block=None):
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        """
        # TODO: Update keys
        return self.redis.xread(streams, count, block)

    def xreadgroup(self, groupname, consumername, streams, count=None,
                   block=None, noack=False):
        """
        Read from a stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        """
        # TODO: Update keys
        return self.redis.xreadgroup(groupname, consumername, streams, count, block, noack)

    def xrevrange(self, name, max='+', min='-', count=None):
        """
        Read stream values within an interval, in reverse order.
        name: name of the stream
        start: first stream ID. defaults to '+',
               meaning the latest available.
        finish: last stream ID. defaults to '-',
                meaning the earliest available.
        count: if set, only return this many items, beginning with the
               latest available.
        """
        # TODO: Update keys
        return self.redis.xrevrange(name, max, min, count)

    def xtrim(self, name, maxlen, approximate=True):
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        approximate: actual stream length may be slightly more than maxlen
        """
        # TODO: Update keys
        return self.redis.xtrim(name, maxlen, approximate)

    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False):
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.
        """
        # TODO: Update keys
        return self.redis.zadd(self._get_key(name), mapping, nx, xx, ch, incr)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.redis.zcard(self._get_key(name))

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.redis.zcount(self._get_key(name), min, max)

    def zincrby(self, name, amount, value):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.redis.zincrby(self._get_key(name), amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        # TODO: Update keys
        return self.redis.zinterstore(dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.redis.zlexcount(self._get_key(name), min, max)

    def zpopmax(self, name, count=None):
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.
        """
        return self.redis.zpopmax(self._get_key(name), count)

    def zpopmin(self, name, count=None):
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.
        """
        return self.redis.zpopmin(self._get_key(name), count)

    def bzpopmax(self, keys, timeout=0):
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        # TODO: Update keys
        return self.redis.bzpopmax(keys, timeout)

    def bzpopmin(self, keys, timeout=0):
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        # TODO: Update keys
        return self.redis.bzpopmin(keys, timeout)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        # TODO: Update keys
        return self.redis.zrange(name, start, end, desc, withscores, score_cast_func)

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        # TODO: Update keys
        return self.redis.zrangebylex(name, min, max, start, num)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        # TODO: Update keys
        return self.redis.zrevrangebylex(name, max, min, start, num)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        return self.redis.zrangebyscore(self._get_key(name), min, max, start, num, withscores, score_cast_func)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.redis.zrank(self._get_key(name), value)

    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"
        return self.redis.zrem(self._get_key(name), *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return self.redis.zremrangebylex(self._get_key(name), min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.redis.zremrangebyrank(self._get_key(name), min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.redis.zremrangebyscore(self._get_key(name), min, max)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self.redis.zrevrange(self._get_key(name), start, end, withscores, score_cast_func)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        return self.redis.zrevrangebyscore(self._get_key(name), max, min, start, num, withscores, score_cast_func)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.redis.zrevrank(self._get_key(name), value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.redis.zscore(self._get_key(name), value)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        # TODO: Update keys
        return self.redis.zunionstore(dest, keys, aggregate)

    def pfadd(self, name, *values):
        "Adds the specified elements to the specified HyperLogLog."
        return self.redis.pfadd(self._get_key(name), *values)

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return self.redis.pfcount(*sources)

    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        return self.redis.pfmerge(self._get_key(dest), *sources)

    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return self.redis.hdel(self._get_key(name), *keys)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.redis.hexists(self._get_key(name), self._get_key(key))

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.redis.hget(self._get_key(name), self._get_key(key))

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.redis.hgetall(self._get_key(name))

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.redis.hincrby(name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return self.redis.hincrbyfloat(name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.redis.hkeys(self._get_key(name))

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.redis.hlen(self._get_key(name))

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.redis.hset(name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.redis.hsetnx(name, key, value)

    def hmset(self, name, mapping):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        return self.redis.hmset(name, mapping)

    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        return self.redis.hmget(name, keys, *args)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.redis.hvals(name)

    def hstrlen(self, name, key):
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        return self.redis.hstrlen(name, key)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.redis.publish(channel, message)

    def pubsub_channels(self, pattern='*'):
        """
        Return a list of channels that have at least one subscriber
        """
        return self.redis.pubsub_channels(pattern)

    # GEO COMMANDS
    def geoadd(self, name, *values):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.
        """
        return self.redis.geoadd(self._get_key(name), *values)

    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        return self.redis.geodist(name, place1, place2, unit)

    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.
        """
        return self.redis.geohash(self._get_key(name), *values)

    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.
        """
        return self.redis.geopos(self._get_key(name), *values)

    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.
        """
        return self.redis.georadius(name, longitude, latitude, radius, unit,
                                    withdist, withcoord, withhash, count,
                                    sort, store, store_dist)

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.
        """
        return self.redis.georadiusbymember(name, member, radius, unit,
                                            withdist, withcoord, withhash,
                                            count, sort, store, store_dist)
