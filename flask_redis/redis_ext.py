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
