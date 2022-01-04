# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/8/10 5:32 下午'
# sys
import decimal
import hashlib
import json
import typing as t
from datetime import datetime, date
from inspect import getfullargspec
from functools import partial, wraps
# 3p
from redis import StrictRedis, ConnectionPool
from redis.commands.core import (
    ACLCommands,
    ManagementCommands,
    HashCommands,
)
# project
from .macro import (
    K_INTERNAL_IGNORE_CACHE,
    K_RDS_BINDS,
    K_RDS_IGNORE_CACHE,
    K_RDS_PREFIX,
    K_RDS_URL,
)
# check
if t.TYPE_CHECKING:
    from flask import Flask
try:
    from bson import ObjectId
except ImportError:
    ObjectId = None


def _md5(ori_str):
    """ MD5加密算法

    :param ori_str: 原始字符串
    :return: 加密后的字符串
    """
    md5_obj = hashlib.md5()
    md5_obj.update(ori_str.encode("utf8"))
    return md5_obj.hexdigest()


class JSONEncoder(json.JSONEncoder):

    def default(self, o):  # pylint: disable=method-hidden

        # 处理datetime
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        # 处理日期
        if isinstance(o, date):
            return o.strftime("%Y-%m-%d")

        # 处理decimal
        if isinstance(o, decimal.Decimal):
            return float(o)

        if ObjectId and isinstance(o, ObjectId):
            return str(o)

        # 其它默认处理
        return json.JSONEncoder.default(self, o)


class _RedisExt(StrictRedis):

    __valid_keys = {
        "name",
        "key",
        "names",
        "keys",
        "src",
        "dst",
        "dest",
        "mapping",
        "store"
    }

    def __init__(self, config):
        """ Initialize redis client with special config
        """
        self._config = config
        self._rds_prefix = self._config.pop(K_RDS_PREFIX, None)

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
            connection_pool = ConnectionPool.from_url(rds_url, **kwargs)
            super().__init__(connection_pool=connection_pool)
        else:
            super().__init__(**kwargs)

        self._hash_commands = {v for v in dir(HashCommands) if not v.startswith("_")}
        self._ignore_commands = self.__ignore_commands()

        self.__partial_methods()

    @staticmethod
    def __ignore_commands():
        """ Find commend should be ignored
        """
        commands = {v for v in dir(ACLCommands) if not v.startswith("_")}
        commands.update({v for v in dir(ManagementCommands) if not v.startswith("_")})
        return commands

    def __partial_methods(self):
        """ Bind origin redis method to instance
        """
        for method in dir(self):
            if method.startswith("_"):
                continue

            # ignore special function
            if method in {"cached", "incr", "decr"}:
                continue

            # ignore acl commands
            if method in self._ignore_commands:
                continue

            func = getattr(self, method)
            try:
                info = getfullargspec(func)
                args_key, var_args = info.args, info.varargs
            except TypeError:
                continue

            valid_keys = self.__valid_keys & set(args_key + [var_args])
            if not valid_keys:
                continue

            setattr(self, "__old_%s" % method, func)
            setattr(self, method, partial(self.__decorator, method, args_key[1:], var_args))

    def __decorator(self, func, keys, var_args, *args, **kwargs):
        kw = dict(zip(keys, args), **kwargs)
        # check *args exists
        if var_args and len(keys) < len(args):
            kw[var_args] = args[len(keys):]

        # rebuild cache keys
        for k, v in kw.items():
            # ignore hash command sub key rebuild
            if func in self._hash_commands and k in {"key", "keys", "mapping", "args"}:
                continue

            # ignore zadd mapping params
            if k == "mapping" and func in {"zadd"}:
                continue

            if k == "mapping" and isinstance(v, dict):
                kw[k] = {self.__get_key(sk): sv for sk, sv in v.items()}
                continue

            # rebuild cache key to add redis prefix
            if k in self.__valid_keys or (k == "args" and len(set(keys) - self.__valid_keys) == 0):
                kw[k] = self.__get_key(v) if isinstance(v, str) else self.__get_keys(v)

        # add *args to func call
        if var_args and var_args in kw:
            new_args = [kw.pop(k) for k in keys] + list(kw.pop(var_args))
            return getattr(self, "__old_%s" % func)(*new_args, **kw)

        # func call for only kw
        return getattr(self, "__old_%s" % func)(**kw)

    def __get_key(self, key):
        """ Generate operate key

        :param key: origin key
        """
        if not key or not isinstance(key, str):
            raise ValueError("Invalid redis cache key")

        if self._rds_prefix and key.startswith("-"):
            return key[1:]

        if self._rds_prefix:
            new_key = self._rds_prefix + key
            return new_key
        return key

    def __get_keys(self, keys):
        """ Generate multi operate keys

        :param keys: multi keys
        """
        if isinstance(keys, str):
            return self.__get_key(keys)
        return [self.__get_key(k) for k in keys]

    def cached(self, key: str, timeout: t.Optional[int] = None) -> t.Callable:
        """ Auto cache function response with special key

        :param key: cache key formatter
        :param timeout: cache timeout
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                def _fmt_kwargs(ori_kw):
                    """ format origin params
                    """
                    fmt_kw = dict()
                    for k, v in ori_kw.items():
                        # 处理数组
                        if isinstance(v, (list, tuple, set)):
                            v = "|".join(sorted(v))
                            v = _md5(v) if len(v) > 50 else v
                        # 处理时间
                        elif isinstance(v, datetime):
                            v = v.strftime("%Y%m%d%H%M%S")
                        # 剔除掉redis中特殊的字符串
                        elif isinstance(v, str):
                            v = v.replace(":", "").replace(" ", "").replace("-", "")
                        fmt_kw[k] = v
                    return fmt_kw

                # get all args from function
                args_key = getfullargspec(func).args

                # get all default value
                default_args = func.__defaults__
                kw = dict() if not default_args else dict(zip(args_key[len(args_key) - len(default_args):],
                                                              default_args))

                args_val = args
                if "cls" in args_key or "self" in args_key:
                    args_val = args[1:]
                    args_key = args_key[1: len(args_val) + 1]
                kw = dict(kw, **dict(zip(args_key, args_val)), **kwargs)

                # 优先读取缓存的结果
                use_cache, cache_key = kw.get("cache"), key.format(**_fmt_kwargs(kw))
                # global variables have higher priority than local variables for only ignore cache
                if K_INTERNAL_IGNORE_CACHE in self._config:
                    use_cache = self._config[K_INTERNAL_IGNORE_CACHE]

                if use_cache:
                    cache_rst = self.get(cache_key)
                    if cache_rst:
                        return json.loads(cache_rst)

                if "cache" in kw.keys() and "cache" not in args_key:
                    del kwargs["cache"]

                # execute origin function
                rst = func(*args, **kwargs)
                # cache function result
                if use_cache is not None:
                    # serialize result use json
                    cache_rst = json.dumps(rst, cls=JSONEncoder) if isinstance(rst, (list, dict, tuple, set)) else rst
                    self.set(cache_key, cache_rst, timeout)

                return rst
            return wrapper
        return decorator


class Redis(_RedisExt):

    def __init__(  # pylint: disable=super-init-not-called
            self,
            app: t.Optional["Flask"] = None,
            config: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        """ Initialize redis client instance

        :param app: mask application
        :param config: config for mask redis
        """
        self.config = config
        self.instances = dict()

        if app is not None:
            self.app = app
            self.init_app(app, config)

    def init_app(
            self,
            app: "Flask",
            config: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        """ format config and build extensions for mask
        """
        if not (config is None or isinstance(config, dict)):
            raise TypeError("'config' must be type of dict or None")

        base_config = app.config.copy()
        if self.config:
            base_config.update(self.config)
        if config:
            base_config.update(config)
        self.config = base_config

        ignore_cache = self.config.pop(K_RDS_IGNORE_CACHE, None)

        # If you use multiple Redis databases within the same application,
        # you should create a separate client instance (and possibly a separate connection pool) for each database.
        rds_binds = self.config.pop(K_RDS_BINDS, None)
        if rds_binds and isinstance(rds_binds, dict):
            for key, cfg in rds_binds.items():
                # set up to sub databases
                if ignore_cache is not None and K_INTERNAL_IGNORE_CACHE not in cfg:
                    cfg[K_INTERNAL_IGNORE_CACHE] = ignore_cache

                if key == "DEFAULT":
                    super().__init__(cfg)
                else:
                    self.instances[key] = _RedisExt(cfg)
        else:
            if ignore_cache is not None and K_INTERNAL_IGNORE_CACHE not in self.config:
                self.config[K_INTERNAL_IGNORE_CACHE] = ignore_cache

            # Redis client instances can safely be shared between threads.
            super().__init__(self.config)

        self.app = app
        self.app.extensions["Redis"] = self

    def __getattr__(self, item) -> "_RedisExt":
        return self[item]

    def __getitem__(self, item) -> "_RedisExt":
        """
        :rtype: RedisExtension
        """
        obj = self.instances.get(item)
        if obj is None:
            raise KeyError("Cannot read db with name '%s'" % item)
        return obj

    def close(self):
        """ Close redis instance and all of sub instances
        """
        super().close()
        for instance in self.instances.values():
            instance.close()
