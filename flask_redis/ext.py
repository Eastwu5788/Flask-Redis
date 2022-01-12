# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/5 2:59 下午'
# sys
import hashlib
import json
import typing as t
from datetime import (
    date,
    datetime
)
from decimal import Decimal
from functools import partial, wraps
from inspect import getfullargspec
from json.decoder import JSONDecodeError
# 3p
try:
    from bson import ObjectId
except ImportError:
    ObjectId = None
from redis.commands.core import (
    HashCommands,
    DataAccessCommands
)


def md5(ori_str):
    """ MD5加密算法

    :param ori_str: 原始字符串
    :return: 加密后的字符串
    """
    md5_obj = hashlib.md5()
    md5_obj.update(ori_str.encode("utf8"))
    return md5_obj.hexdigest()


def fmt_kwargs(ori_kw):
    """ format origin params
    """
    fmt_kw = dict()
    for k, v in ori_kw.items():
        # 处理数组
        if isinstance(v, (list, tuple, set)):
            v = "|".join(sorted(v))
            v = md5(v) if len(v) > 50 else v
        # 处理时间
        elif isinstance(v, datetime):
            v = v.strftime("%Y%m%d%H%M%S")

        elif isinstance(v, date):
            v = v.strftime("%Y%m%d")

        # 剔除掉redis中特殊的字符串
        elif isinstance(v, str):
            v = v.replace(":", "").replace(" ", "").replace("-", "")
        fmt_kw[k] = v
    return fmt_kw


class JSONEncoder(json.JSONEncoder):

    def default(self, o):  # pylint: disable=method-hidden

        # 处理datetime
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        # 处理日期
        if isinstance(o, date):
            return o.strftime("%Y-%m-%d")

        # 处理decimal
        if isinstance(o, Decimal):
            return float(o)

        if ObjectId and isinstance(o, ObjectId):
            return str(o)

        # 其它默认处理
        return json.JSONEncoder.default(self, o)


class BaseExtensions:

    _valid_keys = {
        "name",
        "key",
        "names",
        "keys",
        "src",
        "dst",
        "dest",
        "mapping",
        "store",
        "sources"
    }

    _hash_commands = {v for v in dir(HashCommands) if not v.startswith("_")}

    _rds_prefix = None
    _ignore_cached = None

    def init_config(
            self,
            app: "Flask",
            config: t.Optional[t.Dict[str, t.Any]] = None
    ) -> dict:
        if not (config is None or isinstance(config, dict)):
            raise TypeError("'config' must be type of dict or None")

        base_config = app.config.copy()
        if config:
            base_config.update(config)

        return {k: v for k, v in base_config.items() if k.startswith("REDIS_")}

    def _ext_set(self, key, value, timeout):
        """ Use this method to set cached result to redis
        """
        raise NotImplementedError()

    def _ext_get(self, key):
        """ Use this method to get cache result from redis
        """
        raise NotImplementedError()

    def _partial_methods(self):
        """ Bind origin redis method to instance
        """
        _access_commands = {v for v in dir(DataAccessCommands) if not v.startswith("_")}

        for method in dir(self):
            if method.startswith("_"):
                continue

            # ignore special function
            if method in {"cached", "incr", "decr"}:
                continue

            # ignore acl commands
            if method not in _access_commands:
                continue

            func = getattr(self, method)
            try:
                info = getfullargspec(func)
                args_key, var_args = info.args, info.varargs
            except TypeError:
                continue

            valid_keys = self._valid_keys & set(args_key + [var_args])
            if not valid_keys:
                continue

            setattr(self, "__old_%s" % method, func)
            setattr(self, method, partial(self._decorator, method, args_key[1:], var_args))

    def _decorator(self, func, keys, var_args, *args, **kwargs):
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
                kw[k] = {self._get_key(sk): sv for sk, sv in v.items()}
                continue

            # rebuild cache key to add redis prefix
            if k in self._valid_keys or (k == "args" and len(set(keys) - self._valid_keys) == 0):
                kw[k] = self._get_key(v) if isinstance(v, str) else self._get_keys(v)

        # add *args to func call
        if var_args and var_args in kw:
            new_args = [kw.pop(k) for k in keys] + list(kw.pop(var_args))
            return getattr(self, "__old_%s" % func)(*new_args, **kw)

        # func call for only kw
        return getattr(self, "__old_%s" % func)(**kw)

    def _get_key(self, key):
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

    def _get_keys(self, keys):
        """ Generate multi operate keys

        :param keys: multi keys
        """
        if isinstance(keys, str):
            return self._get_key(keys)
        return [self._get_key(k) for k in keys]

    @staticmethod
    def _deserialize(rst):
        """ Deserialize cached result
        """
        if not isinstance(rst, str):
            return rst

        if not rst.startswith("[") and not rst.startswith("{"):
            return rst

        try:
            return json.loads(rst)
        except JSONDecodeError:
            return rst

    def cached(self, key: str, timeout: t.Optional[int] = None) -> t.Callable:
        """ Auto cache function response with special key

        :param key: cache key formatter
        :param timeout: cache timeout
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
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
                use_cache, cache_key = kw.get("cache", True), key.format(**fmt_kwargs(kw))
                # global variables have higher priority than local variables for only ignore cache
                if isinstance(self._ignore_cached, bool):
                    use_cache = False if self._ignore_cached else True

                if use_cache:
                    cache_rst = self._ext_get(cache_key)
                    if cache_rst:
                        return self._deserialize(cache_rst)

                if "cache" in kw.keys() and "cache" not in args_key:
                    del kwargs["cache"]

                # execute origin function
                rst = func(*args, **kwargs)
                # cache function result
                if use_cache is not None:
                    # serialize result use json
                    cache_rst = rst
                    if isinstance(cache_rst, (list, dict, tuple)):
                        cache_rst = json.dumps(rst, cls=JSONEncoder)
                    elif isinstance(cache_rst, set):
                        raise TypeError("Object of type 'set' is not JSON serializable")
                    elif not isinstance(cache_rst, str):
                        cache_rst = str(use_cache)
                    self._ext_set(cache_key, cache_rst, timeout)
                return rst
            return wrapper
        return decorator
