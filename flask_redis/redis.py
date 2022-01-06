# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/5 11:17 上午'
# sys
import typing as t
# 3p
from redis import Redis, ConnectionPool
# project
from .ext import BaseExtensions
from .macro import (
    K_RDS_BINDS,
    K_RDS_DEFAULT_BIND_KEY,
    K_RDS_IGNORE_CACHE,
    K_RDS_PREFIX,
    K_RDS_URL,
)
# check
if t.TYPE_CHECKING:
    from flask import Flask


class _RedisExt(Redis, BaseExtensions):

    def __init__(self, config):
        """ Initialize redis client with special config
        """
        self._config = config
        self._rds_prefix = self._config.pop(K_RDS_PREFIX, None)
        self._ignore_cached = self._config.pop(K_RDS_IGNORE_CACHE,  None)

        rds_url = config.pop(K_RDS_URL, None)
        # Transform all of the config key with 'REDIS_' to init params
        # e.g. REDIS_HOST -> host
        kwargs = {k[6:].lower(): v for k, v in config.items() if k.startswith("REDIS_")}
        if rds_url:
            connection_pool = ConnectionPool.from_url(rds_url, **kwargs)
            super().__init__(connection_pool=connection_pool)
        else:
            super().__init__(**kwargs)

        self._partial_methods()

    def _ext_set(self, key, value, timeout):
        return self.set(key, value, timeout)

    def _ext_get(self, key):
        return self.get(key)


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
        config = self.init_config(app, config)

        # If you use multiple Redis databases within the same application,
        # you should create a separate client instance (and possibly a separate connection pool) for each database.
        rds_binds = config.pop(K_RDS_BINDS, None)
        if rds_binds and isinstance(rds_binds, dict):
            # load default bind key
            dft_bind_key = config.pop(K_RDS_DEFAULT_BIND_KEY, "DEFAULT")
            for key, value in rds_binds.items():
                cfg = config.copy()
                # sub database config is based on main config items
                if isinstance(value, dict):
                    cfg.update(value)
                elif isinstance(value, str):
                    cfg[K_RDS_URL] = value
                else:
                    raise TypeError("%s sub database items must be type of string or dict" % K_RDS_BINDS)

                if key == dft_bind_key:
                    super().__init__(cfg)
                else:
                    self.instances[key] = _RedisExt(cfg)
        else:
            # Redis client instances can safely be shared between threads.
            super().__init__(config)

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
