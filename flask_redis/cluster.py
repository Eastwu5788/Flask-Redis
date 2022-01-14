# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/5 2:52 下午'
# sys
import typing as t
# 3p
from redis import RedisCluster as Redis
# project
from .ext import BaseExtensions
from .macro import (
    K_RDS_PREFIX,
    K_RDS_IGNORE_CACHE,
)
# type
if t.TYPE_CHECKING:
    from flask import Flask  # pylint: disable=unused-import


class _RedisClusterExt(Redis, BaseExtensions):  # pylint: disable=too-many-ancestors

    def __init__(self, config):
        """ Initialize redis client with special config
        """
        self._config = config
        self._rds_prefix = self._config.pop(K_RDS_PREFIX, None)
        self._ignore_cached = self._config.pop(K_RDS_IGNORE_CACHE,  None)

        # Transform all of the config key with 'REDIS_' to init params
        # e.g. REDIS_HOST -> host
        super().__init__(**{k[6:].lower(): v for k, v in config.items() if k.startswith("REDIS_")})
        self._partial_methods()

    def _ext_get(self, key):
        return self.get(key)

    def _ext_set(self, key, value, timeout):
        return self.set(key, value, timeout)


class RedisCluster(_RedisClusterExt):  # pylint: disable=too-many-ancestors

    def __init__(  # pylint: disable=super-init-not-called
            self,
            app: t.Optional["Flask"] = None,
            config: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        """ Initialize redis client instance

        :param app: mask application
        :param config: config for mask redis
        """
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
        super().__init__(config)

        self.app = app
        self.app.extensions["Redis"] = self
