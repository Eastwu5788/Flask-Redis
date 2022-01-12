# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/6 9:51 上午'
# sys
import typing as t
from copy import deepcopy
# 3p
from redis import Sentinel as _Sentinel
from redis import SentinelConnectionPool
# project
from .redis import RedisExt
# type
if t.TYPE_CHECKING:
    from flask import Flask


class SentinelExt(_Sentinel):

    def __init__(
            self,
            config: dict
    ):
        sentinels = config.pop("REDIS_SENTINELS", list())
        min_other_sentinels = config.pop("REDIS_MIN_OTHER_SENTINELS", 0)
        sentinel_kwargs = config.pop("REDIS_SENTINEL_KWARGS", None)
        connection_kwargs = config.pop("REDIS_CONNECTION_KWARGS", dict())

        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {
                k: v for k, v in connection_kwargs.items() if k.startswith("socket_")
            }
        self.sentinel_kwargs = sentinel_kwargs

        self.sentinels = list()
        for hostname, port in sentinels:
            cfg = deepcopy(config)
            cfg["REDIS_HOST"] = hostname
            cfg["REDIS_PORT"] = port
            cfg.update({"REDIS_%s" % k.upper(): v for k, v in self.sentinel_kwargs.items()})
            self.sentinels.append(RedisExt(cfg))

        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs
        self.config = config

    def master_for(
            self,
            service_name,
            connection_pool_class=SentinelConnectionPool,
            **kwargs,
    ):
        """
        Returns a redis client instance for the ``service_name`` master.

        A :py:class:`~redis.sentinel.SentinelConnectionPool` class is
        used to retrieve the master's address before establishing a new
        connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to
        use.  The :py:class:`~redis.sentinel.SentinelConnectionPool`
        will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_master"] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        config = dict(self.config, **{
            "REDIS_CONNECTION_POOL": connection_pool_class(service_name, self, **connection_kwargs)
        })
        return RedisExt(config)

    def slave_for(
            self,
            service_name,
            connection_pool_class=SentinelConnectionPool,
            **kwargs,
    ):
        """
        Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrieve the slave's
        address before establishing a new connection.

        By default clients will be a :py:class:`~redis.Redis` instance.
        Specify a different class to the ``redis_class`` argument if you
        desire something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_master"] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        config = dict(self.config, **{
            "REDIS_CONNECTION_POOL": connection_pool_class(service_name, self, **connection_kwargs)
        })
        return RedisExt(config)


class Sentinel(SentinelExt):

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

    @staticmethod
    def init_config(
            app: "Flask",
            config: t.Optional[t.Dict[str, t.Any]] = None
    ) -> dict:
        if not (config is None or isinstance(config, dict)):
            raise TypeError("'config' must be type of dict or None")

        base_config = app.config.copy()
        if config:
            base_config.update(config)

        return {k: v for k, v in base_config.items() if k.startswith("REDIS_")}
