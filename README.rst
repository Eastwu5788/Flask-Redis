Flask-Redis
============

.. image:: https://github.com/Eastwu5788/Flask-Redis/actions/workflows/intergration.yml/badge.svg
    :target: https://github.com/Eastwu5788/Flask-Redis/actions/workflows/intergration.yml
.. image:: https://codecov.io/gh/Eastwu5788/Flask-Redis/branch/master/graph/badge.svg?token=GIHTW0XDK7
    :target: https://codecov.io/gh/Eastwu5788/Flask-Redis
.. image:: https://badge.fury.io/py/flask-redis-ext.svg
    :target: https://badge.fury.io/py/flask-redis-ext

Flask-Redis is an extension for Flask that adds support for Redis to your application. It aims to simplify manage multi
Redis databases with Flask.

Installing
============

Install and update using pip:

::

  pip install flask-redis-ext

A Simple Example
==================

::

    from flask import Flask
    from flask-redis import Redis

    redis = Redis()
    app = Flask(__name__)

    app.config["REDIS_PREFIX"] = "EG:"
    app.config["REDIS_URL"] = "redis://:password@host:port/db"
    app.config["REDIS_DECODE_RESPONSES"] = True
    redis.init_app(app)

    redis.get("test")


Multiple Databases
====================

::

    from flask import Flask
    from flask_redis import Redis

    redis = Redis()
    app = Flask(__name__)

    app.config["REDIS_BINDS"] = {
        "default": {
            "REDIS_URL": "redis://:password@host:port/db",
        },
        "DB12": {
            "REDIS_PREFIX": "EG12:",
            "REDIS_URL": "redis://:password@host:port/db",
        },
        "DB13": {
            "REDIS_PREFIX": "EG13:",
            "REDIS_URL": "redis://:password@host:port/db",
        }
    }
    redis.init_app(app)

    # use default database
    redis.set("key1", "value1)
    # use db12
    redis["DB12"].set("key1", "value1")
    # use getter
    redis.DB12.set("key1", "value1)

Support Parameter
====================

========================   ==============================   =========================================================
     Parameter                    Explain                     Example
========================   ==============================   =========================================================
   REDIS_PREFIX             storage key prefix                    app.config["REDIS_PREFIX"] = "EG:"
   REDIS_BINDS              bind multi databases                            --
   REDIS_URL                    redis url                               redis://xx
REDIS_DEFAULT_BIND_KEY         default redis bind key           app.config["REDIS_DEFAULT_BIND_KEY"] = "default"
REDIS_CONNECTION_POOL         custom connection pool         app.config["REDIS_CONNECTION_POOL"] = ConnectionPool()
========================   ==============================   =========================================================


Redis Parameter
================

We support all of the origin redis init parameter which should be started with "REDIS_".
`socket_timeout` should be config as `REDIS_SOCKET_TIMEOUT`.
`encoding` should be config as `REDIS_ENCODING`. All other parameters follow the same rules.


Redis Prefix
===============

`Flask-Redis` support auto add key prefix for all of the `DataAccessCommands`.

For example, if you use `redis.set("k", "v")` to store value at `k` and redis prefix is `EG:`, the really key in redis
storage is "EG:k".

Redis prefix is a globally effective configuration parameter. if you don't want to add prefix for special key,
you can add `-` character to ignore prefix. e.g. `redis.set("-k", "v")` which will store really 'k' as key in redis.

Decorator `cached`
====================

`Flask-Redis` support `cached` decorator to cache function result.

::

    @redis.cached(key="{k1}:{k2}:{k3}", timeout=10)
    def cache_func(sf, k1, k2=1, **kwargs):
        return sf.v1


Cluster Mode
================

`Flask-Redis` support redis cluster mode. There are multiple ways in which a cluster instance can be created:

Specially, `Flask-Redis` don't support multi cluster, therefore `REDIS_BINDS` is invalid for this mode.

* Using the Redis URL specification:

::

    from flask import Flask
    from flask_redis import RedisCluster

    app = Flask(__name__)
    app.config["REDIS_DECODE_RESPONSES"] = True
    app.config["REDIS_PREFIX"] = "CLU:"
    app.config["REDIS_URL"] = "redis://:@127.0.0.1:7001/0"

    redis = RedisCluster()
    redis.init_app(app)

    print(redis.get_nodes())
    redis.set("K", "V")


* Using `host` and `port` arguments:

::

    from flask import Flask
    from flask_redis import RedisCluster

    app = Flask(__name__)
    app.config["REDIS_DECODE_RESPONSES"] = True
    app.config["REDIS_PREFIX"] = "CLU:"
    app.config["REDIS_HOST"] = "127.0.0.1"
    app.config["REDIS_PORT"] = "7001"

    redis = RedisCluster()
    redis.init_app(app)

    print("redis.get_nodes()")
    redis.set("K", "V")


Sentinel Mode
==================

`Flask-Redis` support sentinel mode. You can use a Sentinel connection to discover the master and slaves
network address. You can also create redis client connections from a sentinel instance.

::

    from flask import Flask
    from flask_redis import Sentinel

    app = Flask(__name__)

    app.config["REDIS_PREFIX"] = "SEN:"
    app.config["REDIS_SENTINELS"] = [("192.168.1.189", 18001)]
    app.config["REDIS_SENTINEL_KWARGS"] = {
        "socket_timeout": 0.1
    }
    app.config["REDIS_CONNECTION_KWARGS"] = {
        "decode_responses": True
    }

    rds = Sentinel()
    rds.init_app(app)

    print(rds.discover_master("mymaster"))
    print(rds.discover_slaves("mymaster"))

    master = rds.master_for("mymaster")
    slave = rds.slave_for("mymaster")

    master.set("k", "v")
    slave.get("k")


Sentinel mode is different from other simple and Cluster mode. in this mode, you should use `REDIS_SENTINELS` parameter
to config connection info. You will get details about sentinel mode parameter at below:

========================   ==============================   =========================================================
     Parameter                    Explain                     Example
========================   ==============================   =========================================================
    REDIS_SENTINELS             sentinel connections        app.config["REDIS_SENTINELS"] = [("192.168.1.189", 18001)]
 REDIS_SENTINEL_KWARGS      sentinel kwargs for Sentinel    app.config["REDIS_SENTINEL_KWARGS"] = {"socket_timeout": 0.1}
 REDIS_CONNECTION_KWARGS    redis connection kwargs         app.config["REDIS_CONNECTION_KWARGS"] = {"decode_responses": True}
========================   ==============================   =========================================================


Links
======

* Release: https://pypi.org/project/flask-redis-ext/
* Code: https://github.com/Eastwu5788/Flask-Redis
* Issue tracker: https://github.com/Eastwu5788/Flask-Redis/issues
* Test status: https://coveralls.io/github/Eastwu5788/Flask-Redis
