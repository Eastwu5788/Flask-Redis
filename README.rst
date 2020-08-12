Flask-Redis
============

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
    app.config["REDIS_URL"] = "redis://:LSkdf378M@192.168.1.181:16379/9"
    app.config["REDIS_DECODE_RESPONSES"] = True
    app.config["REDIS_DB"] = 9
    redis.init_app(app)

    redis.get("test")


Multiple Databases
====================

::

    from flask import Flask
    from flask_redis import Redis, K_RDS_DEFAULT_BIND

    redis = Redis()
    app = Flask(__name__)

    app.config[K_RDS_DEFAULT_BIND] = "default"
    app.config["REDIS_BINDS"] = {
        "default": {
            "REDIS_PREFIX": "DEFAULT:",
            "REDIS_URL": "redis://:LSkdf378M@192.168.1.181:16379/12",
        },
        "DB12": {
            "REDIS_PREFIX": "EG12:",
            "REDIS_URL": "redis://:LSkdf378M@192.168.1.181:16379/12",
        },
        "DB13": {
            "REDIS_PREFIX": "EG13:",
            "REDIS_URL": "redis://:LSkdf378M@192.168.1.181:16379/13",
        }
    }
    redis.init_app(app)

    # use default database
    redis.set("key1", "value1)
    # use db12
    redis["DB12"].set("key1", "value1")


Support Parameter
====================

======================   ==============================   ==============================================
     Parameter                    Explain                     Example
======================   ==============================   ==============================================
   K_RDS_PREFIX             storage key prefix                    app.config["REDIS_PREFIX"] = "EG:"
   K_RDS_BINDS              bind multi databases                            --
   K_RDS_URL                    redis url                               redis://xx
K_RDS_DEFAULT_BIND         default redis bind key           app.config[K_RDS_DEFAULT_BIND] = "default"
======================   ==============================   ==============================================

Redis Parameter
================

We support all of the origin redis init parameter.
`socket_timeout` should be config as `REDIS_SOCKET_TIMEOUT`.
`encoding` should be config as `REDIS_ENCODING`. All other parameters follow the same rules.


Documents
===========

Flask-redis manual could be found at:


Coffee
=========

Please give me a cup of coffee, thank you!

BTC: 1657DRJUyfMyz41pdJfpeoNpz23ghMLVM3

ETH: 0xb098600a9a4572a4894dce31471c46f1f290b087


Links
======

* Documentaion: https://pre-request.readthedocs.io/en/master/index.html
* Release: https://pypi.org/project/pre-request/
* Code: https://github.com/Eastwu5788/pre-request
* Issue tracker: https://github.com/Eastwu5788/pre-request/issues
* Test status: https://coveralls.io/github/Eastwu5788/pre-request

