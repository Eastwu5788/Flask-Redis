Flask-Redis
============

Flask-Redis is an extension for Flask that adds support for Redis to your application. It aims to simplify manage multi
Redis databases with Flask.

Installing
============

Install and update using pip:

::

  pip install flask-redis

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


Documents
===========



Contributing
==============




Links
======

