# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-11 09:37'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:LSkdf378M@192.168.1.181:16379/9"
app.config["REDIS_DECODE_RESPONSES"] = True
app.config["REDIS_DB"] = 9
redis.init_app(app)


if __name__ == "__main__":
    assert redis.setnx("SET:NX:1", "VALUE1") is True
    assert redis.setnx("SET:NX:1", "VALUE2") is False

    assert redis.setnx("-SET:NX:1", "VALUE1") is True
    assert redis.setnx("-SET:NX:1", "VALUE1") is False

    redis.expire("SET:NX:1", 60)
    redis.expire("-SET:NX:1", 70)
