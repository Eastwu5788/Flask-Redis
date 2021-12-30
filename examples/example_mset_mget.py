# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-15 13:37'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.mset({"MSET:K1": "v1", "-MSET:K2": "v2"})
    print(redis.mget(["-MSET:K2"], "MSET:K1"))
    print(redis.msetnx({"-MSET:K2": "v3"}))
    redis.delete("MSET:K1", "-MSET:K2")
