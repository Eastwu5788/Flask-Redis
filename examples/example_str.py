# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-18 14:19'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    # redis.set("STR:K1", "VALE1")
    # redis.set("-STR:K2", "VALUE212")
    #
    # rst = redis.mget("STR:K1", "STR:K1", "-STR:K2")
    #
    # print(redis.strlen("STR:K1"))
    # print(redis.strlen("-STR:K2"))
    #
    # redis.append("STR:K1", "APPEND1")
    # redis.append("-STR:K2", "APPEND2")
    #
    # print(redis.get("STR:K1"))
    # print(redis.get("-STR:K2"))
    #
    # redis.delete("STR:K1")
    # redis.expire("-STR:K2", 60)
    redis.set("STR:K1", "V1")
    redis.msetnx({
        "STR:K1": "T1",
        "-STR:K2": "V2",
        "STR:K3": "V3"
    })
