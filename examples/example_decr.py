# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-18 14:31'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
app.config["REDIS_DB"] = 0
redis.init_app(app)


if __name__ == "__main__":
    redis.set("DECR:K1", 20)

    redis.decr("DECR:K1", amount=5)
    redis.decr("-DECR:K2", amount=500)

    print(redis.get("DECR:K1"))
    print(redis.get("-DECR:K2"))

    redis.expire("DECR:K1", 60)
    redis.expire("-DECR:K2", 60)
