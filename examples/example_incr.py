# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-18 14:25'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.set("INCR:K1", 1)
    redis.set("-INCR:K2", 100)

    redis.incr("INCR:K1", amount=5)
    redis.incrby("-INCR:K2", amount=500)

    redis.incrbyfloat("INCR:K1", amount=5.2)
    redis.incrbyfloat("-INCR:K2", amount=4.1)

    print(redis.get("INCR:K1"))
    print(redis.get("-INCR:K2"))

    redis.expire("INCR:K1", 60)
    redis.expire("-INCR:K2", 60)
