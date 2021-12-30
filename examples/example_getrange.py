# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-18 14:09'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.set("GETRANGE:K1", "VALUE1")
    redis.set("-GETRANGE:K2", "VALUE2")

    redis.setrange("GETRANGE:K1", 3, "TTV1")
    redis.setrange("-GETRANGE:K2", 2, "MTLD5")

    print(redis.getrange("GETRANGE:K1", 3, -1))
    print(redis.getrange("-GETRANGE:K2", 2, -1))

    redis.expire("GETRANGE:K1", 60)
    redis.expire("-GETRANGE:K2", 60)
