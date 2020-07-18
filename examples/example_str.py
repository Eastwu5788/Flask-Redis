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
app.config["REDIS_URL"] = "redis://:LSkdf378M@192.168.1.181:16379/9"
app.config["REDIS_DECODE_RESPONSES"] = True
app.config["REDIS_DB"] = 9
redis.init_app(app)


if __name__ == "__main__":
    redis.set("GETRANGE:K1", "VALE1")
    redis.set("-GETRANGE:K2", "VALUE212")

    print(redis.strlen("GETRANGE:K1"))
    print(redis.strlen("-GETRANGE:K2"))

    redis.expire("GETRANGE:K1", 60)
    redis.expire("-GETRANGE:K2", 60)

