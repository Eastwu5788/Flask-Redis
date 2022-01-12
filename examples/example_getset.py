# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-15 14:00'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
app.config["REDIS_DB"] = 9
redis.init_app(app)


if __name__ == "__main__":
    redis.set("-GETSET:K1", "V1")
    print(redis.getset("-GETSET:K1", "V2"))
    del redis["-GETSET:K1"]
