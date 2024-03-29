# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-28 17:29'

from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_HOST"] = "127.0.0.1"
app.config["REDIS_PORT"] = 6379
app.config["REDIS_DB"] = 0
app.config["REDIS_PASSWORD"] = ""
redis.init_app(app)


if __name__ == "__main__":
    redis.set("EXP:HOST:SET", "EXP:HOST:VALUE1", 60)
    print(redis.get("EXP:HOST:SET"))
