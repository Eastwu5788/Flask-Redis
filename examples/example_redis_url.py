# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-28 16:37'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
redis.init_app(app)


if __name__ == "__main__":
    redis.set("EXP1:SET", "EXP1:VALUE1", 60)
    print(redis.get("EXP1:SET"))
