# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-28 17:00'

from flask import Flask
from flask_redis import Redis, K_RDS_DEFAULT_BIND

redis = Redis()
app = Flask(__name__)

app.config[K_RDS_DEFAULT_BIND] = "default"
app.config["REDIS_BINDS"] = {
    "default": {
        "REDIS_PREFIX": "DEFAULT:",
        "REDIS_URL": "redis://:@127.0.0.1:6379/0",
    },
    "DB12": {
        "REDIS_PREFIX": "EG12:",
        "REDIS_URL": "redis://:@127.0.0.1:6379/12",
    },
    "DB13": {
        "REDIS_PREFIX": "EG13:",
        "REDIS_URL": "redis://:@127.0.0.1:6379/23",
    }
}
redis.init_app(app)


if __name__ == "__main__":
    redis.set("EXP2:SET", "EXP2:VALUE1", 60)
    print(redis.get("EXP2:SET"))
