# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-20 09:50'
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
    redis.sadd("SET:K1", "V1", "V1", "V2")
    redis.sadd("-SET:K2", "VV1", "VV1", "VV2")

    print(redis.scard("SET:K1"))
    print(redis.scard("-SET:K2"))

    print(redis.smembers("SET:K1"))
    print(redis.smembers("-SET:K2"))

    print(redis.sscan("SET:K1"))
    print(redis.sscan("-SET:K2"))

    print(redis.sdiff(["SET:K1", "-SET:K2"]))
    print(redis.sdiff(["-SET:K2", "SET:K1"]))

    print(redis.sdiff("SET:K1", "-SET:K2"))
    print(redis.sdiff("-SET:K2", "SET:K1"))

    print(redis.sdiffstore("SET:K3", "SET:K1", "-SET:K2"))
    print(redis.sdiffstore("-SET:K3", "SET:K1", "-SET:K2"))

    redis.expire("SET:K1", 60)
    redis.expire("-SET:K2", 60)
