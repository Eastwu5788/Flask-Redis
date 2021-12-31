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
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    # redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
    # redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")
    #
    # redis.sdiffstore("SET:K3", "-SET:K2", "SET:K1")
    # redis.sdiffstore("-SET:K4", ["SET:K1", "-SET:K2"])

    redis.sadd("SET:K1", "V1", "V2", "U2", "U3")
    redis.sadd("-SET:K2", "T1", "T2", "U1", "U2")

    rst = redis.sscan("SET:K1", 0, match="U*")
    rst = redis.sscan("-SET:K2", 0, match="T*")

    # redis.sadd("SET:K1", "V1", "V1", "V2", "VI3")
    # redis.sadd("-SET:K2", "VV1", "VV1", "VV2", "VI3")
    #
    # print(redis.scard("SET:K1"))
    # print(redis.scard("-SET:K2"))
    #
    # print(redis.smembers("SET:K1"))
    # print(redis.smembers("-SET:K2"))
    #
    # print(redis.sscan("SET:K1"))
    # print(redis.sscan("-SET:K2"))
    #
    # print(redis.sdiff(["SET:K1", "-SET:K2"]))
    # print(redis.sdiff(["-SET:K2", "SET:K1"]))
    #
    # print(redis.sdiff("SET:K1", "-SET:K2"))
    # print(redis.sdiff("-SET:K2", "SET:K1"))
    #
    # print(redis.sdiffstore("SET:K3", "SET:K1", "-SET:K2"))
    # print(redis.sdiffstore("-SET:K3", "SET:K1", "-SET:K2"))
    #
    # print(redis.sinter("SET:K1", "-SET:K2"))
    # redis.sinterstore("-SET:K4", "SET:K1", "-SET:K2")
    #
    # print(redis.sunion("SET:K1", "-SET:K2"))
    # redis.sunionstore("-SET:K5", "SET:K1", "-SET:K2")
    #
    # print(redis.sismember("-SET:K2", "VV2"))
    # print(redis.sismember("SET:K1", "V1"))
    #
    # redis.smove("SET:K1", "-SET:K2", "V1")
    # redis.srem("-SET:K2", "VV1")
    # redis.spop("-SET:K2")
    #
    # redis.expire("SET:K1", 60)
    # redis.expire("-SET:K2", 60)
