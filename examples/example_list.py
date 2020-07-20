# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-19 14:30'
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
    redis.lpush("LS:K1", "V1", "V2")
    redis.lpush("-LS:K2", "VV1", "VV2")

    redis.lpushx("LS:K1", "V3")
    redis.lpushx("-LS:K2", "VV3")

    redis.rpush("LS:K1", "V5", "V6")
    redis.rpush("-LS:K2", "VV5", "VV6")

    redis.rpushx("LS:K1", "V7")
    redis.rpushx("-LS:K2", "VV7")

    redis.linsert("LS:K1", "after", "V1", "VB1")
    redis.linsert("-LS:K2", "before", "VV1", "VF1")

    redis.lset("LS:K1", 1, "VS2")
    redis.lset("-LS:K2", 1, "VVS2")

    redis.lrem("LS:K1", 0, "V7")
    redis.lrem("-LS:K2", 0, "VV7")

    print(redis.lpop("LS:K1"))
    print(redis.lpop("-LS:K2"))

    print(redis.rpop("LS:K1"))
    print(redis.rpop("-LS:K2"))

    print(redis.lindex("LS:K1", 0))
    print(redis.lindex("-LS:K2", 0))

    print(redis.rpoplpush("LS:K1", "-LS:K2"))
    print(redis.brpoplpush("LS:K1", "-LS:K2"))

    print(redis.blpop(["LS:K1", "-LS:K2"]))
    print(redis.brpop(["-LS:K2", "LS:K1"]))

    redis.expire("LS:K1", 60)
    redis.expire("-LS:K2", 60)

