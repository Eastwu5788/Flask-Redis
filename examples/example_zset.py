# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-22 09:24'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.zadd("ZSORT:K1", mapping={
        "V1": "9",
        "V2": "9",
        "V3": "9",
    })
    redis.zadd("-ZSORT:K2", mapping={
        "T1": "6",
        "T2": "6",
        "T3": "6",
    })

    print(redis.zrandmember("ZSORT:K1", count=1, withscores=True))

    redis.delete("ZSORT:K1", "-ZSORT:K2")

    # redis.zadd("ZSET:K1", {
    #     "V1": 11,
    #     "V2": 22,
    #     "V3": 66,
    # })
    # redis.zadd("-ZSET:K2", {
    #     "VV1": 55,
    #     "VV2": 33,
    #     "VV3": 77,
    #     "VV4": 200,
    # })
    #
    # print(redis.zcard("ZSET:K1"))
    # print(redis.zcard("-ZSET:K2"))
    #
    # print(redis.zrange("ZSET:K1", start=0, end=1))
    # print(redis.zrange("-ZSET:K2", start=1, end=2))
    #
    # print(redis.zrevrange("ZSET:K1", start=0, end=-1))
    # print(redis.zrevrange("-ZSET:K2", start=0, end=-1))
    #
    # print(redis.zrangebyscore("ZSET:K1", 22, 33))
    # print(redis.zrangebyscore("-ZSET:K2", 55, 99))
    #
    # print(redis.zrevrangebyscore("ZSET:K1", 33, 22))
    # print(redis.zrevrangebyscore("-ZSET:K2", 99, 55))
    #
    # print(redis.zscan("ZSET:K1"))
    # print(redis.zscan("-ZSET:K2"))
    #
    # print(redis.zcount("ZSET:K1", 22, 33))
    # print(redis.zcount("-ZSET:K2", 55, 99))
    #
    # redis.zincrby("ZSET:K1", 22, "V1")
    # redis.zincrby("-ZSET:K2", 55, "VV1")
    #
    # print(redis.zrank("ZSET:K1", "V1"))
    # print(redis.zrank("-ZSET:K2", "VV1"))
    #
    # print(redis.zrevrank("ZSET:K1", "V1"))
    # print(redis.zrevrank("-ZSET:K2", "VV1"))
    #
    # redis.zrem("ZSET:K1", "V2")
    # redis.zrem("-ZSET:K2", "VV2")
    #
    # redis.zremrangebyrank("ZSET:K1", 0, 0)
    # redis.zremrangebyrank("-ZSET:K2", 0, 0)
    #
    # redis.zremrangebyscore("ZSET:K1", 77, 77)
    # redis.zremrangebyscore("-ZSET:K2", 100, 120)
    #
    # print(redis.zscore("ZSET:K1", "V3"))
    # print(redis.zscore("-ZSET:K2", "VV4"))
    #
    # redis.expire("ZSET:K1", 60)
    # redis.expire("-ZSET:K2", 60)
