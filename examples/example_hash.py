# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-07-18 14:41'
from flask import Flask
from flask_redis import Redis

redis = Redis()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "EG:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.hset("HASH:K1", "K1", "V1")
    redis.hset("-HASH:K2", "K11", "V11")

    print(redis.hget("HASH:K1", "K1"))
    print(redis.hget("-HASH:K2", "K11"))

    redis.hmset("HASH:K1", {"K2": "V2", "K3": "3"})
    redis.hmset("-HASH:K2", {"K22": "V22", "K33": "33"})

    print(redis.hmget("HASH:K1", ["K1", "K2"], "K3"))
    print(redis.hmget("-HASH:K2", "K22", "K33"))

    print(redis.hgetall("HASH:K1"))
    print(redis.hgetall("-HASH:K2"))

    print(redis.hlen("HASH:K1"))
    print(redis.hlen("-HASH:K2"))

    print(redis.hkeys("HASH:K1"))
    print(redis.hkeys("-HASH:K2"))

    print(redis.hvals("HASH:K1"))
    print(redis.hvals("-HASH:K2"))

    print(redis.hexists("HASH:K1", "K1"))
    print(redis.hexists("-HASH:K2", "K22"))

    print(redis.hdel("HASH:K1", "K1"))
    print(redis.hdel("-HASH:K2", "K22"))

    print(redis.hexists("HASH:K1", "K1"))
    print(redis.hexists("-HASH:K2", "K22"))

    redis.hincrby("HASH:K1", "K3", amount=5)
    redis.hincrby("-HASH:K2", "K33", amount=-3)

    redis.hincrbyfloat("HASH:K1", "K3", amount=2.5)
    redis.hincrbyfloat("-HASH:K2", "K33", amount=-3.5)

    print(redis.hgetall("HASH:K1"))
    print(redis.hgetall("-HASH:K2"))

    cursor, data = redis.hscan("HASH:K1", cursor=0, match=None, count=1)
    print(cursor, data)
    cursor, data = redis.hscan("-HASH:K2", cursor=0, match="K*", count=1)
    print(cursor, data)

    for item in redis.hscan_iter("HASH:K1"):
        print(item)

    for item in redis.hscan_iter("-HASH:K2"):
        print(item)
