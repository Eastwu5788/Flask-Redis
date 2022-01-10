# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/5 3:45 下午'
from flask import Flask
from flask_redis import RedisCluster


redis = RedisCluster()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "CLU:"
app.config["REDIS_URL"] = "redis://:@127.0.0.1:7001/0"
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    redis.zadd("-{CLU:ZSORT:K2}:1", mapping={
        "V1": 13,
        "V2": 23,
        "V3": 9,
    })
    redis.zadd("-{CLU:ZSORT:K2}:2", mapping={
        "T1": 56,
        "V2": 23,
        "T3": 39,
    })

    assert redis.zdiff(["-{CLU:ZSORT:K2}:1", "-{CLU:ZSORT:K2}:2"], withscores=False) == ["V3", "V1"]
    assert redis.zdiff(["-{CLU:ZSORT:K2}:2", "-{CLU:ZSORT:K2}:1"], withscores=True) == ["T3", '39', "T1", '56']

    redis.delete("-{CLU:ZSORT:K2}:1", "-{CLU:ZSORT:K2}:2")
