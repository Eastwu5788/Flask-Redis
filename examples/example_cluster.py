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
    redis.set("K1:T1", "V1")
    print(redis.get("K1:T1"))
