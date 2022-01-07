# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/6 3:29 下午'
from flask import Flask
from flask_redis import Sentinel


redis = Sentinel()
app = Flask(__name__)


app.config["REDIS_PREFIX"] = "CLU:"
app.config["REDIS_SENTINELS"] = [("192.168.1.189", 18001)]
app.config["REDIS_SENTINEL_KWARGS"] = {
    "socket_timeout": 0.1
}
app.config["REDIS_DECODE_RESPONSES"] = True
redis.init_app(app)


if __name__ == "__main__":
    master = redis.master_for("mymaster")
    master.set("SEN:K1", "V1", 600)
    print(redis.discover_master("mymaster"))
    print(redis.discover_slaves("mymaster"))

    slave = redis.slave_for("mymaster")
    rst = slave.get("SEN:K1")
    print(rst)
