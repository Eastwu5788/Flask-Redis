# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/29 1:16 下午'
# 3p
import pytest
from flask import Flask
# project
from flask_redis import (
    Redis,
    RedisCluster,
    Sentinel
)


@pytest.fixture
def redis():
    """ build redis client for test cases
    """
    app = Flask(__name__)
    app.config["REDIS_PREFIX"] = "EG:"
    app.config["REDIS_URL"] = "redis://:@127.0.0.1:6390/0"
    app.config["REDIS_DECODE_RESPONSES"] = True

    rds = Redis()
    rds.init_app(app)
    return rds


@pytest.fixture
def mredis():
    """ buind multi redis client
    """
    app = Flask(__name__)
    app.config["REDIS_DECODE_RESPONSES"] = True
    app.config["REDIS_PREFIX"] = "EGM:"
    app.config["REDIS_DEFAULT_BIND_KEY"] = "DB0"

    app.config["REDIS_BINDS"] = {
        "DB0": "redis://:@127.0.0.1:6390/0",
        "DB1": {
            "REDIS_PREFIX": "EGM2:",
            "REDIS_URL": "redis://:@127.0.0.1:6390/1"
        },
        "DB2": {
            "REDIS_HOST": "127.0.0.1",
            "REDIS_PORT": 6390,
            "REDIS_DB": 2,
            "REDIS_PASSWORD": None
        }
    }

    rds = Redis()
    rds.init_app(app)
    return rds


@pytest.fixture
def cluster():
    """ build redis cluster
    """
    app = Flask(__name__)
    app.config["REDIS_DECODE_RESPONSES"] = True
    app.config["REDIS_PREFIX"] = "CLU:"
    app.config["REDIS_URL"] = "redis://:@127.0.0.1:7001/0"

    rds = RedisCluster()
    rds.init_app(app)
    return rds


@pytest.fixture
def master():
    """ build redis sentinel
    """
    app = Flask(__name__)

    app.config["REDIS_PREFIX"] = "SEN:"
    app.config["REDIS_SENTINELS"] = [("192.168.1.189", 18001)]
    app.config["REDIS_SENTINEL_KWARGS"] = {
        "socket_timeout": 0.1
    }
    app.config["REDIS_CONNECTION_KWARGS"] = {
        "decode_responses": True
    }

    rds = Sentinel()
    rds.init_app(app)
    return rds.master_for("mymaster")


@pytest.fixture
def slave():
    """ build redis sentinel
    """
    app = Flask(__name__)

    app.config["REDIS_PREFIX"] = "SEN:"
    app.config["REDIS_SENTINELS"] = [("192.168.1.189", 18001)]
    app.config["REDIS_SENTINEL_KWARGS"] = {
        "socket_timeout": 2,
        "socket_connect_timeout": 1
    }
    app.config["REDIS_CONNECTION_KWARGS"] = {
        "decode_responses": True
    }

    rds = Sentinel()
    rds.init_app(app)
    return rds.slave_for("mymaster")
