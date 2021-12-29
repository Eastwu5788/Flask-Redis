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
from flask_redis import Redis


@pytest.fixture
def redis():
    """ build redis client for test cases
    """
    app = Flask(__name__)
    app.config["REDIS_PREFIX"] = "EG:"
    app.config["REDIS_URL"] = "redis://:@127.0.0.1:6379/0"
    app.config["REDIS_DECODE_RESPONSES"] = True

    rds = Redis()
    rds.init_app(app)
    return rds
