# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-24 16:02'

# from flask_redis import FlaskRedis
#
# redis = FlaskRedis()
from redis import Redis


class TestObj:

    def __init__(self):
        self.rds = Redis()

    def __getattr__(self, item):
        return getattr(self.rds, item)

    def __getitem__(self, key):
        return self.rds


if __name__ == "__main__":
    tt = dict()
    tt["test"]
    # obj = TestObj()
    # obj["hello"].get("ss")
