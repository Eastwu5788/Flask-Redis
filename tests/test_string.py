# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/29 1:49 下午'
import time


class TestString:

    def test_set_value(self, redis):
        redis.set("STR:K1", "VALUE1")
        redis.set("-STR:K2", "VALUE2")

        assert redis.get("STR:K1") == "VALUE1"
        assert redis.get("-STR:K2") == "VALUE2"

        redis.delete("STR:K1", "-STR:K2")

    def test_get_range(self, redis):
        redis.set("STR:K1", "VALUE1")
        redis.set("-STR:K2", "VALUE2")

        assert redis.getrange("STR:K1", -2, -1) == "E1"
        assert redis.getrange("-STR:K2", start=2, end=-1) == "LUE2"

        redis.delete("STR:K1", "-STR:K2")

    def test_get_set(self, redis):
        redis.set("STR:K1", "VALUE1")
        redis.set("-STR:K2", "VALUE2")

        assert redis.getset("STR:K1", "V1") == "VALUE1"
        assert redis.getset("-STR:K2", "V2") == "VALUE2"

        assert redis.get("STR:K1") == "V1"
        assert redis.get("-STR:K2") == "V2"

        redis.delete("STR:K1", "-STR:K2")

    def test_get_bit(self, redis):
        redis.set("STR:K1", "V1")
        redis.set("-STR:K2", "V2")

        assert redis.getbit("STR:K1", 3) == 1
        assert redis.getbit("-STR:K2", 14) == 1

        redis.setbit("STR:K1", 3, 0)
        redis.setbit("-STR:K2", 14, 0)

        assert redis.get("STR:K1") == "F1"
        assert redis.get("-STR:K2") == "V0"

        redis.delete("STR:K1", "-STR:K2")

    def test_mget(self, redis):
        redis.set("STR:K1", "V1")
        redis.set("-STR:K2", "V2")

        assert redis.mget("STR:K1", "STR:K1", "-STR:K2") == ["V1", "V1", "V2"]

        redis.delete("STR:K1", "-STR:K2")

    def test_setex(self, redis):
        redis.setex("STR:K1", 1, "V1")
        redis.setex("-STR:K2", 1, "V2")

        assert redis.get("STR:K1") == "V1"
        assert redis.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert redis.get("STR:K1") is None
        assert redis.get("-STR:K2") is None

    def test_setnx(self, redis):
        redis.setnx("STR:K1", "V1")
        redis.setnx("-STR:K2", "V2")

        assert redis.get("STR:K1") == "V1"
        assert redis.get("-STR:K2") == "V2"

        redis.delete("STR:K1", "-STR:K2")
