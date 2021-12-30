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

    def test_setrange(self, redis):
        redis.setnx("STR:K1", "V1")
        redis.setnx("-STR:K2", "V2")

        redis.setrange("STR:K1", 1, "T")
        redis.setrange("-STR:K2", 1, "JLK")

        assert redis.get("STR:K1") == "VT"
        assert redis.get("-STR:K2") == "VJLK"

        redis.delete("STR:K1", "-STR:K2")

    def test_strlen(self, redis):
        redis.set("STR:K1", "V1")
        redis.set("-STR:K2", "UserT")

        assert redis.strlen("STR:K1") == 2
        assert redis.strlen("-STR:K2") == 5

        redis.delete("STR:K1", "-STR:K2")

    def test_mset(self, redis):
        redis.mset({
            "STR:K1": "V1",
            "-STR:K2": "V2"
        })

        assert redis.get("STR:K1") == "V1"
        assert redis.get("-STR:K2") == "V2"

        redis.delete("STR:K1", "-STR:K2")

    def test_msetnx(self, redis):
        redis.msetnx({
            "STR:K1": "T1",
            "-STR:K2": "V2",
        })

        assert redis.get("STR:K1") == "T1"
        assert redis.get("-STR:K2") == "V2"

        redis.delete("STR:K1", "-STR:K2")

        redis.set("STR:K1", "V1")

        redis.msetnx({
            "STR:K1": "T1",
            "STR:K2": "V2"
        })
        assert redis.get("STR:K1") == "V1"
        assert redis.get("STR:K2") is None

        redis.delete("STR:K1")

    def test_psetex(self, redis):
        redis.psetex("STR:K1", 1000, "V1")
        redis.psetex("-STR:K2", 1000, "V2")

        assert redis.get("STR:K1") == "V1"
        assert redis.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert redis.get("STR:K1") is None
        assert redis.get("STR:K2") is None

    def test_incr(self, redis):
        redis.incr("STR:K1")
        redis.incr("-STR:K2")

        assert redis.get("STR:K1") == "1"
        assert redis.get("-STR:K2") == "1"

        redis.delete("STR:K1", "-STR:K2")

    def test_incrby(self, redis):
        redis.set("STR:K1", "45")
        redis.set("-STR:K2", "12")

        redis.incrby("STR:K1", amount=10)
        redis.incrby("-STR:K2", 3)

        assert redis.get("STR:K1") == "55"
        assert redis.get("-STR:K2") == "15"

        redis.delete("STR:K1", "-STR:K2")

    def test_incrbyfloat(self, redis):
        redis.set("STR:K1", "30")
        redis.set("-STR:K2", "10")

        redis.incrbyfloat("STR:K1", amount=10.25)
        redis.incrbyfloat("-STR:K2", 3.8)

        assert redis.get("STR:K1") == "40.25"
        assert redis.get("-STR:K2") == "13.8"

        redis.delete("STR:K1", "-STR:K2")

    def test_decr(self, redis):
        redis.set("STR:K1", "30")
        redis.set("-STR:K2", "10")

        redis.decr("STR:K1", amount=10)
        redis.decr("-STR:K2", 3)

        assert redis.get("STR:K1") == "20"
        assert redis.get("-STR:K2") == "7"

        redis.delete("STR:K1", "-STR:K2")

    def test_decrby(self, redis):
        redis.set("STR:K1", "30")
        redis.set("-STR:K2", "10")

        redis.decrby("STR:K1", amount=10)
        redis.decrby("-STR:K2", 3)

        assert redis.get("STR:K1") == "20"
        assert redis.get("-STR:K2") == "7"

        redis.delete("STR:K1", "-STR:K2")

    def test_append(self, redis):
        redis.set("STR:K1", "V1")
        redis.set("-STR:K2", "V2")

        redis.append("STR:K1", "U1")
        redis.append("-STR:K2", "U2")

        assert redis.get("STR:K1") == "V1U1"
        assert redis.get("-STR:K2") == "V2U2"

        redis.delete("STR:K1", "-STR:K2")
