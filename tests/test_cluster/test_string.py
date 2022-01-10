# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 10:09 上午'
import time


class TestString:

    def test_set_value(self, cluster):
        cluster.set("STR:K1", "VALUE1")
        cluster.set("-STR:K2", "VALUE2")

        assert cluster.get("STR:K1") == "VALUE1"
        assert cluster.get("-STR:K2") == "VALUE2"

        cluster.delete("STR:K1", "-STR:K2")

    def test_get_range(self, cluster):
        cluster.set("STR:K1", "VALUE1")
        cluster.set("-STR:K2", "VALUE2")

        assert cluster.getrange("STR:K1", -2, -1) == "E1"
        assert cluster.getrange("-STR:K2", start=2, end=-1) == "LUE2"

        cluster.delete("STR:K1", "-STR:K2")

    def test_get_set(self, cluster):
        cluster.set("STR:K1", "VALUE1")
        cluster.set("-STR:K2", "VALUE2")

        assert cluster.getset("STR:K1", "V1") == "VALUE1"
        assert cluster.getset("-STR:K2", "V2") == "VALUE2"

        assert cluster.get("STR:K1") == "V1"
        assert cluster.get("-STR:K2") == "V2"

        cluster.delete("STR:K1", "-STR:K2")

    def test_get_bit(self, cluster):
        cluster.set("STR:K1", "V1")
        cluster.set("-STR:K2", "V2")

        assert cluster.getbit("STR:K1", 3) == 1
        assert cluster.getbit("-STR:K2", 14) == 1

        cluster.setbit("STR:K1", 3, 0)
        cluster.setbit("-STR:K2", 14, 0)

        assert cluster.get("STR:K1") == "F1"
        assert cluster.get("-STR:K2") == "V0"

        cluster.delete("STR:K1", "-STR:K2")

    def test_mget(self, cluster):
        cluster.set("-{STR:K}:1", "V1")
        cluster.set("-{STR:K}:2", "V2")

        assert cluster.mget("-{STR:K}:1", "-{STR:K}:1", "-{STR:K}:2") == ["V1", "V1", "V2"]

        cluster.delete("-{STR:K}:1", "-{STR:K}:2")

    def test_setex(self, cluster):
        cluster.setex("STR:K1", 1, "V1")
        cluster.setex("-STR:K2", 1, "V2")

        assert cluster.get("STR:K1") == "V1"
        assert cluster.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert cluster.get("STR:K1") is None
        assert cluster.get("-STR:K2") is None

    def test_setnx(self, cluster):
        cluster.setnx("STR:K1", "V1")
        cluster.setnx("-STR:K2", "V2")

        assert cluster.get("STR:K1") == "V1"
        assert cluster.get("-STR:K2") == "V2"

        cluster.delete("STR:K1", "-STR:K2")

    def test_setrange(self, cluster):
        cluster.setnx("STR:K1", "V1")
        cluster.setnx("-STR:K2", "V2")

        cluster.setrange("STR:K1", 1, "T")
        cluster.setrange("-STR:K2", 1, "JLK")

        assert cluster.get("STR:K1") == "VT"
        assert cluster.get("-STR:K2") == "VJLK"

        cluster.delete("STR:K1", "-STR:K2")

    def test_strlen(self, cluster):
        cluster.set("STR:K1", "V1")
        cluster.set("-STR:K2", "UserT")

        assert cluster.strlen("STR:K1") == 2
        assert cluster.strlen("-STR:K2") == 5

        cluster.delete("STR:K1", "-STR:K2")

    def test_mset(self, cluster):
        cluster.mset({
            "-{STR:K}:1": "V1",
            "-{STR:K}:2": "V2"
        })

        assert cluster.get("-{STR:K}:1") == "V1"
        assert cluster.get("-{STR:K}:2") == "V2"

        cluster.delete("-{STR:K}:1", "-{STR:K}:2")

    def test_msetnx(self, cluster):
        cluster.msetnx({
            "-{STR:K}:1": "T1",
            "-{STR:K}:2": "V2",
        })

        assert cluster.get("-{STR:K}:1") == "T1"
        assert cluster.get("-{STR:K}:2") == "V2"

        cluster.delete("-{STR:K}:1", "-{STR:K}:2")

        cluster.set("-{STR:K}:1", "V1")

        cluster.msetnx({
            "-{STR:K}:1": "T1",
            "-{STR:K}:2": "V2"
        })
        assert cluster.get("-{STR:K}:1") == "V1"
        assert cluster.get("-{STR:K}:2") is None

        cluster.delete("-{STR:K}:1", "-{STR:K}:2")

    def test_psetex(self, cluster):
        cluster.psetex("STR:K1", 1000, "V1")
        cluster.psetex("-STR:K2", 1000, "V2")

        assert cluster.get("STR:K1") == "V1"
        assert cluster.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert cluster.get("STR:K1") is None
        assert cluster.get("STR:K2") is None

    def test_incr(self, cluster):
        cluster.incr("STR:K1")
        cluster.incr("-STR:K2")

        assert cluster.get("STR:K1") == "1"
        assert cluster.get("-STR:K2") == "1"

        cluster.delete("STR:K1", "-STR:K2")

    def test_incrby(self, cluster):
        cluster.set("STR:K1", "45")
        cluster.set("-STR:K2", "12")

        cluster.incrby("STR:K1", amount=10)
        cluster.incrby("-STR:K2", 3)

        assert cluster.get("STR:K1") == "55"
        assert cluster.get("-STR:K2") == "15"

        cluster.delete("STR:K1", "-STR:K2")

    def test_incrbyfloat(self, cluster):
        cluster.set("STR:K1", "30")
        cluster.set("-STR:K2", "10")

        cluster.incrbyfloat("STR:K1", amount=10.25)
        cluster.incrbyfloat("-STR:K2", 3.8)

        assert cluster.get("STR:K1") == "40.25"
        assert cluster.get("-STR:K2") == "13.8"

        cluster.delete("STR:K1", "-STR:K2")

    def test_decr(self, cluster):
        cluster.set("STR:K1", "30")
        cluster.set("-STR:K2", "10")

        cluster.decr("STR:K1", amount=10)
        cluster.decr("-STR:K2", 3)

        assert cluster.get("STR:K1") == "20"
        assert cluster.get("-STR:K2") == "7"

        cluster.delete("STR:K1", "-STR:K2")

    def test_decrby(self, cluster):
        cluster.set("STR:K1", "30")
        cluster.set("-STR:K2", "10")

        cluster.decrby("STR:K1", amount=10)
        cluster.decrby("-STR:K2", 3)

        assert cluster.get("STR:K1") == "20"
        assert cluster.get("-STR:K2") == "7"

        cluster.delete("STR:K1", "-STR:K2")

    def test_append(self, cluster):
        cluster.set("STR:K1", "V1")
        cluster.set("-STR:K2", "V2")

        cluster.append("STR:K1", "U1")
        cluster.append("-STR:K2", "U2")

        assert cluster.get("STR:K1") == "V1U1"
        assert cluster.get("-STR:K2") == "V2U2"

        cluster.delete("STR:K1", "-STR:K2")
