# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:07 下午'
import time


class TestString:

    def test_set_value(self, master, slave):
        master.set("STR:K1", "VALUE1")
        master.set("-STR:K2", "VALUE2")

        assert slave.get("STR:K1") == "VALUE1"
        assert slave.get("-STR:K2") == "VALUE2"

        master.delete("STR:K1", "-STR:K2")

    def test_get_range(self, master, slave):
        master.set("STR:K1", "VALUE1")
        master.set("-STR:K2", "VALUE2")

        assert slave.getrange("STR:K1", -2, -1) == "E1"
        assert slave.getrange("-STR:K2", start=2, end=-1) == "LUE2"

        master.delete("STR:K1", "-STR:K2")

    def test_get_set(self, master, slave):
        master.set("STR:K1", "VALUE1")
        master.set("-STR:K2", "VALUE2")

        assert master.getset("STR:K1", "V1") == "VALUE1"
        assert master.getset("-STR:K2", "V2") == "VALUE2"

        assert slave.get("STR:K1") == "V1"
        assert slave.get("-STR:K2") == "V2"

        master.delete("STR:K1", "-STR:K2")

    def test_get_bit(self, master, slave):
        master.set("STR:K1", "V1")
        master.set("-STR:K2", "V2")

        assert slave.getbit("STR:K1", 3) == 1
        assert slave.getbit("-STR:K2", 14) == 1

        master.setbit("STR:K1", 3, 0)
        master.setbit("-STR:K2", 14, 0)

        assert slave.get("STR:K1") == "F1"
        assert slave.get("-STR:K2") == "V0"

        master.delete("STR:K1", "-STR:K2")

    def test_mget(self, master, slave):
        master.set("STR:K1", "V1")
        master.set("-STR:K2", "V2")

        assert slave.mget("STR:K1", "STR:K1", "-STR:K2") == ["V1", "V1", "V2"]

        master.delete("STR:K1", "-STR:K2")

    def test_setex(self, master, slave):
        master.setex("STR:K1", 1, "V1")
        master.setex("-STR:K2", 1, "V2")

        assert slave.get("STR:K1") == "V1"
        assert slave.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert slave.get("STR:K1") is None
        assert slave.get("-STR:K2") is None

    def test_setnx(self, master, slave):
        master.setnx("STR:K1", "V1")
        master.setnx("-STR:K2", "V2")

        assert slave.get("STR:K1") == "V1"
        assert slave.get("-STR:K2") == "V2"

        master.delete("STR:K1", "-STR:K2")

    def test_setrange(self, master, slave):
        master.setnx("STR:K1", "V1")
        master.setnx("-STR:K2", "V2")

        master.setrange("STR:K1", 1, "T")
        master.setrange("-STR:K2", 1, "JLK")

        assert slave.get("STR:K1") == "VT"
        assert slave.get("-STR:K2") == "VJLK"

        master.delete("STR:K1", "-STR:K2")

    def test_strlen(self, master, slave):
        master.set("STR:K1", "V1")
        master.set("-STR:K2", "UserT")

        assert slave.strlen("STR:K1") == 2
        assert slave.strlen("-STR:K2") == 5

        master.delete("STR:K1", "-STR:K2")

    def test_mset(self, master, slave):
        master.mset({
            "STR:K1": "V1",
            "-STR:K2": "V2"
        })

        assert slave.get("STR:K1") == "V1"
        assert slave.get("-STR:K2") == "V2"

        master.delete("STR:K1", "-STR:K2")

    def test_msetnx(self, master, slave):
        master.msetnx({
            "STR:K1": "T1",
            "-STR:K2": "V2",
        })

        assert slave.get("STR:K1") == "T1"
        assert slave.get("-STR:K2") == "V2"

        master.delete("STR:K1", "-STR:K2")

        master.set("STR:K1", "V1")

        master.msetnx({
            "STR:K1": "T1",
            "STR:K2": "V2"
        })
        assert slave.get("STR:K1") == "V1"
        assert slave.get("STR:K2") is None

        master.delete("STR:K1")

    def test_psetex(self, master, slave):
        master.psetex("STR:K1", 1000, "V1")
        master.psetex("-STR:K2", 1000, "V2")

        assert slave.get("STR:K1") == "V1"
        assert slave.get("-STR:K2") == "V2"

        time.sleep(1.5)

        assert slave.get("STR:K1") is None
        assert slave.get("STR:K2") is None

    def test_incr(self, master, slave):
        master.incr("STR:K1")
        master.incr("-STR:K2")

        assert slave.get("STR:K1") == "1"
        assert slave.get("-STR:K2") == "1"

        master.delete("STR:K1", "-STR:K2")

    def test_incrby(self, master, slave):
        master.set("STR:K1", "45")
        master.set("-STR:K2", "12")

        master.incrby("STR:K1", amount=10)
        master.incrby("-STR:K2", 3)

        assert slave.get("STR:K1") == "55"
        assert slave.get("-STR:K2") == "15"

        master.delete("STR:K1", "-STR:K2")

    def test_incrbyfloat(self, master, slave):
        master.set("STR:K1", "30")
        master.set("-STR:K2", "10")

        master.incrbyfloat("STR:K1", amount=10.25)
        master.incrbyfloat("-STR:K2", 3.8)

        assert slave.get("STR:K1") == "40.25"
        assert slave.get("-STR:K2") == "13.8"

        master.delete("STR:K1", "-STR:K2")

    def test_decr(self, master, slave):
        master.set("STR:K1", "30")
        master.set("-STR:K2", "10")

        master.decr("STR:K1", amount=10)
        master.decr("-STR:K2", 3)

        assert slave.get("STR:K1") == "20"
        assert slave.get("-STR:K2") == "7"

        master.delete("STR:K1", "-STR:K2")

    def test_decrby(self, master, slave):
        master.set("STR:K1", "30")
        master.set("-STR:K2", "10")

        master.decrby("STR:K1", amount=10)
        master.decrby("-STR:K2", 3)

        assert slave.get("STR:K1") == "20"
        assert slave.get("-STR:K2") == "7"

        master.delete("STR:K1", "-STR:K2")

    def test_append(self, master, slave):
        master.set("STR:K1", "V1")
        master.set("-STR:K2", "V2")

        master.append("STR:K1", "U1")
        master.append("-STR:K2", "U2")

        assert slave.get("STR:K1") == "V1U1"
        assert slave.get("-STR:K2") == "V2U2"

        master.delete("STR:K1", "-STR:K2")
