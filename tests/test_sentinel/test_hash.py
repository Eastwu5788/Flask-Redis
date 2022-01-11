# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:47 下午'


class TestHash:

    def test_hset(self, master, slave):
        master.hset("HASH:V1", "K1", "V1")
        master.hset("-HASH:V2", "K2", "V2")
        master.hset("HASH:V3", "K3", "V3", mapping={
            "K4": "V4",
            "K5": "V5"
        })

        assert slave.hget("HASH:V1", "K1") == "V1"
        assert slave.hget("-HASH:V2", "K2") == "V2"
        assert slave.hget("HASH:V3", "K4") == "V4"

        master.delete("HASH:V1", "-HASH:V2", "HASH:V3")

    def test_hdel(self, master, slave):
        master.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
            "K3": "V3",
            "K4": "V4"
        })
        master.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        master.hdel("HASH:V1", "K2", "K3")
        master.hdel("-HASH:V2", "K5")

        assert slave.hget("HASH:V1", "K2") is None
        assert slave.hget("-HASH:V2", "K5") is None

        master.delete("HASH:V1", "-HASH:V2")

    def test_hexists(self, master, slave):
        master.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        master.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert slave.hexists("HASH:V1", "K2") is True
        assert slave.hexists("HASH:V1", "K3") is not True
        assert slave.hexists("-HASH:V2", "K5") is True
        assert slave.hexists("-HASH:V2", "K9") is not True

        master.delete("HASH:V1", "-HASH:V2")

    def test_hgetall(self, master, slave):
        master.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        master.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert slave.hgetall("HASH:V1") == {
            "K1": "V1",
            "K2": "V2"
        }
        assert slave.hgetall("-HASH:V2") == {
            "K5": "V5",
            "K6": "V6"
        }

        master.delete("HASH:V1", "-HASH:V2")

    def test_hincrby(self, master, slave):
        master.hset("HASH:V1", "K1", "1", mapping={
            "K2": "5",
        })
        master.hset("-HASH:V2", "K5", "67", mapping={
            "K6": "23"
        })

        master.hincrby("HASH:V1", "K1", amount=11)
        master.hincrby("-HASH:V2", "K6", 55)

        assert slave.hget("HASH:V1", "K1") == "12"
        assert slave.hget("-HASH:V2", "K6") == "78"

        master.delete("HASH:V1", "-HASH:V2")

    def test_hincrbyfloat(self, master, slave):
        master.hset("HASH:V1", "K1", "1.2")
        master.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        master.hincrbyfloat("HASH:V1", "K1", amount=5.5)
        master.hincrbyfloat("-HASH:V2", "K6", 3.6)

        assert slave.hget("HASH:V1", "K1") == "6.7"
        assert slave.hget("-HASH:V2", "K6") == "26.6"

        master.delete("HASH:V1", "-HASH:V2")

    def test_hkeys(self, master, slave):
        master.hset("HASH:V1", "K1", "1.2")
        master.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        assert slave.hkeys("HASH:V1") == ["K1"]
        assert slave.hkeys("-HASH:V2") == ["K6"]

        master.delete("HASH:V1", "-HASH:V2")

    def test_hlen(self, master, slave):
        master.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        master.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert slave.hlen("HASH:V1") == 3
        assert slave.hlen("-HASH:V2") == 2

        master.delete("HASH:V1", "-HASH:V2")

    def test_hmget(self, master, slave):
        master.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        master.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert slave.hmget("HASH:V1", "K1", "K3") == ["1.2", "V3"]
        assert slave.hmget("-HASH:V2", "K6", "K11") == ["23", None]

        master.delete("HASH:V1", "-HASH:V2")

    def test_hmset(self, master, slave):
        master.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        master.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert slave.hmget("HASH:V1", "K1", "K2") == ["V1", "V2"]
        assert slave.hmget("-HASH:V2", "K3", "K4") == ["V3", "V4"]

        master.delete("HASH:V1", "-HASH:V2")

    def test_hsetnx(self, master, slave):
        master.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        master.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        master.hsetnx("HASH:V1", "K1", "TTT")
        master.hsetnx("HASH:V1", "K9", "V9")

        master.hsetnx("-HASH:V2", "K3", "SSS")
        master.hsetnx("-HASH:V2", "K5", "V5")

        assert slave.hmget("HASH:V1", "K1", "K9") == ["V1", "V9"]
        assert slave.hmget("-HASH:V2", "K3", "K5") == ["V3", "V5"]

        master.delete("HASH:V1", "-HASH:V2")

    def test_hvals(self, master, slave):
        master.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        master.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert slave.hvals("HASH:V1") == ["V1", "V2"]
        assert slave.hvals("-HASH:V2") == ["V3", "V4"]

        master.delete("HASH:V1", "-HASH:V2")

    def test_hstrlen(self, master, slave):
        master.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2ss"
        })
        master.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert slave.hstrlen("HASH:V1", "K2") == 4
        assert slave.hstrlen("-HASH:V2", "K3") == 2

        master.delete("HASH:V1", "-HASH:V2")
