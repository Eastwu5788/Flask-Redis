# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 11:12 上午'


class TestHash:

    def test_hset(self, cluster):
        cluster.hset("HASH:V1", "K1", "V1")
        cluster.hset("-HASH:V2", "K2", "V2")
        cluster.hset("HASH:V3", "K3", "V3", mapping={
            "K4": "V4",
            "K5": "V5"
        })

        assert cluster.hget("HASH:V1", "K1") == "V1"
        assert cluster.hget("-HASH:V2", "K2") == "V2"
        assert cluster.hget("HASH:V3", "K4") == "V4"

        cluster.delete("HASH:V1", "-HASH:V2", "HASH:V3")

    def test_hdel(self, cluster):
        cluster.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
            "K3": "V3",
            "K4": "V4"
        })
        cluster.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        cluster.hdel("HASH:V1", "K2", "K3")
        cluster.hdel("-HASH:V2", "K5")

        assert cluster.hget("HASH:V1", "K2") is None
        assert cluster.hget("-HASH:V2", "K5") is None

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hexists(self, cluster):
        cluster.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        cluster.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert cluster.hexists("HASH:V1", "K2") is True
        assert cluster.hexists("HASH:V1", "K3") is not True
        assert cluster.hexists("-HASH:V2", "K5") is True
        assert cluster.hexists("-HASH:V2", "K9") is not True

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hgetall(self, cluster):
        cluster.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        cluster.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert cluster.hgetall("HASH:V1") == {
            "K1": "V1",
            "K2": "V2"
        }
        assert cluster.hgetall("-HASH:V2") == {
            "K5": "V5",
            "K6": "V6"
        }

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hincrby(self, cluster):
        cluster.hset("HASH:V1", "K1", "1", mapping={
            "K2": "5",
        })
        cluster.hset("-HASH:V2", "K5", "67", mapping={
            "K6": "23"
        })

        cluster.hincrby("HASH:V1", "K1", amount=11)
        cluster.hincrby("-HASH:V2", "K6", 55)

        assert cluster.hget("HASH:V1", "K1") == "12"
        assert cluster.hget("-HASH:V2", "K6") == "78"

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hincrbyfloat(self, cluster):
        cluster.hset("HASH:V1", "K1", "1.2")
        cluster.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        cluster.hincrbyfloat("HASH:V1", "K1", amount=5.5)
        cluster.hincrbyfloat("-HASH:V2", "K6", 3.6)

        assert cluster.hget("HASH:V1", "K1") == "6.7"
        assert cluster.hget("-HASH:V2", "K6") == "26.6"

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hkeys(self, cluster):
        cluster.hset("HASH:V1", "K1", "1.2")
        cluster.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        assert cluster.hkeys("HASH:V1") == ["K1"]
        assert cluster.hkeys("-HASH:V2") == ["K6"]

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hlen(self, cluster):
        cluster.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        cluster.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert cluster.hlen("HASH:V1") == 3
        assert cluster.hlen("-HASH:V2") == 2

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hmget(self, cluster):
        cluster.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        cluster.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert cluster.hmget("HASH:V1", "K1", "K3") == ["1.2", "V3"]
        assert cluster.hmget("-HASH:V2", "K6", "K11") == ["23", None]

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hmset(self, cluster):
        cluster.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        cluster.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert cluster.hmget("HASH:V1", "K1", "K2") == ["V1", "V2"]
        assert cluster.hmget("-HASH:V2", "K3", "K4") == ["V3", "V4"]

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hsetnx(self, cluster):
        cluster.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        cluster.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        cluster.hsetnx("HASH:V1", "K1", "TTT")
        cluster.hsetnx("HASH:V1", "K9", "V9")

        cluster.hsetnx("-HASH:V2", "K3", "SSS")
        cluster.hsetnx("-HASH:V2", "K5", "V5")

        assert cluster.hmget("HASH:V1", "K1", "K9") == ["V1", "V9"]
        assert cluster.hmget("-HASH:V2", "K3", "K5") == ["V3", "V5"]

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hvals(self, cluster):
        cluster.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        cluster.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert cluster.hvals("HASH:V1") == ["V1", "V2"]
        assert cluster.hvals("-HASH:V2") == ["V3", "V4"]

        cluster.delete("HASH:V1", "-HASH:V2")

    def test_hstrlen(self, cluster):
        cluster.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2ss"
        })
        cluster.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert cluster.hstrlen("HASH:V1", "K2") == 4
        assert cluster.hstrlen("-HASH:V2", "K3") == 2

        cluster.delete("HASH:V1", "-HASH:V2")
