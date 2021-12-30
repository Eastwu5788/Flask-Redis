# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/30 9:39 上午'


class TestHash:

    def test_hset(self, redis):
        redis.hset("HASH:V1", "K1", "V1")
        redis.hset("-HASH:V2", "K2", "V2")
        redis.hset("HASH:V3", "K3", "V3", mapping={
            "K4": "V4",
            "K5": "V5"
        })

        assert redis.hget("HASH:V1", "K1") == "V1"
        assert redis.hget("-HASH:V2", "K2") == "V2"
        assert redis.hget("HASH:V3", "K4") == "V4"

        redis.delete("HASH:V1", "-HASH:V2", "HASH:V3")

    def test_hdel(self, redis):
        redis.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
            "K3": "V3",
            "K4": "V4"
        })
        redis.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        redis.hdel("HASH:V1", "K2", "K3")
        redis.hdel("-HASH:V2", "K5")

        assert redis.hget("HASH:V1", "K2") is None
        assert redis.hget("-HASH:V2", "K5") is None

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hexists(self, redis):
        redis.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        redis.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert redis.hexists("HASH:V1", "K2") is True
        assert redis.hexists("HASH:V1", "K3") is not True
        assert redis.hexists("-HASH:V2", "K5") is True
        assert redis.hexists("-HASH:V2", "K9") is not True

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hgetall(self, redis):
        redis.hset("HASH:V1", "K1", "V1", mapping={
            "K2": "V2",
        })
        redis.hset("-HASH:V2", "K5", "V5", mapping={
            "K6": "V6"
        })

        assert redis.hgetall("HASH:V1") == {
            "K1": "V1",
            "K2": "V2"
        }
        assert redis.hgetall("-HASH:V2") == {
            "K5": "V5",
            "K6": "V6"
        }

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hincrby(self, redis):
        redis.hset("HASH:V1", "K1", "1", mapping={
            "K2": "5",
        })
        redis.hset("-HASH:V2", "K5", "67", mapping={
            "K6": "23"
        })

        redis.hincrby("HASH:V1", "K1", amount=11)
        redis.hincrby("-HASH:V2", "K6", 55)

        assert redis.hget("HASH:V1", "K1") == "12"
        assert redis.hget("-HASH:V2", "K6") == "78"

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hincrbyfloat(self, redis):
        redis.hset("HASH:V1", "K1", "1.2")
        redis.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        redis.hincrbyfloat("HASH:V1", "K1", amount=5.5)
        redis.hincrbyfloat("-HASH:V2", "K6", 3.6)

        assert redis.hget("HASH:V1", "K1") == "6.7"
        assert redis.hget("-HASH:V2", "K6") == "26.6"

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hkeys(self, redis):
        redis.hset("HASH:V1", "K1", "1.2")
        redis.hset("-HASH:V2", mapping={
            "K6": "23"
        })

        assert redis.hkeys("HASH:V1") == ["K1"]
        assert redis.hkeys("-HASH:V2") == ["K6"]

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hlen(self, redis):
        redis.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        redis.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert redis.hlen("HASH:V1") == 3
        assert redis.hlen("-HASH:V2") == 2

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hmget(self, redis):
        redis.hset("HASH:V1", "K1", "1.2", {
            "K3": "V3",
            "K4": "V4"
        })
        redis.hset("-HASH:V2", mapping={
            "K6": "23",
            "K9": "V3"
        })

        assert redis.hmget("HASH:V1", "K1", "K3") == ["1.2", "V3"]
        assert redis.hmget("-HASH:V2", "K6", "K11") == ["23", None]

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hmset(self, redis):
        redis.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        redis.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert redis.hmget("HASH:V1", "K1", "K2") == ["V1", "V2"]
        assert redis.hmget("-HASH:V2", "K3", "K4") == ["V3", "V4"]

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hsetnx(self, redis):
        redis.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        redis.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        redis.hsetnx("HASH:V1", "K1", "TTT")
        redis.hsetnx("HASH:V1", "K9", "V9")

        redis.hsetnx("-HASH:V2", "K3", "SSS")
        redis.hsetnx("-HASH:V2", "K5", "V5")

        assert redis.hmget("HASH:V1", "K1", "K9") == ["V1", "V9"]
        assert redis.hmget("-HASH:V2", "K3", "K5") == ["V3", "V5"]

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hvals(self, redis):
        redis.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2"
        })
        redis.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert redis.hvals("HASH:V1") == ["V1", "V2"]
        assert redis.hvals("-HASH:V2") == ["V3", "V4"]

        redis.delete("HASH:V1", "-HASH:V2")

    def test_hstrlen(self, redis):
        redis.hmset("HASH:V1", {
            "K1": "V1",
            "K2": "V2ss"
        })
        redis.hmset("-HASH:V2", mapping={
            "K3": "V3",
            "K4": "V4"
        })

        assert redis.hstrlen("HASH:V1", "K2") == 4
        assert redis.hstrlen("-HASH:V2", "K3") == 2

        redis.delete("HASH:V1", "-HASH:V2")
