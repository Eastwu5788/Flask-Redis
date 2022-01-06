# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/30 2:52 下午'


class TestSet:

    def test_sadd(self, redis):
        redis.sadd("SET:K1", "V1", "V1", "V2")
        redis.sadd("-SET:K2", "T1", "T1", "T2", "T3")

        assert redis.scard("SET:K1") == 2
        assert redis.scard("-SET:K2") == 3

        redis.delete("SET:K1", "-SET:K2")

    def test_sdiff(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert redis.sdiff("SET:K1", "-SET:K2") == {"V1", "V3"}
        assert redis.sdiff(["SET:K1", "-SET:K2"]) == {"V1", "V3"}

        assert redis.sdiff("-SET:K2", "SET:K1") == {"T1", "T3"}
        assert redis.sdiff(["-SET:K2", "SET:K1"]) == {"T1", "T3"}

        redis.delete("SET:K1", "-SET:K2")

    def test_sdiffstore(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        redis.sdiffstore("SET:K3", "-SET:K2", "SET:K1")
        redis.sdiffstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert redis.smembers("SET:K3") == {"T1", "T3"}
        assert redis.smembers("-SET:K4") == {"V1", "V3"}

        redis.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sinter(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert redis.sinter("SET:K1", "-SET:K2") == {"V2"}
        assert redis.sinter(["SET:K1", "-SET:K2"]) == {"V2"}

        redis.delete("SET:K1", "-SET:K2")

    def test_sinterstore(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        redis.sinterstore("SET:K3", "SET:K1", "-SET:K2")
        redis.sinterstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert redis.smembers("SET:K3") == {"V2"}
        assert redis.smembers("-SET:K4") == {"V2"}

        redis.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sismember(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert redis.sismember("SET:K1", "V2") is True
        assert redis.sismember("SET:K1", "V5") is not True
        assert redis.sismember("-SET:K2", "T1") is True
        assert redis.sismember("-SET:K2", "T55") is not True

        redis.delete("SET:K1", "-SET:K2")

    def test_smove(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        redis.smove("SET:K1", "-SET:K2", "V3")
        redis.smove("-SET:K2", "SET:K1", "T1")

        assert redis.smembers("SET:K1") == {"T1", "V2", "V1"}
        assert redis.smembers("-SET:K2") == {"V2", "V3", "T3"}

        redis.delete("SET:K1", "-SET:K2")

    def test_spop(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "T2", "T3")

        assert redis.spop("SET:K1") in {"V1", "V2", "V3"}
        assert redis.scard("SET:K1") == 2

        assert redis.spop("-SET:K2") in {"T1", "T2", "T3"}
        assert redis.scard("-SET:K2") == 2

        redis.delete("SET:K1", "-SET:K2")

    def test_srandmember(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "T2", "T3")

        assert len({"V1", "V2", "V3"} - set(redis.srandmember("SET:K1", 2))) == 1
        assert len({"T1", "T2", "T3"} - set(redis.srandmember("-SET:K2", 2))) == 1

        redis.delete("SET:K1", "-SET:K2")

    def test_srem(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "V3")
        redis.sadd("-SET:K2", "T1", "T2", "T3")

        redis.srem("SET:K1", "V1", "V2")
        redis.srem("-SET:K2", "T1")

        assert redis.smembers("SET:K1") == {"V3"}
        assert redis.smembers("-SET:K2") == {"T2", "T3"}

        redis.delete("SET:K1", "-SET:K2")

    def test_sunion(self, redis):
        redis.sadd("SET:K1", "V1", "V2")
        redis.sadd("-SET:K2", "T1", "T2")

        assert redis.sunion("SET:K1", "-SET:K2") == {"V1", "V2", "T1", "T2"}
        assert redis.sunion(["SET:K1", "-SET:K2"]) == {"V1", "V2", "T1", "T2"}

        redis.delete("SET:K1", "-SET:K2")

    def test_sunionstore(self, redis):
        redis.sadd("SET:K1", "V1", "V2")
        redis.sadd("-SET:K2", "T1", "T2")

        redis.sunionstore("SET:K3", "SET:K1", "-SET:K2")
        redis.sunionstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert redis.smembers("SET:K3") == {"V1", "V2", "T1", "T2"}
        assert redis.smembers("-SET:K4") == {"V1", "V2", "T1", "T2"}

        redis.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sscan(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "U2", "U3")
        redis.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        assert redis.sscan("SET:K1", 0, match="V*") == (0, ["V1", "V2"]) or (0, ["V2", "V1"])
        assert redis.sscan("-SET:K2", 0, match="T*") == (0, ["T1", "T2"]) or (0, ["T2", "T1"])

        redis.delete("SET:K1", "-SET:K2")

    def test_sscan_iter(self, redis):
        redis.sadd("SET:K1", "V1", "V2", "U2", "U3")
        redis.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        for v in redis.sscan_iter("SET:K1", match="V*"):
            assert v in {"V1", "V2"}

        for v in redis.sscan_iter("-SET:K2", match="U*"):
            assert v in {"U1", "U2"}

        redis.delete("SET:K1", "-SET:K2")
