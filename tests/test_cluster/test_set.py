# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 10:39 上午'


class TestSet:

    def test_sadd(self, cluster):
        cluster.sadd("SET:K1", "V1", "V1", "V2")
        cluster.sadd("-SET:K2", "T1", "T1", "T2", "T3")

        assert cluster.scard("SET:K1") == 2
        assert cluster.scard("-SET:K2") == 3

        cluster.delete("SET:K1", "-SET:K2")

    def test_sdiff(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "V2", "V2", "T3")

        assert cluster.sdiff("-{SET:K}:1", "-{SET:K}:2") == {"V1", "V3"}
        assert cluster.sdiff(["-{SET:K}:1", "-{SET:K}:2"]) == {"V1", "V3"}

        assert cluster.sdiff("-{SET:K}:2", "-{SET:K}:1") == {"T1", "T3"}
        assert cluster.sdiff(["-{SET:K}:2", "-{SET:K}:1"]) == {"T1", "T3"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_sdiffstore(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "V2", "V2", "T3")

        cluster.sdiffstore("-{SET:K}:3", "-{SET:K}:2", "-{SET:K}:1")
        cluster.sdiffstore("-{SET:K}:4", ["-{SET:K}:1", "-{SET:K}:2"])

        assert cluster.smembers("-{SET:K}:3") == {"T1", "T3"}
        assert cluster.smembers("-{SET:K}:4") == {"V1", "V3"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2", "-{SET:K}:3", "-{SET:K}:4")

    def test_sinter(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "V2", "V2", "T3")

        assert cluster.sinter("-{SET:K}:1", "-{SET:K}:2") == {"V2"}
        assert cluster.sinter(["-{SET:K}:1", "-{SET:K}:2"]) == {"V2"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_sinterstore(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "V2", "V2", "T3")

        cluster.sinterstore("-{SET:K}:3", "-{SET:K}:1", "-{SET:K}:2")
        cluster.sinterstore("-{SET:K}:4", ["-{SET:K}:1", "-{SET:K}:2"])

        assert cluster.smembers("-{SET:K}:3") == {"V2"}
        assert cluster.smembers("-{SET:K}:4") == {"V2"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2", "-{SET:K}:3", "-{SET:K}:4")

    def test_sismember(self, cluster):
        cluster.sadd("SET:K1", "V1", "V2", "V2", "V3")
        cluster.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert cluster.sismember("SET:K1", "V2") is True
        assert cluster.sismember("SET:K1", "V5") is not True
        assert cluster.sismember("-SET:K2", "T1") is True
        assert cluster.sismember("-SET:K2", "T55") is not True

        cluster.delete("SET:K1", "-SET:K2")

    def test_smove(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "V2", "V2", "T3")

        cluster.smove("-{SET:K}:1", "-{SET:K}:2", "V3")
        cluster.smove("-{SET:K}:2", "-{SET:K}:1", "T1")

        assert cluster.smembers("-{SET:K}:1") == {"T1", "V2", "V1"}
        assert cluster.smembers("-{SET:K}:2") == {"V2", "V3", "T3"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_spop(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "T2", "T3")

        assert cluster.spop("-{SET:K}:1") in {"V1", "V2", "V3"}
        assert cluster.scard("-{SET:K}:1") == 2

        assert cluster.spop("-{SET:K}:2") in {"T1", "T2", "T3"}
        assert cluster.scard("-{SET:K}:2") == 2

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_srandmember(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "T2", "T3")

        assert len({"V1", "V2", "V3"} - set(cluster.srandmember("-{SET:K}:1", 2))) == 1
        assert len({"T1", "T2", "T3"} - set(cluster.srandmember("-{SET:K}:2", 2))) == 1

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_srem(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2", "V3")
        cluster.sadd("-{SET:K}:2", "T1", "T2", "T3")

        cluster.srem("-{SET:K}:1", "V1", "V2")
        cluster.srem("-{SET:K}:2", "T1")

        assert cluster.smembers("-{SET:K}:1") == {"V3"}
        assert cluster.smembers("-{SET:K}:2") == {"T2", "T3"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_sunion(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2")
        cluster.sadd("-{SET:K}:2", "T1", "T2")

        assert cluster.sunion("-{SET:K}:1", "-{SET:K}:2") == {"V1", "V2", "T1", "T2"}
        assert cluster.sunion(["-{SET:K}:1", "-{SET:K}:2"]) == {"V1", "V2", "T1", "T2"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2")

    def test_sunionstore(self, cluster):
        cluster.sadd("-{SET:K}:1", "V1", "V2")
        cluster.sadd("-{SET:K}:2", "T1", "T2")

        cluster.sunionstore("-{SET:K}:3", "-{SET:K}:1", "-{SET:K}:2")
        cluster.sunionstore("-{SET:K}:4", ["-{SET:K}:1", "-{SET:K}:2"])

        assert cluster.smembers("-{SET:K}:3") == {"V1", "V2", "T1", "T2"}
        assert cluster.smembers("-{SET:K}:4") == {"V1", "V2", "T1", "T2"}

        cluster.delete("-{SET:K}:1", "-{SET:K}:2", "-{SET:K}:3", "-{SET:K}:4")

    def test_sscan(self, cluster):
        cluster.sadd("SET:K1", "V1", "V2", "U2", "U3")
        cluster.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        assert cluster.sscan("SET:K1", 0, match="V*") == (0, ["V1", "V2"]) or (0, ["V2", "V1"])
        assert cluster.sscan("-SET:K2", 0, match="T*") == (0, ["T1", "T2"]) or (0, ["T2", "T1"])

        cluster.delete("SET:K1", "-SET:K2")

    def test_sscan_iter(self, cluster):
        cluster.sadd("SET:K1", "V1", "V2", "U2", "U3")
        cluster.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        for v in cluster.sscan_iter("SET:K1", match="V*"):
            assert v in {"V1", "V2"}

        for v in cluster.sscan_iter("-SET:K2", match="U*"):
            assert v in {"U1", "U2"}

        cluster.delete("SET:K1", "-SET:K2")
