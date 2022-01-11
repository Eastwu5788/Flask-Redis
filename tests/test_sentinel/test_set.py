# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:33 下午'


class TestSet:

    def test_sadd(self, master, slave):
        master.sadd("SET:K1", "V1", "V1", "V2")
        master.sadd("-SET:K2", "T1", "T1", "T2", "T3")

        assert slave.scard("SET:K1") == 2
        assert slave.scard("-SET:K2") == 3

        master.delete("SET:K1", "-SET:K2")

    def test_sdiff(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert slave.sdiff("SET:K1", "-SET:K2") == {"V1", "V3"}
        assert slave.sdiff(["SET:K1", "-SET:K2"]) == {"V1", "V3"}

        assert slave.sdiff("-SET:K2", "SET:K1") == {"T1", "T3"}
        assert slave.sdiff(["-SET:K2", "SET:K1"]) == {"T1", "T3"}

        master.delete("SET:K1", "-SET:K2")

    def test_sdiffstore(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        master.sdiffstore("SET:K3", "-SET:K2", "SET:K1")
        master.sdiffstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert slave.smembers("SET:K3") == {"T1", "T3"}
        assert slave.smembers("-SET:K4") == {"V1", "V3"}

        master.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sinter(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert slave.sinter("SET:K1", "-SET:K2") == {"V2"}
        assert slave.sinter(["SET:K1", "-SET:K2"]) == {"V2"}

        master.delete("SET:K1", "-SET:K2")

    def test_sinterstore(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        master.sinterstore("SET:K3", "SET:K1", "-SET:K2")
        master.sinterstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert slave.smembers("SET:K3") == {"V2"}
        assert slave.smembers("-SET:K4") == {"V2"}

        master.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sismember(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        assert slave.sismember("SET:K1", "V2") is True
        assert slave.sismember("SET:K1", "V5") is not True
        assert slave.sismember("-SET:K2", "T1") is True
        assert slave.sismember("-SET:K2", "T55") is not True

        master.delete("SET:K1", "-SET:K2")

    def test_smove(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V2", "V3")
        master.sadd("-SET:K2", "T1", "V2", "V2", "T3")

        master.smove("SET:K1", "-SET:K2", "V3")
        master.smove("-SET:K2", "SET:K1", "T1")

        assert slave.smembers("SET:K1") == {"T1", "V2", "V1"}
        assert slave.smembers("-SET:K2") == {"V2", "V3", "T3"}

        master.delete("SET:K1", "-SET:K2")

    def test_spop(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V3")
        master.sadd("-SET:K2", "T1", "T2", "T3")

        assert master.spop("SET:K1") in {"V1", "V2", "V3"}
        assert slave.scard("SET:K1") == 2

        assert master.spop("-SET:K2") in {"T1", "T2", "T3"}
        assert slave.scard("-SET:K2") == 2

        master.delete("SET:K1", "-SET:K2")

    def test_srandmember(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V3")
        master.sadd("-SET:K2", "T1", "T2", "T3")

        assert len({"V1", "V2", "V3"} - set(slave.srandmember("SET:K1", 2))) == 1
        assert len({"T1", "T2", "T3"} - set(slave.srandmember("-SET:K2", 2))) == 1

        master.delete("SET:K1", "-SET:K2")

    def test_srem(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "V3")
        master.sadd("-SET:K2", "T1", "T2", "T3")

        master.srem("SET:K1", "V1", "V2")
        master.srem("-SET:K2", "T1")

        assert slave.smembers("SET:K1") == {"V3"}
        assert slave.smembers("-SET:K2") == {"T2", "T3"}

        master.delete("SET:K1", "-SET:K2")

    def test_sunion(self, master, slave):
        master.sadd("SET:K1", "V1", "V2")
        master.sadd("-SET:K2", "T1", "T2")

        assert slave.sunion("SET:K1", "-SET:K2") == {"V1", "V2", "T1", "T2"}
        assert slave.sunion(["SET:K1", "-SET:K2"]) == {"V1", "V2", "T1", "T2"}

        master.delete("SET:K1", "-SET:K2")

    def test_sunionstore(self, master, slave):
        master.sadd("SET:K1", "V1", "V2")
        master.sadd("-SET:K2", "T1", "T2")

        master.sunionstore("SET:K3", "SET:K1", "-SET:K2")
        master.sunionstore("-SET:K4", ["SET:K1", "-SET:K2"])

        assert slave.smembers("SET:K3") == {"V1", "V2", "T1", "T2"}
        assert slave.smembers("-SET:K4") == {"V1", "V2", "T1", "T2"}

        master.delete("SET:K1", "-SET:K2", "SET:K3", "-SET:K4")

    def test_sscan(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "U2", "U3")
        master.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        assert slave.sscan("SET:K1", 0, match="V*") == (0, ["V1", "V2"]) or (0, ["V2", "V1"])
        assert slave.sscan("-SET:K2", 0, match="T*") == (0, ["T1", "T2"]) or (0, ["T2", "T1"])

        master.delete("SET:K1", "-SET:K2")

    def test_sscan_iter(self, master, slave):
        master.sadd("SET:K1", "V1", "V2", "U2", "U3")
        master.sadd("-SET:K2", "T1", "T2", "U1", "U2")

        for v in slave.sscan_iter("SET:K1", match="V*"):
            assert v in {"V1", "V2"}

        for v in slave.sscan_iter("-SET:K2", match="U*"):
            assert v in {"U1", "U2"}

        master.delete("SET:K1", "-SET:K2")
