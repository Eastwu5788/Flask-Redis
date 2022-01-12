# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:27 下午'


class TestList:

    def test_lpush(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T2")

        assert slave.lindex("LIST:K1", 0) == "V2"
        assert slave.lindex("LIST:K1", 1) == "V1"
        assert slave.lindex("-LIST:K2", 0) == "T2"
        assert slave.lindex("-LIST:K2", 1) == "T1"

        master.delete("LIST:K1", "-LIST:K2")

    def test_blpop(self, master):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T2")

        assert master.blpop("LIST:K1") == ("SEN:LIST:K1", "V2")
        assert master.blpop("-LIST:K2") == ("LIST:K2", "T2")

        master.delete("LIST:K1", "-LIST:K2")

    def test_brpop(self, master):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T2")

        assert master.brpop("LIST:K1") == ("SEN:LIST:K1", "V1")
        assert master.brpop("-LIST:K2") == ("LIST:K2", "T1")

        master.delete("LIST:K1", "-LIST:K2")

    def test_linsert(self, master, slave):
        master.lpush("LIST:K1", "V1")
        master.lpush("-LIST:K2", "T1")

        master.linsert("LIST:K1", "BEFORE", "V1", "V22")
        master.linsert("-LIST:K2", "AFTER", "T1", "T333")

        assert slave.lindex("LIST:K1", 0) == "V22"
        assert slave.lindex("-LIST:K2", 1) == "T333"

        master.delete("LIST:K1", "-LIST:K2")

    def test_llen(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T3", "T5")

        assert slave.llen("LIST:K1") == 2
        assert slave.llen("-LIST:K2") == 3

        master.delete("LIST:K1", "-LIST:K2")

    def test_lpop(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T3", "T5")

        master.lpop("LIST:K1")
        master.lpop("-LIST:K2")

        assert slave.lindex("LIST:K1", 0) == "V1"
        assert slave.lindex("-LIST:K2", 0) == "T3"

        master.delete("LIST:K1", "-LIST:K2")

    def test_lpushx(self, master, slave):
        master.lpush("LIST:K1", "V1")
        master.lpushx("LIST:K1", "V2")
        master.lpushx("LIST:K3", "V3")

        assert slave.lindex("LIST:K1", 0) == "V2"
        assert slave.lindex("LIST:K3", 0) is None

        master.lpush("-LIST:K4", "V4")
        master.lpushx("-LIST:K4", "V6")
        master.lpushx("-LIST:K5", "V5")

        assert slave.lindex("-LIST:K4", 0) == "V6"
        assert slave.lindex("-LIST:K5", 0) is None

        master.delete("LIST:K1", "-LIST:K4")

    def test_lrange(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2", "V3")
        master.lpush("-LIST:K2", "T1", "T3", "T5")

        assert slave.lrange("LIST:K1", 0, 1) == ["V3", "V2"]
        assert slave.lrange("-LIST:K2", 0, -2) == ["T5", "T3"]

        master.delete("LIST:K1", "-LIST:K2")

    def test_lrem(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2", "V2")
        master.lpush("-LIST:K2", "T1", "T3", "T3")

        master.lrem("LIST:K1", 1, "V2")
        master.lrem("-LIST:K2", 2, "T3")

        assert slave.lindex("LIST:K1", 0) == "V2"
        assert slave.lindex("-LIST:K2", 0) == "T1"

        master.delete("LIST:K1", "-LIST:K2")

    def test_lset(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T3")

        master.lset("LIST:K1", 0, "V9")
        master.lset("-LIST:K2", 1, "T222")

        assert slave.lindex("LIST:K1", 0) == "V9"
        assert slave.lindex("-LIST:K2", 1) == "T222"

        master.delete("LIST:K1", "-LIST:K2")

    def test_ltrim(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2", "V2")
        master.lpush("-LIST:K2", "T1", "T3", "T3")

        master.ltrim("LIST:K1", 0, 1)
        master.ltrim("-LIST:K2", 0, 0)

        assert slave.llen("LIST:K1") == 2
        assert slave.llen("-LIST:K2") == 1

        master.delete("LIST:K1", "-LIST:K2")

    def test_rpop(self, master):
        master.lpush("LIST:K1", "V1", "V2", "V2")
        master.lpush("-LIST:K2", "T1", "T3", "T3")

        assert master.rpop("LIST:K1", 2) == ["V1", "V2"]
        assert master.rpop("-LIST:K2", 1) == ["T1"]

        master.delete("LIST:K1", "-LIST:K2")

    def test_rpoplpush(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T3")

        master.rpoplpush("LIST:K1", "-LIST:K2")

        assert slave.lindex("LIST:K1", -1) == "V2"
        assert slave.lindex("-LIST:K2", 0) == "V1"

        master.delete("LIST:K1", "-LIST:K2")

    def test_rpush(self, master, slave):
        master.lpush("LIST:K1", "V1", "V2")
        master.lpush("-LIST:K2", "T1", "T3")

        master.rpush("LIST:K1", "SS")
        master.rpush("-LIST:K2", "TT")

        assert slave.lindex("LIST:K1", -1) == "SS"
        assert slave.lindex("-LIST:K2", -1) == "TT"

        master.delete("LIST:K1", "-LIST:K2")

    def test_rpushx(self, master, slave):
        master.lpush("LIST:K1", "V1")
        master.lpush("-LIST:K2", "T1")

        master.rpushx("LIST:K1", "SS")
        master.rpushx("LIST:K8", "JJ")
        master.rpushx("-LIST:K2", "TT")
        master.rpushx("-LIST:K3", "KK")

        assert slave.lindex("LIST:K1", -1) == "SS"
        assert slave.lindex("-LIST:K2", -1) == "TT"
        assert slave.lindex("LIST:K8", -1) is None
        assert slave.lindex("-LIST:K3", -1) is None

        master.delete("LIST:K1", "-LIST:K2")

    def test_lpos(self, master, slave):
        master.rpush("LIST:K1", "V1", "V2", "V2")
        master.rpush("-LIST:K2", "T1", "T3", "T3")

        assert slave.lpos("LIST:K1", "V2", -1, 2) == [2, 1]
        assert slave.lpos("-LIST:K2", "T3", -1, 2) == [2, 1]

        master.delete("LIST:K1", "-LIST:K2")

    def test_sort(self, master, slave):
        master.rpush("LIST:K1", "1", "6", "21", "24", "4", "6")
        master.rpush("-LIST:K1", "1", "42", "4", "5", "23", "5")

        assert master.sort("LIST:K1") == ["1", "4", "6", "6", "21", "24"]
        assert master.sort("LIST:K1", store="LIST:SORT:K1") == 6
        assert slave.llen("LIST:SORT:K1") == 6

        assert master.sort("-LIST:K1") == ["1", "4", "5", "5", "23", "42"]
        assert master.sort("-LIST:K1", store="-LIST:SORT:K2")
        assert slave.llen("-LIST:SORT:K2") == 6

        master.delete("LIST:K1", "-LIST:K1", "LIST:SORT:K1", "-LIST:SORT:K2")
