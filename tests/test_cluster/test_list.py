# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 10:35 上午'


class TestList:

    def test_lpush(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T2")

        assert cluster.lindex("LIST:K1", 0) == "V2"
        assert cluster.lindex("LIST:K1", 1) == "V1"
        assert cluster.lindex("-LIST:K2", 0) == "T2"
        assert cluster.lindex("-LIST:K2", 1) == "T1"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_blpop(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T2")

        assert cluster.blpop("LIST:K1") == ("CLU:LIST:K1", "V2")
        assert cluster.blpop("-LIST:K2") == ("LIST:K2", "T2")

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_brpop(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T2")

        assert cluster.brpop("LIST:K1") == ("CLU:LIST:K1", "V1")
        assert cluster.brpop("-LIST:K2") == ("LIST:K2", "T1")

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_linsert(self, cluster):
        cluster.lpush("LIST:K1", "V1")
        cluster.lpush("-LIST:K2", "T1")

        cluster.linsert("LIST:K1", "BEFORE", "V1", "V22")
        cluster.linsert("-LIST:K2", "AFTER", "T1", "T333")

        assert cluster.lindex("LIST:K1", 0) == "V22"
        assert cluster.lindex("-LIST:K2", 1) == "T333"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_llen(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3", "T5")

        assert cluster.llen("LIST:K1") == 2
        assert cluster.llen("-LIST:K2") == 3

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_lpop(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3", "T5")

        cluster.lpop("LIST:K1")
        cluster.lpop("-LIST:K2")

        assert cluster.lindex("LIST:K1", 0) == "V1"
        assert cluster.lindex("-LIST:K2", 0) == "T3"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_lpushx(self, cluster):
        cluster.lpush("LIST:K1", "V1")
        cluster.lpushx("LIST:K1", "V2")
        cluster.lpushx("LIST:K3", "V3")

        assert cluster.lindex("LIST:K1", 0) == "V2"
        assert cluster.lindex("LIST:K3", 0) is None

        cluster.lpush("-LIST:K4", "V4")
        cluster.lpushx("-LIST:K4", "V6")
        cluster.lpushx("-LIST:K5", "V5")

        assert cluster.lindex("-LIST:K4", 0) == "V6"
        assert cluster.lindex("-LIST:K5", 0) is None

        cluster.delete("LIST:K1", "-LIST:K4")

    def test_lrange(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2", "V3")
        cluster.lpush("-LIST:K2", "T1", "T3", "T5")

        assert cluster.lrange("LIST:K1", 0, 1) == ["V3", "V2"]
        assert cluster.lrange("-LIST:K2", 0, -2) == ["T5", "T3"]

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_lrem(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3", "T3")

        cluster.lrem("LIST:K1", 1, "V2")
        cluster.lrem("-LIST:K2", 2, "T3")

        assert cluster.lindex("LIST:K1", 0) == "V2"
        assert cluster.lindex("-LIST:K2", 0) == "T1"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_lset(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3")

        cluster.lset("LIST:K1", 0, "V9")
        cluster.lset("-LIST:K2", 1, "T222")

        assert cluster.lindex("LIST:K1", 0) == "V9"
        assert cluster.lindex("-LIST:K2", 1) == "T222"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_ltrim(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3", "T3")

        cluster.ltrim("LIST:K1", 0, 1)
        cluster.ltrim("-LIST:K2", 0, 0)

        assert cluster.llen("LIST:K1") == 2
        assert cluster.llen("-LIST:K2") == 1

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_rpop(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3", "T3")

        assert cluster.rpop("LIST:K1", 2) == ["V1", "V2"]
        assert cluster.rpop("-LIST:K2", 1) == ["T1"]

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_rpoplpush(self, cluster):
        cluster.lpush("-{LIST:K}:1", "V1", "V2")
        cluster.lpush("-{LIST:K}:2", "T1", "T3")

        cluster.rpoplpush("-{LIST:K}:1", "-{LIST:K}:2")

        assert cluster.lindex("-{LIST:K}:1", -1) == "V2"
        assert cluster.lindex("-{LIST:K}:2", 0) == "V1"

        cluster.delete("-{LIST:K}:1", "-{LIST:K}:2")

    def test_rpush(self, cluster):
        cluster.lpush("LIST:K1", "V1", "V2")
        cluster.lpush("-LIST:K2", "T1", "T3")

        cluster.rpush("LIST:K1", "SS")
        cluster.rpush("-LIST:K2", "TT")

        assert cluster.lindex("LIST:K1", -1) == "SS"
        assert cluster.lindex("-LIST:K2", -1) == "TT"

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_rpushx(self, cluster):
        cluster.lpush("LIST:K1", "V1")
        cluster.lpush("-LIST:K2", "T1")

        cluster.rpushx("LIST:K1", "SS")
        cluster.rpushx("LIST:K8", "JJ")
        cluster.rpushx("-LIST:K2", "TT")
        cluster.rpushx("-LIST:K3", "KK")

        assert cluster.lindex("LIST:K1", -1) == "SS"
        assert cluster.lindex("-LIST:K2", -1) == "TT"
        assert cluster.lindex("LIST:K8", -1) is None
        assert cluster.lindex("-LIST:K3", -1) is None

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_lpos(self, cluster):
        cluster.rpush("LIST:K1", "V1", "V2", "V2")
        cluster.rpush("-LIST:K2", "T1", "T3", "T3")

        assert cluster.lpos("LIST:K1", "V2", -1, 2) == [2, 1]
        assert cluster.lpos("-LIST:K2", "T3", -1, 2) == [2, 1]

        cluster.delete("LIST:K1", "-LIST:K2")

    def test_sort(self, cluster):
        cluster.rpush("-{SORT:K}:1", "1", "6", "21", "24", "4", "6")
        cluster.rpush("-{SORT:K}:2", "1", "42", "4", "5", "23", "5")

        assert cluster.sort("-{SORT:K}:1") == ["1", "4", "6", "6", "21", "24"]
        assert cluster.sort("-{SORT:K}:1", store="-{SORT:K}:3") == 6
        assert cluster.llen("-{SORT:K}:3") == 6

        assert cluster.sort("-{SORT:K}:2") == ["1", "4", "5", "5", "23", "42"]
        assert cluster.sort("-{SORT:K}:2", store="-{SORT:K}:4")
        assert cluster.llen("-{SORT:K}:4") == 6

        cluster.delete("-{SORT:K}:1", "-{SORT:K}:2", "-{SORT:K}:3", "-{SORT:K}:4")
