# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/30 11:21 上午'


class TestList:

    def test_lpush(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T2")

        assert redis.lindex("LIST:K1", 0) == "V2"
        assert redis.lindex("LIST:K1", 1) == "V1"
        assert redis.lindex("-LIST:K2", 0) == "T2"
        assert redis.lindex("-LIST:K2", 1) == "T1"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_blpop(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T2")

        assert redis.blpop("LIST:K1") == ("EG:LIST:K1", "V2")
        assert redis.blpop("-LIST:K2") == ("LIST:K2", "T2")

        redis.delete("LIST:K1", "-LIST:K2")

    def test_brpop(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T2")

        assert redis.brpop("LIST:K1") == ("EG:LIST:K1", "V1")
        assert redis.brpop("-LIST:K2") == ("LIST:K2", "T1")

        redis.delete("LIST:K1", "-LIST:K2")

    def test_linsert(self, redis):
        redis.lpush("LIST:K1", "V1")
        redis.lpush("-LIST:K2", "T1")

        redis.linsert("LIST:K1", "BEFORE", "V1", "V22")
        redis.linsert("-LIST:K2", "AFTER", "T1", "T333")

        assert redis.lindex("LIST:K1", 0) == "V22"
        assert redis.lindex("-LIST:K2", 1) == "T333"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_llen(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T3", "T5")

        assert redis.llen("LIST:K1") == 2
        assert redis.llen("-LIST:K2") == 3

        redis.delete("LIST:K1", "-LIST:K2")

    def test_lpop(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T3", "T5")

        redis.lpop("LIST:K1")
        redis.lpop("-LIST:K2")

        assert redis.lindex("LIST:K1", 0) == "V1"
        assert redis.lindex("-LIST:K2", 0) == "T3"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_lpushx(self, redis):
        redis.lpush("LIST:K1", "V1")
        redis.lpushx("LIST:K1", "V2")
        redis.lpushx("LIST:K3", "V3")

        assert redis.lindex("LIST:K1", 0) == "V2"
        assert redis.lindex("LIST:K3", 0) is None

        redis.lpush("-LIST:K4", "V4")
        redis.lpushx("-LIST:K4", "V6")
        redis.lpushx("-LIST:K5", "V5")

        assert redis.lindex("-LIST:K4", 0) == "V6"
        assert redis.lindex("-LIST:K5", 0) is None

        redis.delete("LIST:K1", "-LIST:K4")

    def test_lrange(self, redis):
        redis.lpush("LIST:K1", "V1", "V2", "V3")
        redis.lpush("-LIST:K2", "T1", "T3", "T5")

        assert redis.lrange("LIST:K1", 0, 1) == ["V3", "V2"]
        assert redis.lrange("-LIST:K2", 0, -2) == ["T5", "T3"]

        redis.delete("LIST:K1", "-LIST:K2")

    def test_lrem(self, redis):
        redis.lpush("LIST:K1", "V1", "V2", "V2")
        redis.lpush("-LIST:K2", "T1", "T3", "T3")

        redis.lrem("LIST:K1", 1, "V2")
        redis.lrem("-LIST:K2", 2, "T3")

        assert redis.lindex("LIST:K1", 0) == "V2"
        assert redis.lindex("-LIST:K2", 0) == "T1"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_lset(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T3")

        redis.lset("LIST:K1", 0, "V9")
        redis.lset("-LIST:K2", 1, "T222")

        assert redis.lindex("LIST:K1", 0) == "V9"
        assert redis.lindex("-LIST:K2", 1) == "T222"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_ltrim(self, redis):
        redis.lpush("LIST:K1", "V1", "V2", "V2")
        redis.lpush("-LIST:K2", "T1", "T3", "T3")

        redis.ltrim("LIST:K1", 0, 1)
        redis.ltrim("-LIST:K2", 0, 0)

        assert redis.llen("LIST:K1") == 2
        assert redis.llen("-LIST:K2") == 1

        redis.delete("LIST:K1", "-LIST:K2")

    def test_rpop(self, redis):
        redis.lpush("LIST:K1", "V1", "V2", "V2")
        redis.lpush("-LIST:K2", "T1", "T3", "T3")

        assert redis.rpop("LIST:K1", 2) == ["V1", "V2"]
        assert redis.rpop("-LIST:K2", 1) == ["T1"]

        redis.delete("LIST:K1", "-LIST:K2")

    def test_rpoplpush(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T3")

        redis.rpoplpush("LIST:K1", "-LIST:K2")

        assert redis.lindex("LIST:K1", -1) == "V2"
        assert redis.lindex("-LIST:K2", 0) == "V1"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_rpush(self, redis):
        redis.lpush("LIST:K1", "V1", "V2")
        redis.lpush("-LIST:K2", "T1", "T3")

        redis.rpush("LIST:K1", "SS")
        redis.rpush("-LIST:K2", "TT")

        assert redis.lindex("LIST:K1", -1) == "SS"
        assert redis.lindex("-LIST:K2", -1) == "TT"

        redis.delete("LIST:K1", "-LIST:K2")

    def test_rpushx(self, redis):
        redis.lpush("LIST:K1", "V1")
        redis.lpush("-LIST:K2", "T1")

        redis.rpushx("LIST:K1", "SS")
        redis.rpushx("LIST:K8", "JJ")
        redis.rpushx("-LIST:K2", "TT")
        redis.rpushx("-LIST:K3", "KK")

        assert redis.lindex("LIST:K1", -1) == "SS"
        assert redis.lindex("-LIST:K2", -1) == "TT"
        assert redis.lindex("LIST:K8", -1) is None
        assert redis.lindex("-LIST:K3", -1) is None

        redis.delete("LIST:K1", "-LIST:K2")

    def test_lpos(self, redis):
        redis.rpush("LIST:K1", "V1", "V2", "V2")
        redis.rpush("-LIST:K2", "T1", "T3", "T3")

        assert redis.lpos("LIST:K1", "V2", -1, 2) == [2, 1]
        assert redis.lpos("-LIST:K2", "T3", -1, 2) == [2, 1]

        redis.delete("LIST:K1", "-LIST:K2")

    def test_sort(self, redis):
        redis.rpush("LIST:K1", "1", "6", "21", "24", "4", "6")
        redis.rpush("-LIST:K1", "1", "42", "4", "5", "23", "5")

        assert redis.sort("LIST:K1") == ["1", "4", "6", "6", "21", "24"]
        assert redis.sort("LIST:K1", store="LIST:SORT:K1") == 6
        assert redis.llen("LIST:SORT:K1") == 6

        assert redis.sort("-LIST:K1") == ["1", "4", "5", "5", "23", "42"]
        assert redis.sort("-LIST:K1", store="-LIST:SORT:K2")
        assert redis.llen("-LIST:SORT:K2") == 6

        redis.delete("LIST:K1", "-LIST:K1", "LIST:SORT:K1", "-LIST:SORT:K2")
