# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:37 下午'


class TestSortedSet:

    def test_zadd(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39
        })

        assert slave.zscore("ZSORT:K1", "V2") == 23
        assert slave.zscore("-ZSORT:K2", "T3") == 39

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcard(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T3": 39
        })

        assert slave.zcard("ZSORT:K1") == 3
        assert slave.zcard("-ZSORT:K2") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcount(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39,
        })

        assert slave.zcount("ZSORT:K1", 1, 20) == 2
        assert slave.zcount("-ZSORT:K2", 1, 100) == 3

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zdiff(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert slave.zdiff(["ZSORT:K1", "-ZSORT:K2"], withscores=False) == ["V3", "V1"]
        assert slave.zdiff(["-ZSORT:K2", "ZSORT:K1"], withscores=True) == ["T3", '39', "T1", '56']

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zdiffstore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        master.zdiffstore("ZDIFF:ST:K1", ["ZSORT:K1", "-ZSORT:K2"])
        master.zdiffstore("-ZDIFF:ST:K2", ["-ZSORT:K2", "ZSORT:K1"])

        assert slave.zscore("ZDIFF:ST:K1", "V1") == 13
        assert slave.zscore("ZDIFF:ST:K1", "V2") is None

        assert slave.zscore("-ZDIFF:ST:K2", "T1") == 56
        assert slave.zscore("-ZDIFF:ST:K2", "V2") is None

        master.delete("ZSORT:K1", "-ZSORT:K2", "ZDIFF:ST:K1", "-ZDIFF:ST:K2")

    def test_zincrby(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        master.zincrby("ZSORT:K1", 10, "V2")
        master.zincrby("-ZSORT:K2", 9, "T2")

        assert slave.zscore("ZSORT:K1", "V2") == 33
        assert slave.zscore("-ZSORT:K2", "T2") == 32

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zinter(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert slave.zinter(["ZSORT:K1", "-ZSORT:K2"], aggregate="SUM", withscores=True) == [("V2", 46)]
        assert slave.zinter(["-ZSORT:K2", "ZSORT:K1"], aggregate="MIN", withscores=True) == [("V2", 23)]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zinterstore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        master.zinterstore("ZIN:K1", ["ZSORT:K1", "-ZSORT:K2"], aggregate="SUM")
        master.zinterstore("-ZIN:K2", ["-ZSORT:K2", "ZSORT:K1"], aggregate="MIN")

        assert slave.zscore("ZIN:K1", "V2") == 46
        assert slave.zscore("-ZIN:K2", "V2") == 23

        master.delete("ZSORT:K1", "-ZSORT:K2", "ZIN:K1", "-ZIN:K2")

    def test_zlexcount(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert slave.zlexcount("ZSORT:K1", "[V1", "[V3") == 3
        assert slave.zlexcount("-ZSORT:K2", "[T2", "[T3") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmax(self, master):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert master.zpopmax("ZSORT:K1") == [("V2", 23)]
        assert master.zpopmax("-ZSORT:K2") == [("T1", 56)]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmin(self, master):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert master.zpopmin("ZSORT:K1") == [("V3", 9)]
        assert master.zpopmin("-ZSORT:K2") == [("T2", 23)]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrandmember(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrandmember("ZSORT:K1", count=1, withscores=False)[0] in {"V1", "V2", "V3"}
        assert slave.zrandmember("-ZSORT:K2", count=1, withscores=False)[0] in {"T1", "T2", "T3"}

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmax(self, master):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert master.bzpopmax(["ZSORT:K1"]) == ("SEN:ZSORT:K1", "V2", 23)
        assert master.bzpopmax(["-ZSORT:K2"]) == ("ZSORT:K2", "T1", 56)

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmin(self, master):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert master.bzpopmin(["ZSORT:K1"]) == ("SEN:ZSORT:K1", "V3", 9)
        assert master.bzpopmin(["-ZSORT:K2"]) == ("ZSORT:K2", "T2", 23)

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrange(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrange("ZSORT:K1", 0, 1, withscores=True) == [("V3", 9), ("V1", 13)]
        assert slave.zrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T1", 56)]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrange(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrevrange("ZSORT:K1", 0, 1, withscores=True) == [("V2", 23), ("V1", 13)]
        assert slave.zrevrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T2", 23)]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangestore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert master.zrangestore("ZRANGE:K1", "ZSORT:K1", 0, 1)
        assert master.zrangestore("-ZRANGE:K2", "-ZSORT:K2", 1, 2)

        assert slave.zscore("ZRANGE:K1", "V3") == 9
        assert slave.zscore("-ZRANGE:K2", "T1") == 56

        master.delete("ZSORT:K1", "-ZSORT:K2", "ZRANGE:K1", "-ZRANGE:K2")

    def test_zrangebylex(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert slave.zrangebylex("ZSORT:K1", "[V1", "[V3") == ["V1", "V2", "V3"]
        assert slave.zrangebylex("-ZSORT:K2", "[T2", "[T3") == ["T2", "T3"]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebylex(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert slave.zrevrangebylex("ZSORT:K1", "[V3", "[V1") == ["V3", "V2", "V1"]
        assert slave.zrevrangebylex("-ZSORT:K2", "[T3", "[T2") == ["T3", "T2"]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangebyscore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrangebyscore("ZSORT:K1", 9, 13) == ["V3", "V1"]
        assert slave.zrangebyscore("-ZSORT:K2", 30, 100) == ["T3", "T1"]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebyscore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrevrangebyscore("ZSORT:K1", 13, 9) == ["V1", "V3"]
        assert slave.zrevrangebyscore("-ZSORT:K2", 100, 30) == ["T1", "T3"]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrank(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrank("ZSORT:K1", "V1") == 1
        assert slave.zrank("-ZSORT:K2", "T1") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrem(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert master.zrem("ZSORT:K1", "V1")
        assert master.zrem("-ZSORT:K2", "T1", "T2")

        assert slave.zcard("ZSORT:K1") == 2
        assert slave.zcard("-ZSORT:K2") == 1

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebylex(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert master.zremrangebylex("ZSORT:K1", "[V1", "[V2")
        assert master.zremrangebylex("-ZSORT:K2", "[T2", "(T3")

        assert slave.zcard("ZSORT:K1") == 1
        assert slave.zcard("-ZSORT:K2") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyrank(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        master.zremrangebyrank("ZSORT:K1", 0, 1)
        master.zremrangebyrank("-ZSORT:K2", 1, 2)

        assert slave.zcard("ZSORT:K1") == 1
        assert slave.zcard("-ZSORT:K2") == 1

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyscore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        master.zremrangebyscore("ZSORT:K1", 9, 100)
        master.zremrangebyscore("-ZSORT:K2", 40, 100)

        assert slave.zcard("ZSORT:K1") == 0
        assert slave.zcard("-ZSORT:K2") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrank(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert slave.zrevrank("ZSORT:K1", "V2") == 0
        assert slave.zrevrank("-ZSORT:K2", "T2") == 2

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zunion(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert slave.zunion(["ZSORT:K1", "-ZSORT:K2"], withscores=True) == [
            ("V3", 9),
            ("V1", 13),
            ("T3", 39),
            ("V2", 46),
            ("T1", 56)
        ]

        master.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zunionstore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        master.zunionstore("ZUNION:K1", ["ZSORT:K1", "-ZSORT:K2"])
        master.zunionstore("-ZUNION:K1", ["ZSORT:K1", "-ZSORT:K2"])

        assert slave.zscore("ZUNION:K1", "V2") == 46
        assert slave.zscore("-ZUNION:K1", "V2") == 46

        master.delete("ZSORT:K1", "-ZSORT:K2", "ZUNION:K1", "-ZUNION:K1")

    def test_zmscore(self, master, slave):
        master.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        master.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 22,
            "T3": 39,
        })

        assert slave.zmscore("ZSORT:K1", ["V3", "V2"]) == [9, 23]
        assert slave.zmscore("-ZSORT:K2", ["T2", "T4"]) == [22, None]

        master.delete("ZSORT:K1", "-ZSORT:K2")
