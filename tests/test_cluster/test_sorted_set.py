# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 10:44 上午'


class TestSortedSet:

    def test_zadd(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39
        })

        assert cluster.zscore("ZSORT:K1", "V2") == 23
        assert cluster.zscore("-ZSORT:K2", "T3") == 39

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcard(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T3": 39
        })

        assert cluster.zcard("ZSORT:K1") == 3
        assert cluster.zcard("-ZSORT:K2") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcount(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39,
        })

        assert cluster.zcount("ZSORT:K1", 1, 20) == 2
        assert cluster.zcount("-ZSORT:K2", 1, 100) == 3

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zdiff(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert cluster.zdiff(["-{ZSORT:K}:1", "-{ZSORT:K}:2"], withscores=False) == ["V3", "V1"]
        assert cluster.zdiff(["-{ZSORT:K}:2", "-{ZSORT:K}:1"], withscores=True) == ["T3", '39', "T1", '56']

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2")

    def test_zdiffstore(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        cluster.zdiffstore("-{ZSORT:K}:3", ["-{ZSORT:K}:1", "-{ZSORT:K}:2"])
        cluster.zdiffstore("-{ZSORT:K}:4", ["-{ZSORT:K}:2", "-{ZSORT:K}:1"])

        assert cluster.zscore("-{ZSORT:K}:3", "V1") == 13
        assert cluster.zscore("-{ZSORT:K}:3", "V2") is None

        assert cluster.zscore("-{ZSORT:K}:4", "T1") == 56
        assert cluster.zscore("-{ZSORT:K}:4", "V2") is None

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2", "-{ZSORT:K}:3", "-{ZSORT:K}:4")

    def test_zincrby(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        cluster.zincrby("ZSORT:K1", 10, "V2")
        cluster.zincrby("-ZSORT:K2", 9, "T2")

        assert cluster.zscore("ZSORT:K1", "V2") == 33
        assert cluster.zscore("-ZSORT:K2", "T2") == 32

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zinter(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert cluster.zinter(["-{ZSORT:K}:1", "-{ZSORT:K}:2"], aggregate="SUM", withscores=True) == [("V2", 46)]
        assert cluster.zinter(["-{ZSORT:K}:2", "-{ZSORT:K}:1"], aggregate="MIN", withscores=True) == [("V2", 23)]

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2")

    def test_zinterstore(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        cluster.zinterstore("-{ZSORT:K}:3", ["-{ZSORT:K}:1", "-{ZSORT:K}:2"], aggregate="SUM")
        cluster.zinterstore("-{ZSORT:K}:4", ["-{ZSORT:K}:2", "-{ZSORT:K}:1"], aggregate="MIN")

        assert cluster.zscore("-{ZSORT:K}:3", "V2") == 46
        assert cluster.zscore("-{ZSORT:K}:4", "V2") == 23

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2", "-{ZSORT:K}:3", "-{ZSORT:K}:4")

    def test_zlexcount(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert cluster.zlexcount("ZSORT:K1", "[V1", "[V3") == 3
        assert cluster.zlexcount("-ZSORT:K2", "[T2", "[T3") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmax(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert cluster.zpopmax("ZSORT:K1") == [("V2", 23)]
        assert cluster.zpopmax("-ZSORT:K2") == [("T1", 56)]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmin(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zpopmin("ZSORT:K1") == [("V3", 9)]
        assert cluster.zpopmin("-ZSORT:K2") == [("T2", 23)]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrandmember(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrandmember("ZSORT:K1", count=1, withscores=False)[0] in {"V1", "V2", "V3"}
        assert cluster.zrandmember("-ZSORT:K2", count=1, withscores=False)[0] in {"T1", "T2", "T3"}

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmax(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.bzpopmax(["ZSORT:K1"]) == ("CLU:ZSORT:K1", "V2", 23)
        assert cluster.bzpopmax(["-ZSORT:K2"]) == ("ZSORT:K2", "T1", 56)

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmin(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.bzpopmin(["ZSORT:K1"]) == ("CLU:ZSORT:K1", "V3", 9)
        assert cluster.bzpopmin(["-ZSORT:K2"]) == ("ZSORT:K2", "T2", 23)

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrange(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrange("ZSORT:K1", 0, 1, withscores=True) == [("V3", 9), ("V1", 13)]
        assert cluster.zrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T1", 56)]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrange(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrevrange("ZSORT:K1", 0, 1, withscores=True) == [("V2", 23), ("V1", 13)]
        assert cluster.zrevrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T2", 23)]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangestore(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrangestore("-{ZSORT:K}:3", "-{ZSORT:K}:1", 0, 1)
        assert cluster.zrangestore("-{ZSORT:K}:4", "-{ZSORT:K}:2", 1, 2)

        assert cluster.zscore("-{ZSORT:K}:3", "V3") == 9
        assert cluster.zscore("-{ZSORT:K}:4", "T1") == 56

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2", "-{ZSORT:K}:3", "-{ZSORT:K}:4")

    def test_zrangebylex(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert cluster.zrangebylex("ZSORT:K1", "[V1", "[V3") == ["V1", "V2", "V3"]
        assert cluster.zrangebylex("-ZSORT:K2", "[T2", "[T3") == ["T2", "T3"]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebylex(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert cluster.zrevrangebylex("ZSORT:K1", "[V3", "[V1") == ["V3", "V2", "V1"]
        assert cluster.zrevrangebylex("-ZSORT:K2", "[T3", "[T2") == ["T3", "T2"]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangebyscore(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrangebyscore("ZSORT:K1", 9, 13) == ["V3", "V1"]
        assert cluster.zrangebyscore("-ZSORT:K2", 30, 100) == ["T3", "T1"]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebyscore(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrevrangebyscore("ZSORT:K1", 13, 9) == ["V1", "V3"]
        assert cluster.zrevrangebyscore("-ZSORT:K2", 100, 30) == ["T1", "T3"]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrank(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrank("ZSORT:K1", "V1") == 1
        assert cluster.zrank("-ZSORT:K2", "T1") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrem(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrem("ZSORT:K1", "V1")
        assert cluster.zrem("-ZSORT:K2", "T1", "T2")

        assert cluster.zcard("ZSORT:K1") == 2
        assert cluster.zcard("-ZSORT:K2") == 1

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebylex(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert cluster.zremrangebylex("ZSORT:K1", "[V1", "[V2")
        assert cluster.zremrangebylex("-ZSORT:K2", "[T2", "(T3")

        assert cluster.zcard("ZSORT:K1") == 1
        assert cluster.zcard("-ZSORT:K2") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyrank(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        cluster.zremrangebyrank("ZSORT:K1", 0, 1)
        cluster.zremrangebyrank("-ZSORT:K2", 1, 2)

        assert cluster.zcard("ZSORT:K1") == 1
        assert cluster.zcard("-ZSORT:K2") == 1

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyscore(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        cluster.zremrangebyscore("ZSORT:K1", 9, 100)
        cluster.zremrangebyscore("-ZSORT:K2", 40, 100)

        assert cluster.zcard("ZSORT:K1") == 0
        assert cluster.zcard("-ZSORT:K2") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrank(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert cluster.zrevrank("ZSORT:K1", "V2") == 0
        assert cluster.zrevrank("-ZSORT:K2", "T2") == 2

        cluster.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zunion(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert cluster.zunion(["-{ZSORT:K}:1", "-{ZSORT:K}:2"], withscores=True) == [
            ("V3", 9),
            ("V1", 13),
            ("T3", 39),
            ("V2", 46),
            ("T1", 56)
        ]

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2")

    def test_zunionstore(self, cluster):
        cluster.zadd("-{ZSORT:K}:1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-{ZSORT:K}:2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        cluster.zunionstore("-{ZSORT:K}:3", ["-{ZSORT:K}:1", "-{ZSORT:K}:2"])

        assert cluster.zscore("-{ZSORT:K}:3", "V2") == 46

        cluster.delete("-{ZSORT:K}:1", "-{ZSORT:K}:2", "-{ZSORT:K}:3")

    def test_zmscore(self, cluster):
        cluster.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        cluster.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 22,
            "T3": 39,
        })

        assert cluster.zmscore("ZSORT:K1", ["V3", "V2"]) == [9, 23]
        assert cluster.zmscore("-ZSORT:K2", ["T2", "T4"]) == [22, None]

        cluster.delete("ZSORT:K1", "-ZSORT:K2")
