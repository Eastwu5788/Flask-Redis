# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2021/12/30 5:13 下午'


class TestSortedSet:

    def test_zadd(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39
        })

        assert redis.zscore("ZSORT:K1", "V2") == 23
        assert redis.zscore("-ZSORT:K2", "T3") == 39

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcard(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T3": 39
        })

        assert redis.zcard("ZSORT:K1") == 3
        assert redis.zcard("-ZSORT:K2") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zcount(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 93,
            "T3": 39,
        })

        assert redis.zcount("ZSORT:K1", 1, 20) == 2
        assert redis.zcount("-ZSORT:K2", 1, 100) == 3

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zdiff(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert redis.zdiff(["ZSORT:K1", "-ZSORT:K2"], withscores=False) == ["V3", "V1"]
        assert redis.zdiff(["-ZSORT:K2", "ZSORT:K1"], withscores=True) == ["T3", '39', "T1", '56']

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zdiffstore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        redis.zdiffstore("ZDIFF:ST:K1", ["ZSORT:K1", "-ZSORT:K2"])
        redis.zdiffstore("-ZDIFF:ST:K2", ["-ZSORT:K2", "ZSORT:K1"])

        assert redis.zscore("ZDIFF:ST:K1", "V1") == 13
        assert redis.zscore("ZDIFF:ST:K1", "V2") is None

        assert redis.zscore("-ZDIFF:ST:K2", "T1") == 56
        assert redis.zscore("-ZDIFF:ST:K2", "V2") is None

        redis.delete("ZSORT:K1", "-ZSORT:K2", "ZDIFF:ST:K1", "-ZDIFF:ST:K2")

    def test_zincrby(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        redis.zincrby("ZSORT:K1", 10, "V2")
        redis.zincrby("-ZSORT:K2", 9, "T2")

        assert redis.zscore("ZSORT:K1", "V2") == 33
        assert redis.zscore("-ZSORT:K2", "T2") == 32

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zinter(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert redis.zinter(["ZSORT:K1", "-ZSORT:K2"], aggregate="SUM", withscores=True) == [("V2", 46)]
        assert redis.zinter(["-ZSORT:K2", "ZSORT:K1"], aggregate="MIN", withscores=True) == [("V2", 23)]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zinterstore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        redis.zinterstore("ZIN:K1", ["ZSORT:K1", "-ZSORT:K2"], aggregate="SUM")
        redis.zinterstore("-ZIN:K2", ["-ZSORT:K2", "ZSORT:K1"], aggregate="MIN")

        assert redis.zscore("ZIN:K1", "V2") == 46
        assert redis.zscore("-ZIN:K2", "V2") == 23

        redis.delete("ZSORT:K1", "-ZSORT:K2", "ZIN:K1", "-ZIN:K2")

    def test_zlexcount(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert redis.zlexcount("ZSORT:K1", "[V1", "[V3") == 3
        assert redis.zlexcount("-ZSORT:K2", "[T2", "[T3") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmax(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert redis.zpopmax("ZSORT:K1") == [("V2", 23)]
        assert redis.zpopmax("-ZSORT:K2") == [("T1", 56)]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zpopmin(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zpopmin("ZSORT:K1") == [("V3", 9)]
        assert redis.zpopmin("-ZSORT:K2") == [("T2", 23)]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrandmember(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrandmember("ZSORT:K1", count=1, withscores=False)[0] in {"V1", "V2", "V3"}
        assert redis.zrandmember("-ZSORT:K2", count=1, withscores=False)[0] in {"T1", "T2", "T3"}

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmax(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.bzpopmax(["ZSORT:K1"]) == ("EG:ZSORT:K1", "V2", 23)
        assert redis.bzpopmax(["-ZSORT:K2"]) == ("ZSORT:K2", "T1", 56)

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_bzpopmin(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.bzpopmin(["ZSORT:K1"]) == ("EG:ZSORT:K1", "V3", 9)
        assert redis.bzpopmin(["-ZSORT:K2"]) == ("ZSORT:K2", "T2", 23)

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrange(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrange("ZSORT:K1", 0, 1, withscores=True) == [("V3", 9), ("V1", 13)]
        assert redis.zrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T1", 56)]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrange(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrevrange("ZSORT:K1", 0, 1, withscores=True) == [("V2", 23), ("V1", 13)]
        assert redis.zrevrange("-ZSORT:K2", 1, 2, withscores=True) == [("T3", 39), ("T2", 23)]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangestore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrangestore("ZRANGE:K1", "ZSORT:K1", 0, 1)
        assert redis.zrangestore("-ZRANGE:K2", "-ZSORT:K2", 1, 2)

        assert redis.zscore("ZRANGE:K1", "V3") == 9
        assert redis.zscore("-ZRANGE:K2", "T1") == 56

        redis.delete("ZSORT:K1", "-ZSORT:K2", "ZRANGE:K1", "-ZRANGE:K2")

    def test_zrangebylex(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert redis.zrangebylex("ZSORT:K1", "[V1", "[V3") == ["V1", "V2", "V3"]
        assert redis.zrangebylex("-ZSORT:K2", "[T2", "[T3") == ["T2", "T3"]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebylex(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert redis.zrevrangebylex("ZSORT:K1", "[V3", "[V1") == ["V3", "V2", "V1"]
        assert redis.zrevrangebylex("-ZSORT:K2", "[T3", "[T2") == ["T3", "T2"]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrangebyscore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrangebyscore("ZSORT:K1", 9, 13) == ["V3", "V1"]
        assert redis.zrangebyscore("-ZSORT:K2", 30, 100) == ["T3", "T1"]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrangebyscore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrevrangebyscore("ZSORT:K1", 13, 9) == ["V1", "V3"]
        assert redis.zrevrangebyscore("-ZSORT:K2", 100, 30) == ["T1", "T3"]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrank(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrank("ZSORT:K1", "V1") == 1
        assert redis.zrank("-ZSORT:K2", "T1") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrem(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrem("ZSORT:K1", "V1")
        assert redis.zrem("-ZSORT:K2", "T1", "T2")

        assert redis.zcard("ZSORT:K1") == 2
        assert redis.zcard("-ZSORT:K2") == 1

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebylex(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": "9",
            "V2": "9",
            "V3": "9",
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": "6",
            "T2": "6",
            "T3": "6",
        })

        assert redis.zremrangebylex("ZSORT:K1", "[V1", "[V2")
        assert redis.zremrangebylex("-ZSORT:K2", "[T2", "(T3")

        assert redis.zcard("ZSORT:K1") == 1
        assert redis.zcard("-ZSORT:K2") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyrank(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        redis.zremrangebyrank("ZSORT:K1", 0, 1)
        redis.zremrangebyrank("-ZSORT:K2", 1, 2)

        assert redis.zcard("ZSORT:K1") == 1
        assert redis.zcard("-ZSORT:K2") == 1

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zremrangebyscore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        redis.zremrangebyscore("ZSORT:K1", 9, 100)
        redis.zremrangebyscore("-ZSORT:K2", 40, 100)

        assert redis.zcard("ZSORT:K1") == 0
        assert redis.zcard("-ZSORT:K2") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zrevrank(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 23,
            "T3": 39,
        })

        assert redis.zrevrank("ZSORT:K1", "V2") == 0
        assert redis.zrevrank("-ZSORT:K2", "T2") == 2

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zunion(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        assert redis.zunion(["ZSORT:K1", "-ZSORT:K2"], withscores=True) == [
            ("V3", 9),
            ("V1", 13),
            ("T3", 39),
            ("V2", 46),
            ("T1", 56)
        ]

        redis.delete("ZSORT:K1", "-ZSORT:K2")

    def test_zunionstore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "V2": 23,
            "T3": 39,
        })

        redis.zunionstore("ZUNION:K1", ["ZSORT:K1", "-ZSORT:K2"])
        redis.zunionstore("-ZUNION:K1", ["ZSORT:K1", "-ZSORT:K2"])

        assert redis.zscore("ZUNION:K1", "V2") == 46
        assert redis.zscore("-ZUNION:K1", "V2") == 46

        redis.delete("ZSORT:K1", "-ZSORT:K2", "ZUNION:K1", "-ZUNION:K1")

    def test_zmscore(self, redis):
        redis.zadd("ZSORT:K1", mapping={
            "V1": 13,
            "V2": 23,
            "V3": 9,
        })
        redis.zadd("-ZSORT:K2", mapping={
            "T1": 56,
            "T2": 22,
            "T3": 39,
        })

        assert redis.zmscore("ZSORT:K1", ["V3", "V2"]) == [9, 23]
        assert redis.zmscore("-ZSORT:K2", ["T2", "T4"]) == [22, None]

        redis.delete("ZSORT:K1", "-ZSORT:K2")
