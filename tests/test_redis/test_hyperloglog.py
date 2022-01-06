# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/4 1:07 下午'


class TestHyperLogLog:

    def test_pfadd(self, redis):
        redis.pfadd("HLL:K1", "V1", "V2", "V1", "V2")
        redis.pfadd("-HLL:K2", "T1", "T2", "T3", "T3")

        assert redis.pfcount("HLL:K1") == 2
        assert redis.pfcount("-HLL:K2") == 3
        assert redis.pfcount("HLL:K1", "-HLL:K2") == 5

        redis.delete("HLL:K1", "-HLL:K2")

    def test_pfmerge(self, redis):
        redis.pfadd("HLL:K1", "V1", "V2", "V1", "V2")
        redis.pfadd("-HLL:K2", "T1", "T2", "T3", "T3")

        redis.pfmerge("MG:K1", "HLL:K1", "-HLL:K2")
        redis.pfmerge("-MG:K2", "HLL:K1", "-HLL:K2")

        assert redis.pfcount("MG:K1") == 5
        assert redis.pfcount("-MG:K2") == 5

        redis.delete("HLL:K1", "-HLL:K2", "MG:K1", "-MG:K2")
