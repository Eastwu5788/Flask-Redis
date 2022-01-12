# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 11:14 上午'


class TestHyperLogLog:

    def test_pfadd(self, cluster):
        cluster.pfadd("-{HLL:K}:1", "V1", "V2", "V1", "V2")
        cluster.pfadd("-{HLL:K}:2", "T1", "T2", "T3", "T3")

        assert cluster.pfcount("-{HLL:K}:1") == 2
        assert cluster.pfcount("-{HLL:K}:2") == 3
        assert cluster.pfcount("-{HLL:K}:1", "-{HLL:K}:2") == 5

        cluster.delete("-{HLL:K}:1", "-{HLL:K}:2")

    def test_pfmerge(self, cluster):
        cluster.pfadd("-{HLL:K}:1", "V1", "V2", "V1", "V2")
        cluster.pfadd("-{HLL:K}:2", "T1", "T2", "T3", "T3")

        cluster.pfmerge("-{HLL:K}:3", "-{HLL:K}:1", "-{HLL:K}:2")
        cluster.pfmerge("-{HLL:K}:4", "-{HLL:K}:1", "-{HLL:K}:2")

        assert cluster.pfcount("-{HLL:K}:3") == 5
        assert cluster.pfcount("-{HLL:K}:4") == 5

        cluster.delete("-{HLL:K}:1", "-{HLL:K}:2", "-{HLL:K}:3", "-{HLL:K}:4")
