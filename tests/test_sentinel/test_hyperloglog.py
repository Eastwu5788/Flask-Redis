# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:51 下午'


class TestHyperLogLog:

    def test_pfadd(self, master, slave):
        master.pfadd("HLL:K1", "V1", "V2", "V1", "V2")
        master.pfadd("-HLL:K2", "T1", "T2", "T3", "T3")

        assert slave.pfcount("HLL:K1") == 2
        assert slave.pfcount("-HLL:K2") == 3
        assert slave.pfcount("HLL:K1", "-HLL:K2") == 5

        master.delete("HLL:K1", "-HLL:K2")

    def test_pfmerge(self, master, slave):
        master.pfadd("HLL:K1", "V1", "V2", "V1", "V2")
        master.pfadd("-HLL:K2", "T1", "T2", "T3", "T3")

        master.pfmerge("MG:K1", "HLL:K1", "-HLL:K2")
        master.pfmerge("-MG:K2", "HLL:K1", "-HLL:K2")

        assert slave.pfcount("MG:K1") == 5
        assert slave.pfcount("-MG:K2") == 5

        master.delete("HLL:K1", "-HLL:K2", "MG:K1", "-MG:K2")
