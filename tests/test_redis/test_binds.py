# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/4 3:17 下午'


class TestBinds:

    def test_multi_binds(self, mredis):
        mredis.set("MDB1:K1", "V1")

        mredis["DB1"].set("MDB2:K2", "V2")
        mredis.DB1.set("MDB2:K3", "V3")

        assert mredis.get("MDB1:K1") == "V1"
        assert mredis["DB1"].get("MDB2:K2") == "V2"
        assert mredis.DB1.get("MDB2:K3") == "V3"

        mredis.delete("MDB1:K1")
        mredis.DB1.delete("MDB2:K2", "MDB2:K3")

    def test_simple_config(self, mredis):
        mredis["DB2"].set("MDB3:K1", "V1")

        assert mredis.DB2.get("MDB3:K1") == "V1"

        mredis.DB2.delete("MDB3:K1")
