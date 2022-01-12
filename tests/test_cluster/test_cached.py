# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 11:19 上午'


class TestCached:

    v1 = "355"

    def test_cached(self, cluster):
        self.v1 = "355"

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY1", 6, **{"k3": "V3"}) == "355"
        self.v1 = "799"
        assert cache_func(self, "KEY1", 6, **{"k3": "V3"}) == '355'

    def test_ignore_cache(self, cluster):
        self.v1 = "355"

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY2", 6, **{"k3": "V3"}) == "355"
        self.v1 = "799"
        assert cache_func(self, "KEY2", 6, **{"k3": "V3", "cache": False}) == '799'

    def test_cached_serialize(self, cluster):
        self.v1 = {
            "user": "Tok",
            "age": 14,
        }

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY3", 6, **{"k3": "V3"}) == {
            "user": "Tok",
            "age": 14,
        }
        self.v1 = "799"
        assert cache_func(self, "KEY3", 6, **{"k3": "V3"}) == {
            "user": "Tok",
            "age": 14,
        }

        assert cache_func(self, "KEY3", 6, **{"k3": "V3"}, cache=False) == "799"

    def test_cached_list(self, cluster):
        self.v1 = [
            {
                "user": "Tok",
                "age": 10
            }
        ]

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY4", 6, **{"k3": "V3"}) == [
            {
                "user": "Tok",
                "age": 10
            }
        ]
        self.v1 = "799"
        assert cache_func(self, "KEY4", 6, **{"k3": "V3"}) == [
            {
                "user": "Tok",
                "age": 10
            }
        ]

    def test_cached_tuple(self, cluster):
        self.v1 = (
            {
                "user": "Tok",
                "age": 10
            },
            {
                "t1": "V1",
                "t2": "V2"
            }
        )

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY5", 6, **{"k3": "V3"}) == (
            {
                "user": "Tok",
                "age": 10
            },
            {
                "t1": "V1",
                "t2": "V2"
            }
        )
        self.v1 = "799"
        assert cache_func(self, "KEY5", 6, **{"k3": "V3"}) == [
            {
                "user": "Tok",
                "age": 10
            },
            {
                "t1": "V1",
                "t2": "V2"
            }
        ]

    def test_invalid_serialized(self, cluster):
        self.v1 = "{'T1', 'T2'}"

        @cluster.cached(key="{k1}:{k2}:{k3}", timeout=10)
        def cache_func(sf, k1, k2=1, **kwargs):
            return sf.v1

        assert cache_func(self, "KEY6", 6, **{"k3": "V3"}) == "{'T1', 'T2'}"
        self.v1 = "799"
        assert cache_func(self, "KEY6", 6, **{"k3": "V3"}) == "{'T1', 'T2'}"
