# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 11:16 上午'


class TestGeo:

    def test_geoadd(self, cluster):
        cluster.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74853973308315, -73.98564817839411, "Empire State Building"
        ])

        cluster.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        pos_1 = cluster.geopos("GEO:K1", "Foley Square")[0]
        assert round(pos_1[0], 4) == round(40.71446401665754, 4)
        assert round(pos_1[1], 4) == round(-74.0029768392272, 4)

        pos_2 = cluster.geopos("-GEO:K2", "Lincoln Park")[0]
        assert round(pos_2[0], 4) == round(38.889766960052754, 4)
        assert round(pos_2[1], 4) == round(-76.98996107538841, 4)

        cluster.delete("GEO:K1", "-GEO:K2")

    def test_geodist(self, cluster):
        cluster.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74853973308315, -73.98564817839411, "Empire State Building"
        ])

        cluster.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert cluster.geodist("GEO:K1", "Foley Square", "New York Stock Exchange", unit="km") == 0.9503
        assert cluster.geodist("-GEO:K2", "The White House", "Lincoln Park", unit="km") == 5.2757

        cluster.delete("GEO:K1", "-GEO:K2")

    def test_georadius(self, cluster):
        cluster.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        cluster.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert cluster.georadius("GEO:K1", 40.74137935029946, -73.98978351053742, 1, "km") == ["Empire State Building"]
        assert cluster.georadius("-GEO:K2", 38.89304891594253, -77.0305464513952, 1, "km") == ["The White House"]

        cluster.delete("GEO:K1", "-GEO:K2")

    def test_georediusbymember(self, cluster):
        cluster.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        cluster.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert cluster.georadiusbymember("GEO:K1", "New York Stock Exchange", 1, "km") == \
               ["New York Stock Exchange", "Foley Square"]
        assert cluster.georadiusbymember("-GEO:K2", "Cathedral of St. Matthew the Apostle", 1, "km") == \
               ["The White House", "Cathedral of St. Matthew the Apostle"]

        cluster.delete("GEO:K1", "-GEO:K2")

    def test_geosearch(self, cluster):
        cluster.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        cluster.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert cluster.geosearch("GEO:K1", "New York Stock Exchange", unit="km", radius=1) == \
               ["New York Stock Exchange", "Foley Square"]
        assert cluster.geosearch("-GEO:K2", "Cathedral of St. Matthew the Apostle", unit="km", radius=1) == \
               ["The White House", "Cathedral of St. Matthew the Apostle"]

        cluster.delete("GEO:K1", "-GEO:K2")

    def test_geosearchstore(self, cluster):
        cluster.geoadd("-{GEO:K}:1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        cluster.geoadd("-{GEO:K}:2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        cluster.geosearchstore("-{GEO:K}:3", "-{GEO:K}:1", "New York Stock Exchange", unit="km", radius=1)
        cluster.geosearchstore("-{GEO:K}:4", "-{GEO:K}:2", "Cathedral of St. Matthew the Apostle", unit="km", radius=1)

        pos_1 = cluster.geopos("-{GEO:K}:3", "Foley Square")[0]
        assert round(pos_1[0], 4) == round(40.71446401665754, 4)
        assert round(pos_1[1], 4) == round(-74.0029768392272, 4)

        pos_2 = cluster.geopos("-{GEO:K}:4", "The White House")[0]
        assert round(pos_2[0], 4) == round(38.89757975111952, 4)
        assert round(pos_2[1], 4) == round(-77.03735946691666, 4)

        cluster.delete("-{GEO:K}:1", "-{GEO:K}:2", "-{GEO:K}:3", "-{GEO:K}:4")
