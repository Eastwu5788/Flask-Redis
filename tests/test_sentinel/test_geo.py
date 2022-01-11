# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2021
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2022/1/10 1:52 下午'


class TestGeo:

    def test_geoadd(self, master, slave):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74853973308315, -73.98564817839411, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        pos_1 = slave.geopos("GEO:K1", "Foley Square")[0]
        assert round(pos_1[0], 4) == round(40.71446401665754, 4)
        assert round(pos_1[1], 4) == round(-74.0029768392272, 4)

        pos_2 = slave.geopos("-GEO:K2", "Lincoln Park")[0]
        assert round(pos_2[0], 4) == round(38.889766960052754, 4)
        assert round(pos_2[1], 4) == round(-76.98996107538841, 4)

        master.delete("GEO:K1", "-GEO:K2")

    def test_geodist(self, master, slave):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74853973308315, -73.98564817839411, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert slave.geodist("GEO:K1", "Foley Square", "New York Stock Exchange", unit="km") == 0.9503
        assert slave.geodist("-GEO:K2", "The White House", "Lincoln Park", unit="km") == 5.2757

        master.delete("GEO:K1", "-GEO:K2")

    def test_georadius(self, master):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert master.georadius("GEO:K1", 40.74137935029946, -73.98978351053742, 1, "km") == ["Empire State Building"]
        assert master.georadius("-GEO:K2", 38.89304891594253, -77.0305464513952, 1, "km") == ["The White House"]

        master.delete("GEO:K1", "-GEO:K2")

    def test_georediusbymember(self, master):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert master.georadiusbymember("GEO:K1", "New York Stock Exchange", 1, "km") == \
               ["New York Stock Exchange", "Foley Square"]
        assert master.georadiusbymember("-GEO:K2", "Cathedral of St. Matthew the Apostle", 1, "km") == \
               ["The White House", "Cathedral of St. Matthew the Apostle"]

        master.delete("GEO:K1", "-GEO:K2")

    def test_geosearch(self, master):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        assert master.geosearch("GEO:K1", "New York Stock Exchange", unit="km", radius=1) == \
               ["New York Stock Exchange", "Foley Square"]
        assert master.geosearch("-GEO:K2", "Cathedral of St. Matthew the Apostle", unit="km", radius=1) == \
               ["The White House", "Cathedral of St. Matthew the Apostle"]

        master.delete("GEO:K1", "-GEO:K2")

    def test_geosearchstore(self, master, slave):
        master.geoadd("GEO:K1", [
            40.70680040974271, -74.01125485098078, "New York Stock Exchange",
            40.71446401665754, -74.0029768392272, "Foley Square",
            40.74848727422424, -73.98585073225857, "Empire State Building"
        ])

        master.geoadd("-GEO:K2", [
            38.89757975111952, -77.03735946691666, "The White House",
            38.90659559936292, -77.04070227181234, "Cathedral of St. Matthew the Apostle",
            38.889766960052754, -76.98996107538841, "Lincoln Park"
        ])

        master.geosearchstore("GSE:K1", "GEO:K1", "New York Stock Exchange", unit="km", radius=1)
        master.geosearchstore("-GSE:K2", "-GEO:K2", "Cathedral of St. Matthew the Apostle", unit="km", radius=1)

        pos_1 = slave.geopos("GSE:K1", "Foley Square")[0]
        assert round(pos_1[0], 4) == round(40.71446401665754, 4)
        assert round(pos_1[1], 4) == round(-74.0029768392272, 4)

        pos_2 = slave.geopos("-GSE:K2", "The White House")[0]
        assert round(pos_2[0], 4) == round(38.89757975111952, 4)
        assert round(pos_2[1], 4) == round(-77.03735946691666, 4)

        master.delete("GEO:K1", "-GEO:K2", "GSE:K1", "-GSE:K2")
