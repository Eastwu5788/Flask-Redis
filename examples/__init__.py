# !/usr/local/python/bin/python
# -*- coding: utf-8 -*-
# (C) Wu Dong, 2020
# All rights reserved
# @Author: 'Wu Dong <wudong@eastwu.cn>'
# @Time: '2020-06-24 16:02'
import inspect


def test_func(user, test=5, *args, **kwargs):
    """ 特殊函数
    """
    print(user)
    print(test)
    print(args)
    print(kwargs)


if __name__ == "__main__":
    info = inspect.getfullargspec(test_func)
    print(info.args)

    kw = {
        "new": 4,
    }
    args = [1, 3, 1, 2, 3] + ["use", "test"]
    test_func(*args, **kw)
