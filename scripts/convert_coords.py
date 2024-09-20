# -*- coding:UTF-8 -*-
# @Time    : 2024/9/3 16:11
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : convert_coords.py
# @Project : pythonScript
# @Software: PyCharm
"""
百度坐标系（BD-09）转换为WGS-84坐标系
"""
import math

# 常量定义
PI = 3.14159265358979324
X_PI = PI * 3000.0 / 180.0


def bd09_to_gcj02(bd_lat, bd_lon):
    """百度坐标系 (BD-09) 转 火星坐标系 (GCJ-02)"""
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * X_PI)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * X_PI)
    gg_lon = z * math.cos(theta)
    gg_lat = z * math.sin(theta)
    return gg_lat, gg_lon


def gcj02_to_wgs84(lat, lon):
    """火星坐标系 (GCJ-02) 转 WGS84"""
    dlat = transform_lat(lon - 105.0, lat - 35.0)
    dlon = transform_lon(lon - 105.0, lat - 35.0)
    radlat = lat / 180.0 * PI
    magic = math.sin(radlat)
    magic = 1 - 0.00669342162296594323 * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((6378245.0 * (1 - 0.00669342162296594323)) / (magic * sqrtmagic) * PI)
    dlon = (dlon * 180.0) / (6378245.0 / sqrtmagic * math.cos(radlat) * PI)
    mg_lat = lat + dlat
    mg_lon = lon + dlon
    return lat * 2 - mg_lat, lon * 2 - mg_lon


def transform_lat(x, y):
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * PI) + 320 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
    return ret


def transform_lon(x, y):
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
    return ret


def bd09_to_wgs84(bd_lat, bd_lon):
    lat, lon = bd09_to_gcj02(bd_lat, bd_lon)
    return gcj02_to_wgs84(lat, lon)


if __name__ == '__main__':
    # 示例使用
    bd_lat = 35.9581940
    bd_lon = 120.1908410
    wgs84_lat, wgs84_lon = bd09_to_wgs84(bd_lat, bd_lon)
    print("WGS84坐标系: 纬度={}, 经度={}".format(wgs84_lat, wgs84_lon))
