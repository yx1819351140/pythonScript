# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> DateUtils         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/12 11:27
# @Software : win10 python3.6
import time
import datetime


def get_current_time():
    """
    获取当前时间
    :return: %Y-%m-%d %H:%M:%S
    """
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def get_current_timestamp():
    """
    获取当前时间戳
    :return: int
    """
    return int(time.time() * 1000)


def get_date_before_day(before_day):
    """
    获取before_day之前的时间
    :param before_day: int
    :return: %Y-%m-%d
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-before_day)
    re_date = (today + offset).strftime('%Y-%m-%d')
    return re_date


def get_date_after_day(after_day):
    """
    获取after_day之后的时间
    :param after_day: int
    :return: %Y-%m-%d
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=+after_day)
    re_date = (today + offset).strftime('%Y-%m-%d')
    return re_date


def get_time_before(hours=0, minutes=0, seconds=0):
    """
    获取几小时、几分钟、几秒之前的时间
    :param hours: int
    :param minutes: int
    :param seconds: int
    :return: %Y-%m-%d %H:%M:%S
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(hours=-hours, minutes=-minutes, seconds=-seconds)
    re_date = (today + offset).strftime('%Y-%m-%d %H:%M:%S')
    return re_date


def get_time_after(hours=0, minutes=0, seconds=0):
    """
    获取几小时、几分钟、几秒之后的时间
    :param hours: int
    :param minutes: int
    :param seconds: int
    :return: %Y-%m-%d %H:%M:%S
    """
    today = datetime.datetime.now()
    offset = datetime.timedelta(hours=+hours, minutes=+minutes, seconds=+seconds)
    re_date = (today + offset).strftime('%Y-%m-%d %H:%M:%S')
    return re_date


def strftime_to_timestamp(strftime):
    """
    时间格式转时间戳
    :param strftime: Y-%m-%d %H:%M:%S
    :return: int
    """
    s_t = time.strptime(strftime, "%Y-%m-%d %H:%M:%S")
    mkt = int(time.mktime(s_t) * 1000)
    return mkt


def timestamp_to_strftime(timestamp):
    """
    时间戳转时间格式
    :param timestamp: int
    :return: Y-%m-%d %H:%M:%S
    """
    timeArray = time.localtime(int(timestamp/1000))
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime


if __name__ == '__main__':
    beforeday = 30
    print(timestamp_to_strftime(1670816667018))

