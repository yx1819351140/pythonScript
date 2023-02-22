# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> export_10days_news_data         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/12 11:23
# @Software : win10 python3.6
"""
投标项目，将before_days天前的新闻数据从测试环境192.168.12.220的新闻表hot_news导入到192.168.12.194的新闻表hot_news_test_03
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ESUtils import ElasticSearchUtils
from utils.DateUtils import get_date_before_day, get_current_timestamp, strftime_to_timestamp

before_days = 5


def set_data(host='127.0.0.1'):

    start_time = strftime_to_timestamp(get_date_before_day(before_days) + ' 00:00:00')
    end_time = get_current_timestamp()

    es = ElasticSearchUtils('192.168.12.220', 9200)

    es1 = ElasticSearchUtils(host)

    num = 0

    for i in range(0, 43):
        query = {
            "size": 1000,
            "from": i * 1000,
            "query": {
                "range": {
                    "pub_time_timestamp": {
                        "gte": start_time,
                        "lte": end_time
                    }
                }
            }
        }

        # print(es.search_data_by_query('hot_news_test_03', query))

        news_list = es.search_data_by_query('hot_news', query)['hits']['hits']

        for news in news_list:
            news['_index'] = 'hot_news_test_03'

        # print(news_list)

        es1.save_data_by_bulk('hot_news_test_03', news_list)

        num += len(news_list)

        print(f'已写入{num}条数据')


if __name__ == '__main__':
    set_data('192.168.12.194')
