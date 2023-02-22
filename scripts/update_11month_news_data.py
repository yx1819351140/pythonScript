# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_11month_news_data         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/13 17:44
# @Software : win10 python3.6
"""
投标项目，将11月的新闻数据从测试环境192.168.12.197的新闻表hot_news_test_03导入到192.168.12.194的新闻表hot_news_test_03
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.ESUtils import ElasticSearchUtils
from utils.DateUtils import get_date_before_day, get_current_timestamp, strftime_to_timestamp


def set_data(host='127.0.0.1'):

    start_time = 1667232000000  # 2022-11-01 00:00:00
    end_time = 1669737600000  # 2022-11-30 00:00:00

    es = ElasticSearchUtils('192.168.12.197', 9200)

    es1 = ElasticSearchUtils(host)

    num = 0

    for i in range(0, 44):
        query = {
            "size": 10000,
            "from": i * 10000,
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

        news_list = es.search_data_by_query('hot_news_test_03', query)['hits']['hits']

        # for news in news_list:
        #     news['_index'] = 'hot_news_test_05'

        # print(news_list)

        es1.save_data_by_bulk('hot_news_test_03', news_list)

        num += len(news_list)

        print(f'已写入{num}条数据')


if __name__ == '__main__':
    set_data('192.168.12.194')
