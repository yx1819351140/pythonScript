# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> compare_repeat_news         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/4/20 10:59
# @Software : win10 python3.6
"""
对照获取es title_id差集
"""
from utils.ESUtils import ElasticSearchUtils


def get_news_id(index):
    es = ElasticSearchUtils("192.168.12.197,192.168.12.198,192.168.12.199")
    query = {
        "_source": ["title_id"],
        "query": {
            "term": {
                "keywords": "BBC"
            }
        }
    }
    data_list = es.search_data_by_query(index, query)['hits']['hits']
    result_list = [i['_source']['title_id'] for i in data_list]
    return result_list


def get_repeat_news():
    title_id_list1 = get_news_id('origin_news')
    title_id_list2 = get_news_id('yuchen_news')
    return list(set(title_id_list1).difference(set(title_id_list2)))


if __name__ == "__main__":
    result_id = get_repeat_news()
    print(result_id)

