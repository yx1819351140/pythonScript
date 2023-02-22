# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_news_data_4region         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/14 14:31
# @Software : win10 python3.6
"""
投标项目，修改区域标签数据，将最精确的几条事件数据导入到192.169.12.194的事件表hot_co_events中
以及将该区域下几个国家的新闻更新到11月底，修改hot_news_test_03中与这几个国家有关的11月底新闻，添加custom_labels中标签值
"""
import json
import os

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import bulk


def get_data(index='', data_id=''):
    url = {'host': '192.168.12.197', 'port': 9200}
    es = Elasticsearch([url])
    if es.ping():
        print('Successfully connect!')
    else:
        print('Failed.....')
        exit()
    if data_id:
        query = {
            "query": {
                "match": {
                    '_id': data_id
                }
            }
        }
        res = helpers.scan(es, index=index, scroll='20m', query=query)
        # print('get data success!')
        return list(res)


def get_data_by_query(index='', query={}):
    url = {'host': '192.168.12.197', 'port': 9200}
    es = Elasticsearch([url])
    if es.ping():
        print('Successfully connect!')
    else:
        print('Failed.....')
        exit()
    if query:
        res = helpers.scan(es, index=index, scroll='20m', query=query)
        # print('get data success!')
        return list(res)


def get_data1(index='', data_id=''):
    url = {'host': '192.168.12.194', 'port': 9200}
    es = Elasticsearch([url])
    if data_id:
        query = {
            "_source": ["nlp_related_place", "custom_labels"],
            "query": {
                "match": {
                    '_id': data_id
                }
            }
        }
        res = helpers.scan(es, index=index, scroll='20m', query=query)
        return dict(list(res)[0])


def set_data(index='', actions=None, host='192.168.12.194'):
    if actions is None:
        actions = []
    url = {'host': host, 'port': 9200}
    es = Elasticsearch([url])
    res, _ = bulk(es, actions, index=index, raise_on_error=True)
    print(res)


def update_taiwan_data(index='', data_id='', data={}):
    """
    添加台湾11月底新闻
    :param index:
    :param data_id:
    :param data:
    :return:
    """
    url = {'host': '192.168.12.194', 'port': 9200}
    es = Elasticsearch([url])

    try:
        custom_labels = list(data['_source']['custom_labels'])
    except Exception as e:
        print(e)
        custom_labels = []

    try:
        nlp_related_place = list(data['_source']['nlp_related_place'])
    except Exception as e:
        print(e)
        nlp_related_place = []

    custom_labels.append(439)
    nlp_related_place.append({
        "place_lat": "25.051",
        "country": "TW",
        "place_lon": "121.569939,",
        "place": "taiwan",
        "place_id": "Q57251"
    })
    body = {"doc": {"nlp_related_place": nlp_related_place, "custom_labels": custom_labels}}
    print(body)
    if data_id and custom_labels:
        res = es.update(index=index, doc_type="_doc", id=data_id, body=body)
        print(res)


def update_data(index='', data_id='', data={}):
    """
    添加俄、美、乌11月底新闻
    :param index:
    :param data_id:
    :param data:
    :return:
    """
    url = {'host': '192.168.12.194', 'port': 9200}
    es = Elasticsearch([url])

    try:
        custom_labels = list(data['_source']['custom_labels'])
    except Exception as e:
        # print(e)
        custom_labels = []

    custom_labels.append(377)  # 俄罗斯：381  美国：377
    body = {"doc": {"custom_labels": custom_labels}}
    print(body)
    if data_id and custom_labels:
        res = es.update(index=index, doc_type="_doc", id=data_id, body=body)
        print(res)


def run(index='', data_id=''):
    actions = get_data(index, data_id)
    set_data(index, actions)


def insert_events():
    """
    添加相关事件
    :return:
    """
    taiwan = {
        "nid": "Q57251",
        "entity_type": "Country_Instance",
        "role": "Place"
    }

    path = "./data/区域"

    actions = []

    for data_path in os.listdir(path):
        for file_name in os.listdir(f"{path}/{data_path}"):
            result_path = f"{path}/{data_path}/{file_name}"

            with open(result_path, 'rb') as f:
                data = json.loads(f.read().decode('utf-8'))

            # data["event_level"] = random.randint(7, 9)

            actions.append({
                "_index": "hot_co_events",
                "_type": "_doc",
                "_id": data['event_id'],
                "_source": data
            })

    # print(len(actions))

    set_data("hot_co_events", actions)


def update_co_events():
    query = {
        "size": 15000,
        "query": {
            "range": {
                "event_time": {
                    "gte": 1668873600000,
                    "lte": 1669823999000
                }
            }
        }
    }

    index = "hot_co_events"

    actions = get_data_by_query(index, query)

    for action in actions:
        action["_source"]["custom_labels"] = []
    set_data("hot_co_events", actions)


def export_news_data():
    query = {
        "size": 15000,
        "query": {
            "match_all": {}
        }
    }

    index = "hot_co_events"

    actions = get_data_by_query(index, query)

    set_data(index, actions, "127.0.0.1")


if __name__ == '__main__':
    # run('hot_news_test_03', '2265cc2af9022cae7475c94831341e8d')
    # run1()
    # change_entity_sort()
    # update_data('hot_entity_test_03', 'Q2787872', get_data('hot_entity_test_03', 'Q2787872')[0])

    # index = 'hot_news_test_03'
    # data_id = 'a45d2eec16d50e909d824afc990eb4bb'
    # data = get_data1(index, data_id)
    # update_data(index, data_id, data)

    # insert_events()

    # update_co_events()

    export_news_data()

