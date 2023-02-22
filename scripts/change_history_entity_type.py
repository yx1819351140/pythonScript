# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> change_history_entity_type         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/9 15:36
# @Software : win10 python3.6
"""
修改历史实体表中实体类型
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from settings import es_host, es_port
from utils.ESUtils import ElasticSearchUtils

print(es_host, es_port)
es = ElasticSearchUtils(es_host, es_port)


def get_entity_data(size, page):
    query = {
      "_source": ["id", "related_entities"],
      "size": size,
      "from": page * size,
      "query": {
        "nested": {
          "path": "related_entities",
          "query": {
            "bool": {
              "must": {
                "exists": {
                  "field": "related_entities"
                }
              }
            }
          }
        }
      }
    }
    data_list = es.search_data_by_query('hot_news_test_03', query)
    return data_list


def change_entity_type():
    size = 1000
    page = 0
    while page <= 417:
        try:
            dict_data = get_entity_data(size, page)
            data_list = dict_data['hits']['hits']
            if not data_list:
                break
            for data in data_list:
                doc_id = data['_source']['id']
                related_entities = data['_source']['related_entities']
                for related_entity in related_entities:
                    try:
                        for entity_mention in related_entity['entity_mentions']:
                            entity_mention['entity_type'] = get_entity_type(related_entity['entity_type'], entity_mention['nid'])
                        related_entity['entity_type'] = get_entity_type(related_entity['entity_type'], related_entity['entity_id'])
                    except:
                        continue
                doc = {'related_entities': related_entities}
                es.update_data_by_id('hot_news_test_03', doc_id, doc)
            with open('num.txt', 'w') as f:
                f.write(str(page))
        except:
            break
        page += 1


def get_entity_type(entity_old_type, entity_id):
    if entity_id.startswith('Q') or entity_id.startswith('NILDB'):
        if entity_old_type == 'PER':
            return 'People'
        elif entity_old_type == 'ORG':
            return 'Org'
        elif entity_old_type == 'WEA':
            return 'Weapon'
        else:
            try:
                return get_wiki_entity_type(entity_id)
            except:
                return 'Other'
    return 'Other'


def get_wiki_entity_type(entity_id):
    url = 'http://192.168.12.210:30023/Global-Event-Data/bigdata/etl/entity/type'
    data = {"qid": entity_id}
    res = requests.post(url, json=data)
    return res.text


if __name__ == '__main__':
    change_entity_type()
