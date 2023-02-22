# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> utils         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/9 15:02
# @Software : win10 python3.6
import time

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import bulk


class ElasticSearchUtils(object):

    def __init__(self, host='', port=9200):
        self.es = Elasticsearch([{'host': i, 'port': port} for i in host.split(',')])
        if self.es.ping():
            print('Successfully connect!')
        else:
            print('Connect failed.....')
            exit()

    def search_data_by_id(self, index='', doc_id=''):
        if doc_id and index:
            query = {
                "query": {
                    "match": {
                        '_id': doc_id
                    }
                }
            }
            res = self.search_data_by_query(index, query)
            try:
                return res
            except:
                return "{}"
        else:
            print('index、doc_id不能为空！')
            return

    def update_data_by_id(self, index='', doc_id='', doc: dict={}, doc_type='_doc'):
        if doc_id and index and doc:
            body = {"doc": doc}
            try:
                res = self.es.update(index=index, doc_type=doc_type, id=doc_id, body=body)
                # print(res)
            except Exception as e:
                print(f'Update data failed, error message: {e}')
        else:
            print('index、doc_id、doc不能为空！')
            return

    def search_data_by_query(self, index='', query: dict={}):
        if query and index:
            res = self.es.search(index=index, body=query, request_timeout=60*60)
            try:
                return res
            except Exception as e:
                print(e)
                return []
        else:
            print('index、query不能为空！')
            return

    def save_data_by_bulk(self, index='', actions: list=[]):
        if actions:
            try:
                bulk(self.es, actions, index=index, raise_on_error=True, request_timeout=60*60)
            except Exception as e:
                print(e)
        else:
            print('新增数据为空！')


if __name__ == '__main__':
    es = ElasticSearchUtils('192.168.12.220')
    query = {
      "size": 1,
      "query": {
          "range": {
              "pub_time_timestamp": {
                  "gte": 1670601600000,
                  "lte": 1671465600000
              }
          }
      }
    }
    print(es.search_data_by_query('hot_news_test_03', query))
