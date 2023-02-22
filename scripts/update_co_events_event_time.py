# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_co_events_event_time         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/1/12 15:58
# @Software : win10 python3.6
"""
更新hot_co_events历史事件时间的时间戳
"""
from utils.ESUtils import ElasticSearchUtils
from utils.DateUtils import strftime_to_timestamp

# host = '192.168.12.197,192.168.12.198,192.168.12.199'
# pages = 38
# host = '192.168.12.220,192.168.12.221,192.168.12.222,192.168.12.223'
host = '192.168.12.223'
pages = 81
index = 'hot_co_events'
size = 1000
num = 0

es = ElasticSearchUtils(host)

for page in range(pages):
    query = {
        "from": page * size,
        "size": size,
        "_source": ["event_time"]
    }
    data_list = es.search_data_by_query(index, query)['hits']['hits']
    for data in data_list:
        data['_source']['event_timestamp'] = strftime_to_timestamp(data['_source']['event_time'])
        data['_op_type'] = 'update'
        data['doc_as_upsert'] = True
        data['doc'] = data.pop('_source')
    num += len(data_list)
    es.save_data_by_bulk(index, data_list)
    print(f'已更新{page}页,{num}条')
