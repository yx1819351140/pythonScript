# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_solr_from_mongo         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/6/5 13:33
# @Software : win10 python3.6
"""
读取mongo议员言论数据，写入solr
"""
import json
import uuid
from datetime import datetime
import pymongo
import pysolr

client = pymongo.MongoClient('mongodb://root:bigdatapass@192.168.12.220:27017,192.168.12.221:27017,192.168.12.222:27017/?replicaSet=repl&readPreference=primaryPreferred&authSource=admin')
db = client['congress']
collection = db['news']
data_list = list(collection.find({"mediaType": "新闻媒体"}, {'_id': 0}))
print(len(data_list))
for data in data_list:
    data['id'] = data['newsId']
    publish_time = data['sortTime']
    try:
        dt = datetime.strptime(publish_time, '%Y-%m-%dT%H:%M:%SZ')
    except:
        data['sortTime'] = '2023-06-20T18:03:59Z'
        data['oriTime'] = '2023-06-20T18:03:59Z'
    try:
        data['content'] = data['content'] + data['author']
    except:
        continue


solr = pysolr.Solr('http://192.168.12.181:8080/solr/usppa_news', always_commit=True)
print(solr.ping())
solr.add(data_list)

client.close()


