# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> download_news_img         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/20 9:28
# @Software : win10 python3.6
"""
临时任务，将es图片地址下载到minio，替换图片地址为本地minio地址
"""
import io
import requests
from elasticsearch import Elasticsearch
from minio import Minio


def search_img_by_id(news_id='', host='192.168.12.197', port=9200, index='hot_news_test_03'):
    """
    根据id查询es
    :param news_id:
    :param host:
    :param port:
    :param index:
    :return:
    """
    es = Elasticsearch([{'host': host, 'port': port}])
    query = {
        "_source": ["id", "image_url"],
        "query": {
            "match": {
                '_id': news_id
            }
        }
    }
    res = es.search(index=index, body=query, request_timeout=60*3)
    return res['hits']['hits'][0]['_source']


def downad_img_2_minio(news_id, img_url):
    """
    下载图片至minio
    :param img_url:
    :return: 图片地址
    """
    res = requests.get(img_url, proxies={'http': 'https://192.168.12.180:6666', 'https': 'http://192.168.12.180:6666'})
    minio = Minio(endpoint='192.168.12.100:9000', access_key='minio', secret_key='admin123456', secure=False)
    img_name = f'{news_id}.jpg'
    minio.put_object(bucket_name='toubiao', object_name=f'news/{img_name}', data=io.BytesIO(res.content),
                                  length=-1, content_type='image/png', part_size=10 * 1024 * 1024)
    return f'http://192.168.12.100:9000/toubiao/news/{img_name}'


def update_img_by_id(news_id='', doc: dict={}, doc_type='_doc', host='127.0.0.1', port=9200, index='hot_news_test_03'):
    """
    更新新闻图片地址
    :param news_id:
    :param doc:
    :param doc_type:
    :param host:
    :param port:
    :param index:
    :return:
    """
    body = {"doc": doc}
    es = Elasticsearch([{'host': host, 'port': port}])
    res = es.update(index=index, doc_type=doc_type, id=news_id, body=body)
    print(res)


def run(news_id, host='127.0.0.1'):
    img_data = search_img_by_id(news_id=news_id, host=host)
    try:
        img_url = downad_img_2_minio(img_data['image_url'])
        doc = {"id": news_id, "image_url": img_url}
        update_img_by_id(news_id=news_id, host=host, doc=doc)
    except:
        pass


if __name__ == '__main__':
    # run('4cbdae15736a761ab2fb0b0d921574ff', '192.168.12.197')
    run('4cbdae15736a761ab2fb0b0d921574ff')  # 不带host默认127.0.0.1
