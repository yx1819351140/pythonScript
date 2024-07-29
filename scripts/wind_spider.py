# -*- coding:UTF-8 -*-
# @Time    : 2024/7/9 09:32
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : wind_spider.py
# @Project : pythonScript
# @Software: PyCharm
"""
万得客户端采集
"""
import time
import requests
import pymysql
import redis
import os
from loguru import logger
import random


class WindSpider(object):

    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="app", charset="utf8", port=9030)
    cursor_doris = db_doris.cursor()
    # redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client = redis.StrictRedis(host='10.32.51.2', port=6379, db=1)
    data_path = 'C:/data/万得数据'
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    cursor_doris.execute("""
CREATE TABLE IF NOT EXISTS ods.ods_wind_company_client (
  `company_name` VARCHAR COMMENT '企业名称',
  `client_name` VARCHAR COMMENT '客户名称',
  `company_id` LARGEINT COMMENT '企业id',
  `client_id` LARGEINT COMMENT '客户id',
  `client_time` VARCHAR COMMENT '客户时间',
  `client_cluster_id` VARCHAR COMMENT '客户领域id',
  `client_cluster_name` VARCHAR COMMENT '客户领域名称',
  `client_number` VARCHAR(30) COMMENT '客户出现次数（新闻）',
  `client_amount` VARCHAR(30) COMMENT '客户交易金额（公告）',
  `client_source` VARCHAR(30) COMMENT '客户来源',
  `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`company_name`, `client_name`)
COMMENT "万得数据-企业客户表"
DISTRIBUTED BY HASH(`company_name`, `client_name`) BUCKETS 16
PROPERTIES (
   "replication_allocation" = "tag.location.online: 1, tag.location.offline: 2",
   "in_memory" = "false",
   "storage_format" = "V2"
);""")

    cursor_doris.execute("""
CREATE TABLE IF NOT EXISTS ods.ods_wind_company_supplier (
  `company_name` VARCHAR COMMENT '企业名称',
  `supplier_name` VARCHAR COMMENT '供应商名称',
  `company_id` LARGEINT COMMENT '企业id',
  `supplier_id` LARGEINT COMMENT '供应商id',
  `supplier_time` VARCHAR COMMENT '供应商时间',
  `supplier_cluster_id` VARCHAR COMMENT '供应商领域id',
  `supplier_cluster_name` VARCHAR COMMENT '供应商领域名称',
  `supplier_number` VARCHAR(30) COMMENT '供应商出现次数（新闻）',
  `supplier_amount` VARCHAR(30) COMMENT '供应商交易金额（公告）',
  `supplier_source` VARCHAR(30) COMMENT '供应商来源',
  `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
UNIQUE KEY(`company_name`, `supplier_name`)
COMMENT "万得数据-企业供应商表"
DISTRIBUTED BY HASH(`company_name`, `supplier_name`) BUCKETS 16
PROPERTIES (
   "replication_allocation" = "tag.location.online: 1, tag.location.offline: 2",
   "in_memory" = "false",
   "storage_format" = "V2"
);""")

    def get_data(self, company_name):
        try:
            headers = {
                'Host': '114.80.154.45',
                'Connection': 'keep-alive',
                'Content-Length': '53',
                'Accept': '*/*',
                'Authorization': 'Bearer null',
                'Content-Type': 'application/json',
                'Cookie': 'wind.sessionid=547e8250b2224d779cb1d9129a453fa1; JSESSIONID=1D2FAA16248162059DA33C6FBF7413EF; wind.sessionid=547e8250b2224d779cb1d9129a453fa1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
                'WKG-Lang': 'cn',
                'X-Wind-Rval': 'eyJwaWQiOjExNTM3LCJtYWdpYyI6Nzc0NzExLCJvcCI6IndtYWluIiwidGltZSI6IjIwMjQtMDctMTUgMDk6MjQ6MzAiLCJwYWdlIjo2ODl9',
                'sec-ch-ua': '"Chromium";v="109"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'wind-language': 'zh-CN',
                'wind.sessionid': 'c33cb35ba78a4ae29f660f2121588eef',
                'windsessionid': 'c33cb35ba78a4ae29f660f2121588eef',
                'windsessionid2': '3E2E1ADE12998DB18A9A8F7A55A1937B',
                'Origin': 'https://114.80.154.45',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://114.80.154.45/windkg/index.html',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7'
            }

            url = 'http://114.80.154.45/windkg.gateway/supplier/supplierListByName'
            data = {"company": company_name, "limit": 10}
            logger.info(f'开始采集 {company_name} 数据...')
            response = requests.post(url=url, headers=headers, json=data, timeout=5)
            if 'SessionId无效' in response.text:
                logger.error(f'SessionId无效或已过期，请重新获取SessionId！\n')
                return
            supplier_list = response.json().get('data', {}).get('tableSupplierList', [])
            client_list = response.json().get('data', {}).get('tableClientList', [])

            # 写入供应商数据
            if supplier_list:
                insert_sql = 'insert into ods.ods_wind_company_supplier (company_name, supplier_name, supplier_time, supplier_cluster_id, supplier_cluster_name, supplier_number, supplier_amount, supplier_source) values '
                supplier_data_list = []
                for supplier in supplier_list:
                    supplier_name = supplier.get('company', '')
                    supplier_time = supplier.get('timeStr', '')
                    supplier_cluster_id = supplier.get('cluster', '')
                    supplier_cluster_name = supplier.get('clusterName', '')
                    supplier_number = supplier.get('number', '')
                    supplier_amount = supplier.get('amount', '')
                    supplier_source = supplier.get('source', '')
                    supplier_data_list.append(f'("{company_name}","{supplier_name}","{supplier_time}","{supplier_cluster_id}","{supplier_cluster_name}","{supplier_number}","{supplier_amount}","{supplier_source}")')
                values = ','.join(supplier_data_list)
                insert_sql += values
                self.cursor_doris.execute(insert_sql)
                logger.info(f'{company_name} 已写入 供应商 数据 {len(supplier_data_list)} 条！')
            else:
                logger.info(f'{company_name} 供应商 数据为空')

            # 写入客户数据
            if client_list:
                insert_sql = 'insert into ods.ods_wind_company_client (company_name, client_name, client_time, client_cluster_id, client_cluster_name, client_number, client_amount, client_source) values '
                client_data_list = []
                for client in client_list:
                    client_name = client.get('company', '')
                    client_time = client.get('timeStr', '')
                    client_cluster_id = client.get('cluster', '')
                    client_cluster_name = client.get('clusterName', '')
                    client_number = client.get('number', '')
                    client_amount = client.get('amount', '')
                    client_source = client.get('source', '')
                    client_data_list.append(f'("{company_name}","{client_name}","{client_time}","{client_cluster_id}","{client_cluster_name}","{client_number}","{client_amount}","{client_source}")')
                values = ','.join(client_data_list)
                insert_sql += values
                self.cursor_doris.execute(insert_sql)
                logger.info(f'{company_name} 已写入 客户 数据 {len(client_data_list)} 条！\n')
            else:
                logger.info(f'{company_name} 客户 数据为空\n')

            with open(f'{self.data_path}/{company_name}.json', 'w', encoding='utf-8') as f:
                f.write(response.text)
            self.redis_client.sadd('company_name_list', company_name)
        except Exception as e:
            logger.error(f'{e}\n')

    def run(self):
        logger.info(f'正在查询上市公司数据...')
        with open('data/万得数据/上市企业名称.txt', 'r', encoding='utf-8') as f:
            company_list = f.readlines()
        logger.info(f'查询完毕，查询公司数量{len(company_list)}，开始采集！')
        if company_list:
            for company_name in company_list:
                company_name = company_name.strip()
                if not self.redis_client.sismember('company_name_list', company_name):
                    self.get_data(company_name)
                    # sleep = random.randint(1, 3)
                    # logger.info(f'随机停止中，停止时间：{sleep}秒')
                    # time.sleep(sleep)
                else:
                    logger.info(f'{company_name} 已采集过，skip\n')


if __name__ == '__main__':
    wind = WindSpider()
    wind.run()
