# -*- coding:UTF-8 -*-
# @Time    : 24.8.6 13:43
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : qianzhan_spider.py
# @Project : pythonScript
# @Software: PyCharm
import json

import requests
from loguru import logger
from lxml import etree
from pypinyin import pinyin, Style
import redis
import random
import hashlib
import pymysql
import urllib.parse


class QianzhanSpider(object):

    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        }
        self.url1 = 'https://y.qianzhan.com/system/GetTableData2'
        self.url2 = 'https://y.qianzhan.com/yuanqu/loadComps'
        self.cookies = {'qznewsite.uid': '2jqj1lj3ginjlpxvjx4ksrv'}
        self.pool = redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        self.start_page = int(self.r.get('qianzhan_park_page') if self.r.get('qianzhan_park_page') else '1')
        self.proxy = self.get_proxy()
        self.db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="ods",
                                        charset="utf8", port=9030)
        self.cursor_doris = self.db_doris.cursor()
        self.cursor_doris.execute("""
                    CREATE TABLE IF NOT EXISTS ods.ods_province_park (
                      `PID` varchar(50) DEFAULT NULL COMMENT '唯一索引-PARK',
                      `PARKNAME` varchar(200) DEFAULT NULL COMMENT '园区名称',
                      `PROVINCE` varchar(50) DEFAULT NULL COMMENT '省份',
                      `CITY` varchar(50) DEFAULT NULL COMMENT '城市-地级',
                      `COUNTY` varchar(50) DEFAULT NULL COMMENT '地区-区县',
                      `DOM` string DEFAULT NULL COMMENT '详细地址',
                      `AREA` varchar(50) DEFAULT '0' COMMENT '大约面积（亩）',
                      `AMOUNT` varchar(50) DEFAULT '0' COMMENT '企业数量',
                      `CONTENT` string DEFAULT NULL COMMENT '园区介绍',
                      `LINK` varchar(200) DEFAULT NULL COMMENT '详情链接',
                      `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                      `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
                    ) ENGINE=OLAP
                    UNIQUE KEY(`PID`)
                    COMMENT "前瞻产业园区-产业园信息表"
                    DISTRIBUTED BY HASH(`PID`) BUCKETS 16
                    PROPERTIES (
                       "replication_allocation" = "tag.location.online: 1",
                       "in_memory" = "false",
                       "storage_format" = "V2"
                    );""")

        self.cursor_doris.execute("""
                    CREATE TABLE IF NOT EXISTS ods.ods_province_park_ent (
                      `ENTID` varchar(200) NOT NULL COMMENT '企业id',
                      `PID` varchar(50) NOT NULL,
                      `ENTNAME` varchar(200) DEFAULT NULL COMMENT '园区企业名称',
                      `CODE` varchar(200) DEFAULT NULL COMMENT '企业统代',
                      `REGCAP` varchar(100) DEFAULT NULL COMMENT '注册资金',
                      `ESDATE` date DEFAULT NULL COMMENT '成立时间',
                      `ADDRESS` string DEFAULT NULL COMMENT '注册地址',
                      `OPSCOPE` string DEFAULT NULL COMMENT '经营范围',
                      `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                      `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
                    ) ENGINE=OLAP
                    UNIQUE KEY(`ENTID`)
                    COMMENT "前瞻产业园区-公司信息表"
                    DISTRIBUTED BY HASH(`ENTID`) BUCKETS 16
                    PROPERTIES (
                       "replication_allocation" = "tag.location.online: 1",
                       "in_memory" = "false",
                       "storage_format" = "V2"
                    );""")

    def get_proxy(self):
        proxies_list = self.r.zrange("WholeJuLiangDaiLi", 0, -1)
        random_number = random.randint(0, len(proxies_list) - 1)
        ip = proxies_list[random_number].split('@')[1]
        return {"http": f'http://{ip}', "https": f'http://{ip}'}

    def get_park(self):
        logger.info(f'正在采集第 {self.start_page} 页...')
        data = {
            'page': str(self.start_page),
            'pageSize': '20',
            'level': '1',
            'NodeType': '0',
            'match': '0',
            'agg': '0',
            'way': 'desc',
        }
        res = requests.get(self.url1, cookies=self.cookies, data=data)
        park_list = res.json().get('list', [])
        insert_sql = 'insert into ods.ods_province_park (PID, PARKNAME, PROVINCE, CITY, COUNTY, DOM, AREA, AMOUNT, CONTENT, LINK) values '
        if park_list:
            data_list = []
            for park in park_list:
                pid = park.get('y_uid', '')
                if pid:
                    park_name = park.get('y_name', '')
                    province = park.get('y_province', '')
                    city = park.get('y_city', '')
                    county = park.get('y_district', '')
                    dom = ''
                    area = park.get('y_area', 0)
                    amount = park.get('y_comps', 0)
                    content = ''
                    link = f'https://y.qianzhan.com/yuanqu/item/{pid}.html'
                    data_list.append(f'("{pid}","{park_name}","{province}","{city}","{county}","{dom}","{area}","{amount}","{content}","{link}")')
                    self.get_park_ent(pid, park_name)
            values = ','.join(data_list)
            insert_sql += values
            self.cursor_doris.execute(insert_sql)
            logger.info(f'第 {self.start_page} 页已采集！\n')
            self.start_page += 1
            self.r.set('qianzhan_park_page', self.start_page)
            self.get_park()

    def get_park_ent(self, yid, park_name):
        data = {
            'yid': yid,
            'page': '1'
        }
        res = requests.get(self.url2, data=data)
        html = etree.HTML(res.text)
        elem_li = html.xpath('//tbody/tr')
        insert_sql = 'insert into ods.ods_province_park_ent (PID, ENTID, ENTNAME, CODE, REGCAP, ESDATE, ADDRESS, OPSCOPE) values '
        if elem_li:
            data_list = []
            for elem in elem_li:
                ent_id = ''.join(elem.xpath('./@cid'))
                ent_name = ''.join(elem.xpath('./td[2]/a/text()'))
                code = ''
                reg_cap = ''.join(elem.xpath('./td[3]/text()'))
                es_date = ''.join(elem.xpath('./td[4]/text()'))
                address = ''
                op_scope = ''.join(elem.xpath('./td[5]/text()'))
                data_list.append(f'("{yid}","{ent_id}","{ent_name}","{code}","{reg_cap}","{es_date}","{address}","{op_scope}")')
            values = ','.join(data_list)
            insert_sql += values
            self.cursor_doris.execute(insert_sql)
            logger.info(f'{park_name} 对应公司已采集！')


if __name__ == '__main__':
    qianzhan = QianzhanSpider()
    qianzhan.get_park()
