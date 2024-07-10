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
import requests
import pymysql
import redis
from loguru import logger


class WindSpider(object):

    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="app", charset="utf8", port=9030)
    cursor_doris = db_doris.cursor()
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    company_list = list(cursor_doris.fetchall("select company_id, company_name in company where reg_status_name = '在营'"))

    def get_data(self, company_id, company_name):
        try:
            headers = {
                'Host': '114.80.154.45',
                'Connection': 'keep-alive',
                'Content-Length': '53',
                'Accept': '*/*',
                'Authorization': 'Bearer null',
                'Content-Type': 'application/json',
                'Cookie': 'wind.sessionid=0dad2a4117d242b2bef8a0fda1a071ae; JSESSIONID=D353B4DDE9E30812807754763275B711; Hm_lvt_e8002ef3d9e0d8274b5b74cc4a027d08=1720492223; wind.sessionid=0dad2a4117d242b2bef8a0fda1a071ae',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
                'WKG-Lang': 'cn',
                'X-Wind-Rval': 'eyJwaWQiOjU1MTIxLCJtYWdpYyI6ODk4NjY5LCJvcCI6IndtYWluIiwidGltZSI6IjIwMjQtMDctMDkgMTc6MjM6MjAiLCJwYWdlIjo3N30=',
                'sec-ch-ua': '"Chromium";v="109"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'wind-language': 'zh-CN',
                'wind.sessionid': '0dad2a4117d242b2bef8a0fda1a071ae',
                'windsessionid': '0dad2a4117d242b2bef8a0fda1a071ae',
                'windsessionid2': '1693AF78752C0E95ABBD66B9AC571AF7YSP',
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
            response = requests.post(url=url, headers=headers, json=data, verify=False)
            with open(f'data/{company_name}.json', 'w', encoding='utf-8') as f:
                f.write(response.text)
            self.redis_client.sadd('company_id_list', company_id)
        except Exception as e:
            logger.error(e)

    def run(self):
        if self.company_list:
            for company in self.company_list:
                company_id = company.get('company_id', '')
                company_name = company.get('company_name', '')
                if not self.redis_client.sismember('company_id_list', company_id):
                    self.get_data(company_id)


if __name__ == '__main__':
    wind = WindSpider()
    wind.get_data('比亚迪股份有限公司')
