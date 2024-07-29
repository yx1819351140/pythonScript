# -*- coding:UTF-8 -*-
# @Time    : 24.7.24 10:17
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : made_in_china_spider.py
# @Project : pythonScript
# @Software: PyCharm
import requests
from loguru import logger
from lxml import etree
from pypinyin import pinyin, Style
import redis
import random
import hashlib
import pymysql
import urllib.parse


class MadeInChinaSpider(object):

    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        }
        self.pool = redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        self.proxy = self.get_proxy()
        self.db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="ods", charset="utf8", port=9030)
        self.cursor_doris = self.db_doris.cursor()
        self.cursor_doris.execute("""
            CREATE TABLE IF NOT EXISTS ods.ods_madeinchina_company_info (
              `company_name` VARCHAR COMMENT '企业名称',
              `pro_name` VARCHAR COMMENT '主营产品',
              `pro_model` VARCHAR COMMENT '经营模式',
              `industry_name1` VARCHAR COMMENT '行业分类（一级）',
              `industry_name2` VARCHAR COMMENT '行业分类（二级）',
              `industry_name3` VARCHAR COMMENT '行业分类（三级）',
              `industry_name4` VARCHAR COMMENT '行业分类（四级）',
              `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
            ) ENGINE=OLAP
            UNIQUE KEY(`company_name`)
            COMMENT "中国制造网-公司信息表"
            DISTRIBUTED BY HASH(`company_name`) BUCKETS 16
            PROPERTIES (
               "replication_allocation" = "tag.location.online: 1",
               "in_memory" = "false",
               "storage_format" = "V2"
            );""")

    # 访问公司列表页，获取公司详细信息
    def get_company_info(self, industry_name1='', industry_name2='', industry_name3='', industry_name4='', data_params='', page=1):
        word = self.url_encode_string(industry_name3)
        # url = f'https://cn.made-in-china.com/productdirectory.do?xcase=mapc&code={data_params}&code4BrowerHistory={data_params}&order=0&style=b&page={page}&comProvince=nolimit&comCity=&size=60&viewType=3&sizeHasChanged=0&uniqfield=&priceStart=&priceEnd=&quantityBegin=&word={word}&tradeType=&from=hunt&hotflag=1'
        url = f'https://cn.made-in-china.com/productdirectory.do?propertyValues=&xcase=mapc&senior=0&certFlag=1&code={data_params}&code4BrowerHistory={data_params}&order=0&style=b&page={page}&comProvince=nolimit&comCity=&size=60&viewType=3&sizeHasChanged=0&uniqfield=&priceStart=&priceEnd=&quantityBegin=&word={word}&tradeType=&from=hunt&hotflag=1'
        res = self.get_response(url)
        try:
            text = res.content.decode('utf-8')
        except:
            text = res.content.decode('gbk')
        if 'Too Many Requests' in text or '请验证' in text or res.status_code >= 300:
            logger.info(f'获取 {industry_name1}-{industry_name2}-{industry_name3}-{industry_name4} 响应失败\n请求网址：{url}\n响应内容：{res.text}\n')
            return
        else:
            try:
                html = etree.HTML(text)
                elem_li = html.xpath('//*[@id="inquiryForm"]/li')
                if elem_li:
                    insert_sql = 'insert into ods.ods_madeinchina_company_info (company_name, pro_name, pro_model, industry_name1, industry_name2, industry_name3, industry_name4) values '
                    company_list = []
                    for elem in elem_li:
                        company_name = ''.join(elem.xpath('./div[1]/label[@class="co-name"]/a/text()'))
                        pro_name = ' '.join(elem.xpath('./div[2]/div/ul[@class="co-intro"]/li[1]/a/text()'))
                        pro_model = ''.join(elem.xpath('./div[2]/div/ul[@class="co-intro"]/li[2]/a/text()'))
                        company_list.append(f'("{company_name}","{pro_name}","{pro_model}","{industry_name1}","{industry_name2}","{industry_name3}","{industry_name4}")')
                    values = ','.join(company_list)
                    insert_sql += values
                    self.cursor_doris.execute(insert_sql)
                    logger.info(f'{industry_name4} 写入公司数据 {len(company_list)} 条！')
                    industry_name = f'{industry_name1}-{industry_name2}-{industry_name3}-{industry_name4}-{page}'
                    self.r.sadd('industry_name', industry_name)
                    # 翻页
                    nxt_page = html.xpath('//a[@class="page-next"]')
                    if nxt_page:
                        page += 1
                        self.get_company_info(industry_name1, industry_name2, industry_name3, industry_name4, data_params, page)
                else:
                    logger.info(f'获取 {industry_name1}-{industry_name2}-{industry_name3}-{industry_name4} 对应公司列表为空\n')
            except Exception as e:
                logger.error(f'解析公司列表页失败，请求网址：{url}，失败原因：{e}\n')

    # 获取四级行业分类
    def get_industry_name4(self, industry_name1, industry_name2, industry_name3):
        suffix = self.url_encode_string(industry_name3)
        # url = f'https://cn.made-in-china.com/productdirectory.do?subaction=hunt&mode=and&style=b&comProvince=nolimit&miccnsource=1&size=60&word={suffix}'
        url = f'https://cn.made-in-china.com/productdirectory.do?subaction=hunt&mode=and&style=b&comProvince=nolimit&code=EEnxEJQbMJmm&size=60&word={suffix}'
        try:
            res = self.get_response(url)
            if res.status_code == 200:
                html = etree.HTML(res.text)
                elem_span = html.xpath('//dl[@class="filter-first"]//span')
                if elem_span:
                    for elem in elem_span:
                        industry_name4 = ''.join(elem.xpath('./text()'))
                        if industry_name4:
                            data_params = elem.xpath('./@data-param')[0]
                            page = 1
                            industry_name = f'{industry_name1}-{industry_name2}-{industry_name3}-{industry_name4}-{page}'
                            if not self.r.sismember('industry_name', industry_name):
                                self.get_company_info(industry_name1, industry_name2, industry_name3, industry_name4, data_params, page)
                            else:
                                logger.info(f'{industry_name} 已采集过，skip\n')
                else:
                    with open('../test/test.html', 'wb') as f:
                        f.write(res.content)
                    logger.warning(f'{industry_name1}-{industry_name2}-{industry_name3} 对应的四级行业分类列表为空！\n')
            else:
                logger.info(f'获取 {industry_name3} 响应失败，请求网址：{url}\n响应内容：{res.text}\n')
        except Exception as e:
            logger.info(f'获取 {industry_name3} 响应失败，请求网址：{url}\n失败原因：{e}\n')

    # 获取一级、二级、三级行业分类
    def get_industry_name(self):
        url = 'https://cn.made-in-china.com/'
        res = self.get_response(url)
        html = etree.HTML(res.text)
        elem_li = html.xpath('//div[@class="catalog-box"]/ul/li')
        if elem_li:
            for i in range(len(elem_li)):
                industry_name1 = elem_li[i].xpath('./div/a[2]/text()')[0]
                elem_dl = html.xpath(f'//div[@class="catalog-box"]/div/div[{i+1}]//div[@class="sub-cata-bd"]/dl')
                if elem_dl:
                    for elem in elem_dl:
                        industry_name2 = elem.xpath('./dt/a/text()')[0]
                        industry_name3_list = elem.xpath('./dd/a/text()')
                        if industry_name3_list:
                            for industry_name3 in industry_name3_list:
                                self.get_industry_name4(industry_name1, industry_name2, industry_name3)
                        else:
                            logger.warning(f'{industry_name1}-{industry_name2} 对应的三级行业分类列表为空！')
                else:
                    logger.warning(f'{industry_name1} 对应的二级行业分类列表为空！')
        else:
            logger.warning('一级行业分类列表为空！')

    # 获取拼音首字母简写
    @staticmethod
    def get_chinese_initials(text):
        return ''.join([word[0][0] for word in pinyin(text, style=Style.FIRST_LETTER)]).lower()

    # 通过代理ip获取响应
    def get_response(self, url):
        fail_num = 0
        while True:
            if fail_num >= 100:
                logger.warning(f'代理池连续失败100次，本次请求不设置代理')
                return requests.get(url, headers=self.headers)
            else:
                try:
                    res = requests.get(url, headers=self.headers, proxies=self.proxy, timeout=10)
                    try:
                        text = res.content.decode('utf-8')
                    except:
                        text = res.content.decode('gbk')
                    if 'Too Many Requests' in text or '请验证' in text or res.status_code >= 300:
                        logger.warning(f'{url} 访问失败，失败计数+1，更换代理重新请求中...')
                        fail_num += 1
                        self.proxy = self.get_proxy()
                    else:
                        return res
                except:
                    logger.warning(f'{url} 访问失败，失败计数+1，更换代理重新请求中...')
                    fail_num += 1
                    self.proxy = self.get_proxy()

    # 获取代理ip
    def get_proxy(self):
        proxies_list = self.r.zrange("WholeJuLiangDaiLi", 0, -1)
        random_number = random.randint(0, len(proxies_list) - 1)
        ip = proxies_list[random_number].split('@')[1]
        return {"http": f'http://{ip}', "https": f'http://{ip}'}

    # 获取md5加密字符串
    @staticmethod
    def md5_string(in_str):
        md5 = hashlib.md5()
        md5.update(in_str.encode("utf8"))
        result = md5.hexdigest()
        return result

    @staticmethod
    def url_encode_string(in_str):
        # 先将字符串编码为 GB2312 字节
        gb2312_encoded_bytes = in_str.encode('gb2312')
        # 然后进行 URL 编码
        encoded_string = urllib.parse.quote(gb2312_encoded_bytes)
        return encoded_string


if __name__ == '__main__':
    made_in_china = MadeInChinaSpider()
    made_in_china.get_industry_name()
    # print(made_in_china.get_response('https://cn.made-in-china.com/market/cwfsj-1.html').content.decode('gbk'))
    # made_in_china.get_proxy()
