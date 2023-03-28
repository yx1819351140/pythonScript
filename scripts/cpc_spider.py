# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> cpc_spider         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/28 13:14
# @Software : win10 python3.6
"""
采集中国共产党新闻网，习大大相关新闻、求是、原文
"""
import requests
from lxml import etree
import datetime
import os
import time


class CpcSpider(object):

    def __init__(self):
        pass

    def start_request(self):
        url = 'http://cpc.people.com.cn/xijinping/'
        res = requests.get(url)
        html = etree.HTML(res.content.decode('gbk'))
        url_list = html.xpath('//h3[@class="white"]/span/a/@href')
        category_list = html.xpath('//h3[@class="white"]/span/a/text()')
        for i in range(len(url_list)):
            page = 1
            url = url_list[i].replace('index.html', f'index{page}.html')
            category = category_list[i]
            print(category)
            self.parse(url, category, page)

    def parse(self, url, category, page):
        current_year = datetime.datetime.today().year
        current_month = datetime.datetime.today().month
        file_path = f'C:/Users/yang/Desktop/data/{current_year}年{current_month}月习主席讲话、活动报道汇编/{category}'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        res = requests.get(url)
        html = etree.HTML(res.content.decode('gbk'))
        elem_list = html.xpath('//div[@class="fl"]/ul/li')
        for elem in elem_list:
            content_pub_time = elem.xpath('./i/text()')[0].replace('[', '').replace(']', '')
            content_pub_year = int(content_pub_time.split('年')[0])
            content_pub_month = int(content_pub_time.split('年')[-1].split('月')[0])
            if content_pub_year < current_year or content_pub_month < current_month:
                return
            content_url = 'http://cpc.people.com.cn' + elem.xpath('./a/@href')[0]
            content_title = elem.xpath('./a/text()')[0].replace('\n', '')
            print(content_title, content_url, content_pub_time)
            content_text = self.parse_content(content_url)
            file_name = f'{file_path}/{content_pub_time.replace("年", "").replace("月", "").replace("日", "")} {content_title}.txt'
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(content_title + '\n')
                f.write(f'({content_pub_time})' + '\n')
                f.write(content_text)
        nxt_page = page + 1
        url = url.replace(f'index{page}.html', f'index{nxt_page}.html')
        self.parse(url, category, page)

    def parse_content(self, url):
        text = ''
        res = requests.get(url)
        html = etree.HTML(res.content.decode('gbk'))
        elem_list = html.xpath('//div[@class="show_text"]/p')
        for elem in elem_list:
            temp_text_list = elem.xpath('.//text()')
            for temp_text in temp_text_list:
                text = text + temp_text.strip().replace('\xa0', '') + '\n'
        return text

    def run(self):
        self.start_request()


if __name__ == '__main__':
    cpc = CpcSpider()
    cpc.run()
