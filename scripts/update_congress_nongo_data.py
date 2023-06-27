# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_congress_nongo_data         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/6/27 14:43
# @Software : win10 python3.6
import time
from selenium.webdriver import Chrome, ChromeOptions
import requests
from pymongo import MongoClient
from lxml import etree


def update_data():
    client = MongoClient('mongodb://root:bigdatapass@192.168.12.220:27017,192.168.12.221:27017,192.168.12.222:27017/?replicaSet=repl&readPreference=primaryPreferred&authSource=admin')
    db = client['congress']
    table = db['congress']
    datas = table.find({"news_text_list": [ ]})
    for data in datas:
        congress_bill_number = data['congress_bill_number']
        congress_type = data['summary_title'].split('—')[0].replace(congress_bill_number, '').replace('.', '').lower().strip()
        url = f'https://www.govinfo.gov/bulkdata/BILLSTATUS/118/{congress_type}/BILLSTATUS-118{congress_type}{congress_bill_number}.xml'
        print(url)
        res = requests.get(url)
        html = etree.XML(res.content)
        # 议案内容列表
        news_text_list = []
        news_text_html_list = html.xpath('//bill/textVersions/item')
        for news_text_html in news_text_html_list:
            try:
                news_text_type = news_text_html.xpath('./type/text()')[0]
            except:
                news_text_type = ''
            try:
                news_text_date = format_date(news_text_html.xpath('./date/text()')[0].split('T')[0])
            except:
                news_text_date = ''
            content_option_value = f'{news_text_type} ({news_text_date})'
            try:
                content_url = news_text_html.xpath('./formats/item/url/text()')[0]
            except:
                continue
            content, content_text = get_content(content_url)
            news_text_list.append(
                {'content_option_value': content_option_value, 'content': content, 'content_text': content_text})
        content_data = {'news_text_list': news_text_list}
        table.update_one({'_id': data['_id']}, {'$set': content_data}, upsert=True)
        print(f'{congress_type}{congress_bill_number} update success!')


def format_date(date):
    try:
        return time.strftime('%m/%d/%Y', time.strptime(date, '%Y-%m-%d'))
    except:
        return date


def get_content(content_url):
    try:
        options = ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # 去掉提示 Chrome正收到自动测试软件的控制
        options.add_argument('disable-infobars')
        options.add_argument(f'--proxy-server=http://192.168.12.180:6666')
        # 线上指定driver路径
        # driver = Chrome(options=options, executable_path='/home/bigdata/apps/yangxin/chromedriver')
        # 本地调用使用默认环境变量driver，如果未添加则也需要指定路径
        driver = Chrome(options=options)
        driver.get(content_url)
        # print(driver.page_source)
        html = etree.HTML(driver.page_source)
        content_text = ''
        p_list = html.xpath('//body[@class="lbexBody"]//p')
        for p_elem in p_list:
            p_text = ''
            temp_p_text_list = p_elem.xpath('.//text()')
            for temp_p_text in temp_p_text_list:
                p_text += temp_p_text
            if p_text.strip():
                content_text = content_text + p_text + '\n'
        if not content_text:
            content_text = driver.find_element_by_xpath('//*').text
        html = driver.page_source
        driver.close()
        return html, content_text
    except Exception as e:
        log_txt = f'[get_content]获取议案正文内容失败，失败原因：{e}，url：{content_url}'
        print(log_txt)
        return '', ''


if __name__ == '__main__':
    update_data()
