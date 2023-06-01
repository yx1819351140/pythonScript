# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_person_index_url         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/5/31 17:09
# @Software : win10 python3.6
"""
更新议员主页网址
"""
import requests
from lxml import etree
from selenium.webdriver import ChromeOptions, Chrome
from datetime import datetime, timedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from utils.MysqlUtils import MySQLUtils


class Person(object):

    def __init__(self):
        self.url = 'https://www.congress.gov'
        self.data_list = []
        self.proxy = '192.168.12.180:6666'
        self.proxies = {'http': '192.168.12.180:6666', 'https': '192.168.12.180:6666'}
        self.mysql = MySQLUtils()

    def get_data_list(self, driver, url):
        driver.get(url)
        WebDriverWait(driver, 10, 0.5).until(EC.presence_of_element_located((By.CLASS_NAME, 'expanded')))
        html = etree.HTML(driver.page_source)
        url_list = html.xpath('//li[@class="expanded"]/span/a/@href')
        self.data_list += url_list
        nxt_url = html.xpath('//a[@class="next"]/@href')
        if nxt_url:
            nxt_url = self.url + nxt_url[0]
            self.get_data_list(driver, nxt_url)

    def get_person_index_url(self, driver, url):
        try:
            # options = ChromeOptions()
            # options.add_argument(f'--proxy-server=http://{self.proxy}')
            # # driver = Chrome(options=options, executable_path='/home/bigdata/apps/yangxin/chromedriver')
            # driver = Chrome(options=options)
            driver.get(url)
            WebDriverWait(driver, 10, 0.5).until(EC.presence_of_element_located((By.XPATH, '//table[@class="standard01 nomargin"]')))
            html = etree.HTML(driver.page_source)
            index_url = html.xpath('//table[@class="standard01 nomargin"]/tbody/tr[1]/td/a/@href')
            if index_url:
                index_url = index_url[0]
            else:
                index_url = ''
            if index_url:
                index_url = index_url[:-1] if index_url.endswith('/') else index_url
                res = requests.get(index_url, proxies=self.proxies)
                html = etree.HTML(res.text)
                temp_news_url_list1 = html.xpath('//ul//a[contains(text(), "the News")]/@href')
                temp_news_url_list2 = html.xpath('//ul//a[contains(text(), "The News")]/@href')
                temp_news_url_list = temp_news_url_list1 + temp_news_url_list2
                if temp_news_url_list:
                    temp_news_url = temp_news_url_list[-1]
                    if 'http' not in temp_news_url:
                        if temp_news_url.startswith('/'):
                            news_url = index_url + temp_news_url
                        else:
                            news_url = index_url + '/' + temp_news_url
                    else:
                        news_url = temp_news_url
                else:
                    news_url = ''
            else:
                news_url = ''
            return index_url, news_url
        except Exception as e:
            print(e)
            return '', ''

    def main(self):
        options = ChromeOptions()
        options.add_argument(f'--proxy-server=http://{self.proxy}')
        # driver = Chrome(options=options, executable_path='/home/bigdata/apps/yangxin/chromedriver')
        driver = Chrome(options=options)
        # url = 'https://www.congress.gov/search?q=%7B%22source%22%3A%22members%22%2C%22congress%22%3A118%7D'
        url = 'https://www.congress.gov/search?q=%7B%22source%22%3A%22members%22%2C%22congress%22%3A118%7D&page=6'
        self.get_data_list(driver, url)
        if self.data_list:
            for temp_url in self.data_list:
                url = self.url + temp_url
                print(url)
                person_id = temp_url.split('/')[-1].split('?')[0]
                index_url, news_url = self.get_person_index_url(driver, url)
                query = f'update usppa_person_info set index_url="{index_url}",news_url="{news_url}" where person_id="{person_id}" and position_session=118'
                print(query)
                self.mysql.update(query)
        driver.close()


if __name__ == '__main__':
    person = Person()
    person.main()
    # print(person.get_person_index_url('https://www.congress.gov/member/james-baird/B001307?s=4&r=13'))
