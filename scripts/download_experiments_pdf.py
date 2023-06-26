# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> download_experiments_pdf         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/6/8 13:19
# @Software : win10 python3.6
"""
帮忙下载海陆空学院实验文档
"""
import json
import os
from lxml import etree
import requests
from settings import PROXY


def download_afit_pdf():
    # url = 'https://www.afit.edu/STAT/page.cfm?page=1941'
    # res = requests.get(url)
    with open('data/afit.html', 'r', encoding='utf-8') as f:
        text = f.read()
    html = etree.HTML(text)
    temp_dir_name_list = html.xpath('//div[@id="ssPageContent"]//strong/text()')
    dir_name_list = []
    for temp_dir_name in temp_dir_name_list:
        if temp_dir_name.strip():
            dir_name_list.append(temp_dir_name)
    for i in range(len(dir_name_list)):
        dir_name = 'data/AFIT/' + dir_name_list[i]
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        pdf_url_list = html.xpath(f'//div[@id="ssPageContent"]//ul[{i+1}]/li/a/@href')
        pdf_name_list = html.xpath(f'//div[@id="ssPageContent"]//ul[{i+1}]/li/a/text()')
        if pdf_url_list:
            for i in range(len(pdf_url_list)):
                res = requests.get(pdf_url_list[i], proxies=PROXY)
                pdf_name = f'{dir_name}/{pdf_name_list[i]}.pdf'.replace(':', ' ')
                with open(pdf_name, 'wb') as f:
                    f.write(res.content)
                print(f'{pdf_name}下载完成')


def download_nps_pdf():
    with open('data/nps.json', 'r', encoding='utf-8') as f:
        json_data = f.read()
    dict_data = json.loads(json_data)
    result_list = dict_data['results']
    for result in result_list:
        try:
            pdf_name = 'data/nps/' + result['titleNoFormatting'].replace('...', '').replace('\\', '').replace('/', '').replace(':', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', '') + '.pdf'
            pdf_url = result['unescapedUrl']
            res = requests.get(pdf_url, proxies=PROXY)
            with open(pdf_name, 'wb') as f:
                f.write(res.content)
            print(f'{pdf_name}下载完成')
        except:
            continue


def download_dau_pdf():
    with open('data/dau.json', 'r', encoding='utf-8') as f:
        json_data = f.read()
    result_list = json.loads(json_data)
    for result in result_list:
        try:
            pdf_name = 'data/nps/' + result['Title'].replace('...', '').replace('\\', '').replace('/', '').replace(':', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', '') + '.pdf'
            pdf_url = result['ServerRedirectedURL']
            res = requests.get(pdf_url, proxies=PROXY)
            with open(pdf_name, 'wb') as f:
                f.write(res.content)
            print(f'{pdf_name}下载完成')
        except:
            continue


if __name__ == '__main__':
    # download_afit_pdf()
    download_nps_pdf()
