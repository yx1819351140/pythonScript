# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> download_tuna_mirrors         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/5/23 9:53
# @Software : win10 python3.6
import json
import os.path
import requests
from lxml import etree

proxies = {'http': '192.168.12.180:6666', 'https': '192.168.12.180:6666'}


class DownloadFile(object):

    def __init__(self):
        self.url = 'https://mirrors.tuna.tsinghua.edu.cn/'
        self.file_list = []
        with open('../test/data.txt', 'r', encoding='utf-8') as f:
            temp_data_list = f.readlines()
        for temp_data in temp_data_list:
            try:
                name = temp_data.split(',')[1]
                is_net = temp_data.split(',')[3].strip()
                if is_net == '是':
                    self.file_list.append(name)
            except:
                continue

    def get_download_url(self):
        for file_name in self.file_list[:1]:
            self.get_file_list(self.url, file_name)

    def get_file_list(self, parent_url, file_name):
        url = parent_url + file_name + '/'
        res = requests.get(url)
        html = etree.HTML(res.text)
        temp_url_list = html.xpath('//tr/td/a/@href')
        size_list = html.xpath('//tr/td[@class="size"]/text()')
        for i in range(len(temp_url_list)):
            temp_url = temp_url_list[i]
            if temp_url == '../':
                continue
            elif '/' in temp_url:
                self.get_file_list(url, temp_url)
            else:
                self.download_file(url, temp_url, size_list[i])

    def download_file(self, parent_url, file_name, fize_size):
        url = parent_url + file_name
        file_path = parent_url.replace('https://mirrors.tuna.tsinghua.edu.cn/', './data/tuna/')
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        print(f'{file_path}{file_name}, 大小: {fize_size}, 下载中...')
        with open(file_path + file_name, 'wb') as f:
            f.write(requests.get(url, proxies=proxies).content)
        print(file_path + file_name + '下载完成')

    def get_file_size(self):
        url = 'https://mirrors.tuna.tsinghua.edu.cn/static/tunasync.json'
        res = requests.get(url)
        data_list = json.loads(res.text)
        for data in data_list:
            name = data['name']
            size = data['size']
            if name in self.file_list:
                print(name, size)

    def download_openwrt(self):
        url = 'https://mirrors.tuna.tsinghua.edu.cn/openwrt/releases/22.03.5/targets/x86/generic/'
        res = requests.get(url, proxies=proxies)
        html = etree.HTML(res.text)
        temp_url_list = html.xpath('//tr/td/a/@href')
        size_list = html.xpath('//tr/td[@class="size"]/text()')
        for i in range(len(temp_url_list)):
            temp_url = temp_url_list[i]
            if temp_url == '../':
                continue
            elif '/' in temp_url:
                continue
            else:
                self.download_file(url, temp_url, size_list[i])


if __name__ == '__main__':
    downloader = DownloadFile()
    downloader.download_openwrt()
