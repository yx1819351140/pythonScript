# -*- coding:UTF-8 -*-
# @Time    : 24.9.20 15:45
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : news_spider.py
# @Project : pythonScript
# @Software: PyCharm
import requests
import redis
import random
from lxml import etree
from gne import GeneralNewsExtractor

r = redis.Redis(connection_pool=redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True))


def get_proxies():
    proxies_list = r.zrange("JuLiangTimeM0T1", 0, -1)
    ip = proxies_list[random.randint(0, len(proxies_list) - 1)]
    return {"http": f'http://{ip}', "https": f'http://{ip}'}


def get_url_list(keyword):
    # # html页面取数据
    # url = f'https://www.bjnews.com.cn/search?bwsk={keyword}'
    # headers = {
    #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    #     "Accept-Language": "en",
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    # }
    # res = requests.get(url, headers=headers, proxies=get_proxies())
    # html = etree.HTML(res.text)
    # url_list = html.xpath('//h3[@class="articleTitle"]/a/@href')
    # for url in url_list:
    #     try:
    #         get_content(url)
    #     except Exception as e:
    #         print(e)
    #         continue

    # json接口取数据
    url = f'https://s.bjnews.com.cn/bjnews/getlist?from=bw&page=2&orderby=0&bwsk={keyword}'
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    res = requests.get(url, headers=headers, proxies=get_proxies())
    data_list = res.json().get('data', {}).get('data', [])
    if data_list:
        for data in data_list[:2]:
            try:
                url = data.get('_source', {}).get('detail_url', {}).get('pc_url', '')
                get_content(url)
            except Exception as e:
                print(e)


def get_content(url):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    res = requests.get(url, headers=headers, proxies=get_proxies())
    extractor = GeneralNewsExtractor()
    extract_result = extractor.extract(html=res.text, with_body_html=True)
    # 新闻作者
    news_author = extract_result['author']
    # 新闻发布时间
    news_publish_time = extract_result['publish_time']
    # 新闻标题
    news_title_gne = extract_result['title']
    # 新闻内容
    news_content_gne = extract_result['content']
    # 新闻内容 HTML
    news_content_html_gne = extract_result['body_html']
    data = {
        'title': news_title_gne,
        'author': news_author,
        'publish_time': news_publish_time,
        'content': news_content_gne,
        'content_html': news_content_html_gne,
    }
    print(data)


if __name__ == '__main__':
    keyword = '汽车'
    get_url_list(keyword)

