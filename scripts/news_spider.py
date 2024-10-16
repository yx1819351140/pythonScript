import requests
import hashlib
import json
import random
from time import sleep
import redis
import requests
from gne import GeneralNewsExtractor

r = redis.Redis(connection_pool=redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True))


def get_proxies():
    proxies_list = r.zrange("JuLiangTimeM0T1", 0, -1)
    ip = proxies_list[random.randint(0, len(proxies_list) - 1)]
    return {"http": f'http://{ip}', "https": f'http://{ip}'}


class NewsSpider(object):
    # 主题词
    keywords = ["儿童意外伤害", "儿童忽视"]
    # 细分主题词：儿童
    child_keywords = ["孩子", "儿童", "女孩", "男孩", "女童", "男童", "孤儿", "幼儿", "小学生", "中学生", "高中生",
                      "1岁", "2岁", "3岁", "4岁", "5岁", "6岁", "7岁", "8岁", "9岁", "10岁", "11岁", "12岁", "13岁",
                      "14岁", "15岁", "16岁", "17岁", "不满18岁"]
    # 细分主题词：伤害程度
    injury_severity_keywords = ["无受伤", "轻度受伤", "中度受伤", "重度受伤", "死亡", "失踪", "重伤", "轻伤"]
    # 细分主题词：伤害类型
    injury_type_keywords = ["被拐", "烫伤", "烧伤", "掉水里", "掉河里", "溺水", "溺亡", "车祸", "事故", "交通事故",
                            "吞食", "不小心", "一不留神", "走失", "坠楼", "坠落", "强奸", "强制猥亵", "被打", "跌落",
                            "摔伤", "电器", "电击", "被电", "磕碰", "割伤", "划伤", "意外伤害", "意外受伤", "玩火",
                            "放炮"]

    def __init__(self):
        self.bjnews = open("bjnews.txt", "a", encoding="UTF-8")
        self.thepaper = open("thepaper.txt", "a", encoding="UTF-8")
        self.infzm = open("infzm.txt", "a", encoding="UTF-8")
        self.proxies = get_proxies()
        self.news_hash = {}

    def bjnews_spider(self, keyword):
        print(keyword)
        page = 1
        while 1:
            # json接口取数据
            url = f'https://s.bjnews.com.cn/bjnews/getlist?from=bw&page={page}&orderby=0&bwsk={keyword}'
            res = self.get_response(url)
            data = res.json().get('data', {})
            if not data:
                break
            data_list = data.get('data', [])
            if data_list:
                for data in data_list:
                    try:
                        detail_url = data.get('_source', {}).get('detail_url', {}).get('pc_url', '')
                        publish_time = data.get('_source', {}).get('publish_time', '')
                        title = data.get('_source', {}).get('title', '')
                        content = data.get('highlight', {}).get('content', '')
                        title_hash = hashlib.md5(title.encode("UTF-8")).hexdigest()
                        if not r.sismember('title_hash', title_hash):
                            data = {
                                'keyword': keyword,
                                'title': title,
                                'publish_time': publish_time,
                                'content': content,
                                'url': detail_url
                            }
                            print(data)
                            self.bjnews.write(json.dumps(data, ensure_ascii=False) + '\n')
                            r.sadd('title_hash', title_hash)
                    except Exception as e:
                        print(e)
                page += 1
                print(f'{url} 采集完毕')
            else:
                break

    def thepaper_spider(self, keyword):
        print(keyword)
        page = 1
        while 1:
            # json接口取数据
            url = f'https://api.thepaper.cn/search/web/news'
            data = {"word": keyword, "orderType": 3, "pageNum": page, "pageSize": 10, "searchType": 1}
            res = self.get_response(url, data=data)
            data_list = res.json().get('data', {}).get('list', [])
            if not data_list:
                break
            for data in data_list:
                try:
                    news_id = data.get('contId', '')
                    if news_id and not r.sismember('title_hash', f'thepaper_{news_id}'):
                        self.get_thepaper_content(news_id, keyword)
                        r.sadd('title_hash', f'thepaper_{news_id}')
                except Exception as e:
                    print(e)
            page += 1
            print(f'{url} 采集完毕')

    def get_thepaper_content(self, news_id, keyword):
        try:
            url = f'https://www.thepaper.cn/newsDetail_forward_{news_id}'
            res = self.get_response(url)
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
                'keyword': keyword,
                'title': news_title_gne,
                'author': news_author,
                'publish_time': news_publish_time,
                'content': news_content_gne,
                'content_html': news_content_html_gne
            }
            # 数据写入文件
            print(data)
            self.thepaper.write(json.dumps(data, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f'[get_thepaper_content]{e}')

    def infzm_spider(self, keyword):
        print(keyword)
        page = 1
        while 1:
            # json接口取数据
            url = f'https://www.infzm.com/search?term_id=&page={page}&k={keyword}&format=json'
            res = self.get_response(url)
            data_list = res.json().get('data', {}).get('list', [])
            if not data_list:
                break
            for data in data_list:
                try:
                    news_id = data.get('id', '')
                    if news_id and not r.sismember('title_hash', f'infzm_{news_id}'):
                        self.get_infzm_content(news_id, keyword)
                        r.sadd('title_hash', f'infzm_{news_id}')
                except Exception as e:
                    print(e)
            page += 1
            print(f'{url} 采集完毕')

    def get_infzm_content(self, news_id, keyword):
        try:
            url = f'https://www.infzm.com/contents/{news_id}'
            res = self.get_response(url)
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
                'keyword': keyword,
                'title': news_title_gne,
                'author': news_author,
                'publish_time': news_publish_time,
                'content': news_content_gne,
                'content_html': news_content_html_gne
            }
            # 数据写入文件
            print(data)
            self.infzm.write(json.dumps(data, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f'[get_infzm_content]{e}')

    def get_response(self, url, data=None):
        while 1:
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
            }
            try:
                if not data:
                    res = requests.get(url, headers=headers, proxies=self.proxies, timeout=10)
                else:
                    res = requests.post(url, json=data, headers=headers, proxies=self.proxies, timeout=10)
                if res.status_code == 200:
                    return res
                else:
                    new_proxy = get_proxies()
                    print(f'{self.proxies} 已失效, 更换代理 {new_proxy}')
                    self.proxies = get_proxies()
            except Exception as e:
                print(e)
                new_proxy = get_proxies()
                print(f'{self.proxies} 已失效, 更换代理 {new_proxy}')
                self.proxies = get_proxies()

    def __del__(self):
        self.bjnews.close()
        self.thepaper.close()
        self.infzm.close()

    def get_content(self, keyword):
        self.bjnews_spider(keyword)
        # self.thepaper_spider(keyword)
        # self.infzm_spider(keyword)

    def run(self):
        for keyword in self.keywords:
            self.get_content(keyword)
            sleep(2)
            for child_keyword in self.child_keywords:
                self.get_content(f'{keyword}%20{child_keyword}')
                sleep(2)
                for injury_severity_keyword in self.injury_severity_keywords:
                    self.get_content(f'{keyword}%20{child_keyword}%20{injury_severity_keyword}')
                    sleep(2)
                for injury_type_keyword in self.injury_type_keywords:
                    self.get_content(f'{keyword}%20{child_keyword}%20{injury_type_keyword}')
                    sleep(2)


if __name__ == '__main__':
    news_spider = NewsSpider()
    news_spider.run()
