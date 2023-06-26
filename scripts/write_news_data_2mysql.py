# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> write_news_data_2mysql         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/6/7 9:15
# @Software : win10 python3.6
import json
import time
from readability import Document
from gne import GeneralNewsExtractor
import pymysql
from pymysql.converters import escape_string
from selenium.webdriver import ChromeOptions, Chrome
from utils.CommonUtils import md5_string
from random import randint


def write_data(url, media, news_type, related_entities, img_name):
    options = ChromeOptions()
    options.add_argument(f'--proxy-server=http://192.168.12.180:6666')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = Chrome(options=options)

    driver.get(url)
    time.sleep(3)
    response = driver.page_source
    extractor = GeneralNewsExtractor()
    extract_result = extractor.extract(html=response, with_body_html=True)
    title = escape_string(extract_result['title'])
    title_id = md5_string(title)
    content = escape_string(extract_result['content'])
    create_time = time.strftime('%Y-%m-%d %H:%M:%S')
    pub_time = extract_result['publish_time']
    if not pub_time:
        pub_time = create_time
    else:
        if len(pub_time) < 18:
            pub_time = create_time
        else:
            pub_time = pub_time[:19].replace('T', ' ')
    score = 9 + randint(0, 9) / 10

    db = pymysql.Connect(host='192.168.12.240', port=3306, user='root', password='123456', database='global_event', charset='utf8')
    cursor = db.cursor()
    sql = f"""
        insert into theme_uas_news (title_id,title,content,pub_time,media,create_time,url,news_img_url,news_type,related_entities,doc_type,`desc`,score) values ('{title_id}','{title}','{content}','{pub_time}','{media}','{create_time}','{url}','{img_name}','{news_type}', '{related_entities}','text','{escape_string(content[:200])}',{score})
    """
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()

    driver.close()
    print(f'{title_id} 写入成功')


if __name__ == '__main__':
    news_type_list = ['general', 'technology', 'purchase', 'exercise']
    url = 'https://www.dvidshub.net/news/365959/tethered-aerostat-radar-system-optimization'
    media = 'dvidshub'
    news_type = news_type_list[1]
    related_entities = json.dumps(["A2_6"])
    img_name = '145.png'
    write_data(url, media, news_type, related_entities, img_name)
