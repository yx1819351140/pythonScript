# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> update_media_info_data         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/24 10:11
# @Software : win10 python3.6
import pymysql
from pymysql.converters import escape_string
import requests
from lxml import etree
from utils.TranslateUtils import BaiduTranslate
"""
更新媒体数据语种和国家英文名
"""

baidu = BaiduTranslate()
proxies = {'http': '192.168.12.180:6666', 'https': '192.168.12.180:6666'}
db = pymysql.connect(host='192.168.12.222', user='root', password='123456', database='data_service', charset='utf8')
cursor = db.cursor()
cursor.execute("select `id`,`domain`,`countryNameZh` from t_media_info")
result = cursor.fetchall()
for i in result:
    id = i[0]
    print(id)
    url = i[1]
    country_name_zh = i[2]
    try:
        res = requests.get('https://' + url, proxies=proxies)
        html = etree.HTML(res.text)
        lang = html.xpath('/html/@lang')[0]
    except:
        lang = ''
    try:
        country_name_en = baidu.translate_2_en(country_name_zh)
    except:
        country_name_en = ''
    cursor.execute(f"update t_media_info set mediaLang='{lang}', countryName='{escape_string(country_name_en)}' where `id`={id}")
db.commit()
cursor.close()
db.close()

