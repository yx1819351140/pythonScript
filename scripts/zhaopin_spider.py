# -*- coding:UTF-8 -*-
# @Time    : 24.11.19 09:23
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : zhaopin_spider.py
# @Project : pythonScript
# @Software: PyCharm
import requests
from lxml import etree
import pymysql
from pymysql.converters import escape_string
import time
import redis
import random

r = redis.Redis(connection_pool=redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True))

proxy = None


def get_proxies():
    proxies_list = r.zrange("WholeJuLiangDaiLi", 0, -1)
    ip = proxies_list[random.randint(0, len(proxies_list) - 1)].split('@')[1]
    return {"http": f'http://{ip}', "https": f'http://{ip}'}


def get_response(url, headers):
    while True:
        global proxy
        try:
            response = requests.get(url, headers=headers, proxies=proxy, timeout=10)
            if '您的IP地址存在异常行为' in response.text or response.status_code >= 300:
                print(f'{url} 采集失败，重试中，当前代理：{proxy}')
                proxy = get_proxies()
                print(f'代理ip更换成功，当前代理：{proxy}')
                time.sleep(0.5)
            else:
                return response
        except Exception as e:
            print(f'{url} 采集失败，重试中，当前代理：{proxy}')
            proxy = get_proxies()
            print(f'代理ip更换成功，当前代理：{proxy}')
            time.sleep(0.5)


def get_intro_from_boss(company_name):
    url = f'https://www.zhipin.com/wapi/zpgeek/miniapp/search/brandlist.json?pageSize=20&query={company_name}&city=101010100&source=1&sortType=0&subwayLineId=&subwayStationId=&districtCode=&businessCode=&longitude=&latitude=&position=&expectId=&expectPosition=&page=1&appId=10002'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090c11)XWEB/11275',
        'Content-Type': 'application/x-www-form-urlencoded',
        'mpt': '7850d81363e0041881d1db7bd9f034a7',
    }
    res = get_response(url, headers=headers)
    brand_list = res.json().get('zpData').get('brandList')
    if brand_list:
        company_info = brand_list[0]
        brand_id = company_info.get('encryptBrandId')
        url = f'https://www.zhipin.com/wapi/zpgeek/miniapp/brand/detail.json?brandId={brand_id}&appId=10002'
        res = get_response(url, headers=headers)
        try:
            intro = res.json().get('zpData').get('introduce')
        except:
            intro = ''
        return intro
    else:
        print(f'{company_name} boss直聘检索结果为空！')
        return ''


def get_intro_from_jobui(company_name):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        # 'Cookie': 'cmp_visit_limit=%5B%2257781f2edf5f58ecc22e7ba9a2153f74%22%5D; Hm_lvt_8b3e2b14eff57d444737b5e71d065e72=1731980932; Hm_lpvt_8b3e2b14eff57d444737b5e71d065e72=1731980932; HMACCOUNT=3E9EA5CAFDF26388; AXUDE_jobuiinfo=XZuun4rOzX; jobui_p=1731980932577_42390981; TN_VisitCookie=1; TN_VisitNum=1',
        'Referer': 'https://www.jobui.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
    }

    res = get_response(f'https://m.jobui.com/cmp?area=全国&keyword={company_name}', headers=headers)
    html = etree.HTML(res.text)
    company_id_list = html.xpath('//a[@class="company-name"]/@data-companyid')
    if company_id_list:
        company_id = company_id_list[0]
        url = f'https://m.jobui.com/company/{company_id}/'
        res = get_response(url, headers=headers)
        html = etree.HTML(res.text)
        temp_data_list = html.xpath('//p[@class="j-content-box"]//text()')
        result = ''
        for temp_data in temp_data_list:
            if temp_data.strip():
                result = result + temp_data.strip()
        return result.strip()
    else:
        print(f'{company_name} 职友集检索结果为空！')
        return ''


def write_data():
    db_doris = pymysql.connect(host="10.32.49.61", port=9030, user="yangxin", password="yangxinQ123", database="tmp", charset="utf8")
    cursor_doris = db_doris.cursor()

    cursor_doris.execute("""SELECT company_id,entname from tmp.company_web_info_20241119 where company_id not in (select company_id from tmp.company_web_info_boss)""")
    data_list = cursor_doris.fetchall()

    for data in data_list:
        company_id = data[0]
        company_name = data[1]
        i = data_list.index(data)
        intro = escape_string(get_intro_from_boss(company_name))
        intro1 = escape_string(get_intro_from_jobui(company_name))

        cursor_doris.execute(f"""insert into tmp.company_web_info_boss (company_id, entname, intro) values (%s, '%s', '%s');""" % (company_id, company_name, intro))
        cursor_doris.execute(f"""insert into tmp.company_web_info_jobui (company_id, entname, intro) values (%s, '%s', '%s');""" % (company_id, company_name, intro1))

        print(f'{i+1} {company_name} 公司简介采集完毕！')
    db_doris.commit()
    db_doris.close()


if __name__ == '__main__':
    write_data()
    # print(get_intro_from_boss('上海名博投资有限公司'))
