# -*- coding:UTF-8 -*-
# @Time    : 2024/8/15 13:51
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : test2.py
# @Project : pythonScript
# @Software: PyCharm
import time
import requests
import pymysql
from kafka import KafkaProducer, KafkaConsumer
import json


def get_address_info(address='重庆市两江新区鱼嘴镇长和路65号12-23'):
    try:
        # print(address)
        # encoding:utf-8
        # 根据您选择的AK已为您生成调用代码
        # 检测到您当前的AK设置了IP白名单校验
        # 您的IP白名单中的IP非公网IP，请设置为公网IP，否则将请求失败
        # 请在IP地址为0.0.0.0/0 外网IP的计算发起请求，否则将请求失败

        # 服务地址
        host = "https://api.map.baidu.com"

        # 接口地址
        uri = "/place/v2/suggestion"

        # 此处填写你在控制台-应用管理-创建应用后获取的AK
        ak = "8ymvmahEPpZT3jNPxK3oBS31LJUQYuO8"

        params = {
            "query": address,
            "region": address,
            "city_limit": "true",
            "output": "json",
            "ak": ak,

        }

        response = requests.get(url=host + uri, params=params)
        if response:
            data_list = response.json().get('result', [])
            if data_list:
                for data in data_list:
                    uid = data.get('uid', '')
                    if uid:
                        province = data.get('province', '')
                        city = data.get('city', '')
                        district = data.get('district', '')
                        adcode = data.get('adcode', '')
                        return f'{province}{city}{district}', adcode
        return '', ''
    except:
        return '', ''


def get_address_info1(address):
    try:
        cookies = {
            'PSTM': '1717486581',
            'BIDUPSID': '9C3C36BF544BED968833257BF4AB0604',
            'BDUSS': 'ndXOFZqdGxET0M2cTRMV0lPU3VWa2ZRRmRWNWpnUUUxNWt6Zm5mYnBDYmtaWWRtRVFBQUFBJCQAAAAAAAAAAAEAAABJaj8heXgxODE5MzUxMTQwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOTYX2bk2F9mc',
            'BDUSS_BFESS': 'ndXOFZqdGxET0M2cTRMV0lPU3VWa2ZRRmRWNWpnUUUxNWt6Zm5mYnBDYmtaWWRtRVFBQUFBJCQAAAAAAAAAAAEAAABJaj8heXgxODE5MzUxMTQwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOTYX2bk2F9mc',
            'BAIDUID': 'F3EC97B6825FC9023AA6BFAF29C6FB7B:SL=0:NR=10:FG=1',
            'H_WISE_SIDS': '60622_60677_60676_60672_60572',
            'BAIDUID_BFESS': 'F3EC97B6825FC9023AA6BFAF29C6FB7B:SL=0:NR=10:FG=1',
            'ZFY': 'FCwsZLvf1:Bu5AvtfizlhFBTnYVuHltyCav:Bs015:ASgc:C',
            '__bid_n': '1917dcb1a5d843229a5c49',
            'delPer': '0',
            'routetype': 'bus',
            'H_PS_PSSID': '60622_60677_60676_60672_60572_60684_60700',
            'PSINO': '1',
            'BA_HECTOR': '04ag2g2l80al2h8g202h8la42rr9nh1jd02bq1u',
            'BDORZ': 'B490B5EBF6F3CD402E515D22BCDA1598',
            'BCLID': '11718421405245721845',
            'BCLID_BFESS': '11718421405245721845',
            'BDSFRCVID': 'biPOJexroG3Qs37tFKW2MkFPPyx5pROTDYLEOwXPsp3LGJLVYZ59EG0PtHt5Ixub6j3eogKK0mOTHUkF_2uxOjjg8UtVJeC6EG0Ptf8g0M5',
            'BDSFRCVID_BFESS': 'biPOJexroG3Qs37tFKW2MkFPPyx5pROTDYLEOwXPsp3LGJLVYZ59EG0PtHt5Ixub6j3eogKK0mOTHUkF_2uxOjjg8UtVJeC6EG0Ptf8g0M5',
            'H_BDCLCKID_SF': 'tRAOoC--JKvqKRopMtOhq4tehHRkWT39WDTm_DoyBn8VHUjmQx452xufQR3TBh5DBe62-pPKKR7ADKoa5hCMKRLmDHrq3lTk3mkjbIbGfn02OPKz0TKVX-4syP4j2xRnWNTWKfA-b4ncjRcTehoM3xI8LNj405OTbIFO0KJDJCF5hDIwDjD-DjPVKgTa54cbb4o2WbCQKJTm8pcN2b5oQTtuKN-f-U5iQbruBhQxLPTWMtJ-XpOUWfAkXpJvQnJjt2JxaqRCyxFhbq5jDh3Myb30yq7t5j5BtKTy0hvctn6cShnaMUjrDRLbXU6BK5vPbNcZ0l8K3l02V-bIe-t2XjQh-p52f6DjJJTP',
            'H_BDCLCKID_SF_BFESS': 'tRAOoC--JKvqKRopMtOhq4tehHRkWT39WDTm_DoyBn8VHUjmQx452xufQR3TBh5DBe62-pPKKR7ADKoa5hCMKRLmDHrq3lTk3mkjbIbGfn02OPKz0TKVX-4syP4j2xRnWNTWKfA-b4ncjRcTehoM3xI8LNj405OTbIFO0KJDJCF5hDIwDjD-DjPVKgTa54cbb4o2WbCQKJTm8pcN2b5oQTtuKN-f-U5iQbruBhQxLPTWMtJ-XpOUWfAkXpJvQnJjt2JxaqRCyxFhbq5jDh3Myb30yq7t5j5BtKTy0hvctn6cShnaMUjrDRLbXU6BK5vPbNcZ0l8K3l02V-bIe-t2XjQh-p52f6DjJJTP',
            'ppfuid': 'FOCoIC3q5fKa8fgJnwzbE67EJ49BGJeplOzf+4l4EOvDuu2RXBRv6R3A1AZMa49I27C0gDDLrJyxcIIeAeEhD8JYsoLTpBiaCXhLqvzbzmvy3SeAW17tKgNq/Xx+RgOdb8TWCFe62MVrDTY6lMf2GrfqL8c87KLF2qFER3obJGn/QJMAwAPnqrAi45EBm1RGGEimjy3MrXEpSuItnI4KD8bfH5NJAYbQDlO1QdCB1HdUs7xh/KYe4CwDQofIEpZ9w576stFR8hFaNkWJRUiuCBJsVwXkGdF24AsEQ3K5XBbh9EHAWDOg2T1ejpq0s2eFy9ar/j566XqWDobGoNNfmfpaEhZpob9le2b5QIEdiQez0E9SVndeXkd9EampG0PcXhLZ126CPFCIEuj/nWa+RAuJu9hkfHqEnTsCqvS3DivOXSxFWocn8LvXoXRLp3foHaFYUcWNK9vBxLWe1150xqUgehOYsWQjkg6YqniuoYPHZFO3SaB4GS7zlBrG2cLm8lTRl19JYcYcqvy3P/50mxpWDwUUC4pvKOF9e+pwNq7l6HzKEZyCMUDd+W6AiaksYiu+4AAz72OnMQfgAyNUbW3IyzL5c+UBht87WUigOY9alcIuR+n1gwn+Dmf3unATYGtv0zKmAog3Ny9wFYiQ/gdKSrR9D25HSwrLQyIe5QKTkKSlY6nVev8MhaT3AUPwNqYIvWCQZXWkhuuU0ZXLMYAKJSeHY7mTrwwSSKC3ZaIcOI0j9k46R3D6dNOT5eQrroOB3dqiBQpEuF9Iv7IX5ZK4hiE4AJ3OPZnO3aEJeMB91rSPWb2eeCt263/A+EJVR/A8+3BQ92SIDoXabq8Wb8ZGN9BAsC9g5OdjE6lhwzTadptHqT7mZN901gDzA4lMYEG/kekC+0J5/N5yVy+ei7UKhQHejRjxCO2+98Bn9oaYh/0qmUySClW5mphyLOoygh9eZkqFuuqiNXfbBwU2Yg+9JZ4edxM843Q5xndRbROGVYF8F1hSzrwF+N963ShNGO/mKL64uD0V1BPPvqVoeGBZrZdicvQj/Ixpg4HV9e/xxJNDJS+Dlsbqu3n4I65ukNGXLyEklC4yELuWd/LWcH9vINW2iNXxXOmtVELY74TxxJNDJS+Dlsbqu3n4I65uiZNjgrTAOu3w4UVXnTxXuL0FM/uFy3y/bm3bXiDfzlcmV/10jRpRMa1Wc01DNVRdz6tbH1m8PotlhxmYCxCuj53Swh7Kr1HWCzhMzIFEQ3X4I7OpaIP8n+Lzocjt3hpX',
            'H_WISE_SIDS_BFESS': '60622_60677_60676_60672_60572',
            'RT': '"z=1&dm=baidu.com&si=b82fd43a-4e99-46d0-b191-7856643c4174&ss=m0euuouv&sl=8&tt=77e&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=1a15&ul=2m8v&hd=2m98"',
            'M_LG_UID': '557804105',
            'M_LG_SALT': 'a5fe20e3366a60db050a7756121c39a9',
            'MCITY': '-%3A',
            'validate': '25655',
            'ab_sr': '1.0.1_NTNiZmVmNmQ3Yzc1MDNkYmM0ZjBjNWUyNWYxMzljOWRlY2YxNmM3YWRhMTU5YjMzM2Y2MGE0ZjE4MTE1OTAxOGEwZGI5ZDU5N2M1OWMyNmY1MmViZWZiNmUyY2ZlYmJmZmUwZmUwY2UzNmNhYjZmM2FkYjIxMjE3MWUyZjZjZWQ2N2U1ZjU2OGVmNTExYTIwYTYyOWFmNWE0Y2Q4NzhkZjQxZTg4N2ViZjFmOTY4ZjM3ZDZiMGJkYTQ0MTY1NzBh',
        }

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Acs-Token': '1724850536888_1724910423407_WRtG5z5UQccoQcFZSg82Ej9wJ5ADJz9igxYQdxeQjFMqAI9afiWsGqFBwpkt7sQTMqubcLcfMPRmCahJvU4U3/pNFVZeBgCQZdQw9WpUXMrL9jlOjUWgA6y0a0rkloTZ/kJuXVqz19E+ewHgHhZfmtWYSWjzGza0VUgeoGGokfeTHQq6C1828mBCNGvOPzoaq53Kjhdtc3BFafmEN6bl53VMNP+b20mZ2SzRRnCv63ECDVI9rVTwy9gJgkvlKGTP/j72BlumeJQbE1giErMHI4kPTE/yzDJaldZYQu82+v2FPvtXpVPHs8sVxrNSr5r6h7aKDKeK569MFWWVxADgyzxk0W+T8eDJWcbHa4fsJoZ0cvfb8aZRnoN7Utq/vyeoLG1YMAub3VvVRf2HhcqX3bcMMzPWA6zckOhFj/6Sz+1AZ1mJWUQeHfdixjMDEKTW3krh4m7/i58MplTHtA3L1goKlTlDpc1uV2qDb8rqdYOnk/pXKYt3Jf9EzVnfKmVO',
            'Connection': 'keep-alive',
            # 'Cookie': 'PSTM=1717486581; BIDUPSID=9C3C36BF544BED968833257BF4AB0604; BDUSS=ndXOFZqdGxET0M2cTRMV0lPU3VWa2ZRRmRWNWpnUUUxNWt6Zm5mYnBDYmtaWWRtRVFBQUFBJCQAAAAAAAAAAAEAAABJaj8heXgxODE5MzUxMTQwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOTYX2bk2F9mc; BDUSS_BFESS=ndXOFZqdGxET0M2cTRMV0lPU3VWa2ZRRmRWNWpnUUUxNWt6Zm5mYnBDYmtaWWRtRVFBQUFBJCQAAAAAAAAAAAEAAABJaj8heXgxODE5MzUxMTQwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOTYX2bk2F9mc; BAIDUID=F3EC97B6825FC9023AA6BFAF29C6FB7B:SL=0:NR=10:FG=1; H_WISE_SIDS=60622_60677_60676_60672_60572; BAIDUID_BFESS=F3EC97B6825FC9023AA6BFAF29C6FB7B:SL=0:NR=10:FG=1; ZFY=FCwsZLvf1:Bu5AvtfizlhFBTnYVuHltyCav:Bs015:ASgc:C; __bid_n=1917dcb1a5d843229a5c49; delPer=0; routetype=bus; H_PS_PSSID=60622_60677_60676_60672_60572_60684_60700; PSINO=1; BA_HECTOR=04ag2g2l80al2h8g202h8la42rr9nh1jd02bq1u; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; BCLID=11718421405245721845; BCLID_BFESS=11718421405245721845; BDSFRCVID=biPOJexroG3Qs37tFKW2MkFPPyx5pROTDYLEOwXPsp3LGJLVYZ59EG0PtHt5Ixub6j3eogKK0mOTHUkF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; BDSFRCVID_BFESS=biPOJexroG3Qs37tFKW2MkFPPyx5pROTDYLEOwXPsp3LGJLVYZ59EG0PtHt5Ixub6j3eogKK0mOTHUkF_2uxOjjg8UtVJeC6EG0Ptf8g0M5; H_BDCLCKID_SF=tRAOoC--JKvqKRopMtOhq4tehHRkWT39WDTm_DoyBn8VHUjmQx452xufQR3TBh5DBe62-pPKKR7ADKoa5hCMKRLmDHrq3lTk3mkjbIbGfn02OPKz0TKVX-4syP4j2xRnWNTWKfA-b4ncjRcTehoM3xI8LNj405OTbIFO0KJDJCF5hDIwDjD-DjPVKgTa54cbb4o2WbCQKJTm8pcN2b5oQTtuKN-f-U5iQbruBhQxLPTWMtJ-XpOUWfAkXpJvQnJjt2JxaqRCyxFhbq5jDh3Myb30yq7t5j5BtKTy0hvctn6cShnaMUjrDRLbXU6BK5vPbNcZ0l8K3l02V-bIe-t2XjQh-p52f6DjJJTP; H_BDCLCKID_SF_BFESS=tRAOoC--JKvqKRopMtOhq4tehHRkWT39WDTm_DoyBn8VHUjmQx452xufQR3TBh5DBe62-pPKKR7ADKoa5hCMKRLmDHrq3lTk3mkjbIbGfn02OPKz0TKVX-4syP4j2xRnWNTWKfA-b4ncjRcTehoM3xI8LNj405OTbIFO0KJDJCF5hDIwDjD-DjPVKgTa54cbb4o2WbCQKJTm8pcN2b5oQTtuKN-f-U5iQbruBhQxLPTWMtJ-XpOUWfAkXpJvQnJjt2JxaqRCyxFhbq5jDh3Myb30yq7t5j5BtKTy0hvctn6cShnaMUjrDRLbXU6BK5vPbNcZ0l8K3l02V-bIe-t2XjQh-p52f6DjJJTP; ppfuid=FOCoIC3q5fKa8fgJnwzbE67EJ49BGJeplOzf+4l4EOvDuu2RXBRv6R3A1AZMa49I27C0gDDLrJyxcIIeAeEhD8JYsoLTpBiaCXhLqvzbzmvy3SeAW17tKgNq/Xx+RgOdb8TWCFe62MVrDTY6lMf2GrfqL8c87KLF2qFER3obJGn/QJMAwAPnqrAi45EBm1RGGEimjy3MrXEpSuItnI4KD8bfH5NJAYbQDlO1QdCB1HdUs7xh/KYe4CwDQofIEpZ9w576stFR8hFaNkWJRUiuCBJsVwXkGdF24AsEQ3K5XBbh9EHAWDOg2T1ejpq0s2eFy9ar/j566XqWDobGoNNfmfpaEhZpob9le2b5QIEdiQez0E9SVndeXkd9EampG0PcXhLZ126CPFCIEuj/nWa+RAuJu9hkfHqEnTsCqvS3DivOXSxFWocn8LvXoXRLp3foHaFYUcWNK9vBxLWe1150xqUgehOYsWQjkg6YqniuoYPHZFO3SaB4GS7zlBrG2cLm8lTRl19JYcYcqvy3P/50mxpWDwUUC4pvKOF9e+pwNq7l6HzKEZyCMUDd+W6AiaksYiu+4AAz72OnMQfgAyNUbW3IyzL5c+UBht87WUigOY9alcIuR+n1gwn+Dmf3unATYGtv0zKmAog3Ny9wFYiQ/gdKSrR9D25HSwrLQyIe5QKTkKSlY6nVev8MhaT3AUPwNqYIvWCQZXWkhuuU0ZXLMYAKJSeHY7mTrwwSSKC3ZaIcOI0j9k46R3D6dNOT5eQrroOB3dqiBQpEuF9Iv7IX5ZK4hiE4AJ3OPZnO3aEJeMB91rSPWb2eeCt263/A+EJVR/A8+3BQ92SIDoXabq8Wb8ZGN9BAsC9g5OdjE6lhwzTadptHqT7mZN901gDzA4lMYEG/kekC+0J5/N5yVy+ei7UKhQHejRjxCO2+98Bn9oaYh/0qmUySClW5mphyLOoygh9eZkqFuuqiNXfbBwU2Yg+9JZ4edxM843Q5xndRbROGVYF8F1hSzrwF+N963ShNGO/mKL64uD0V1BPPvqVoeGBZrZdicvQj/Ixpg4HV9e/xxJNDJS+Dlsbqu3n4I65ukNGXLyEklC4yELuWd/LWcH9vINW2iNXxXOmtVELY74TxxJNDJS+Dlsbqu3n4I65uiZNjgrTAOu3w4UVXnTxXuL0FM/uFy3y/bm3bXiDfzlcmV/10jRpRMa1Wc01DNVRdz6tbH1m8PotlhxmYCxCuj53Swh7Kr1HWCzhMzIFEQ3X4I7OpaIP8n+Lzocjt3hpX; H_WISE_SIDS_BFESS=60622_60677_60676_60672_60572; RT="z=1&dm=baidu.com&si=b82fd43a-4e99-46d0-b191-7856643c4174&ss=m0euuouv&sl=8&tt=77e&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=1a15&ul=2m8v&hd=2m98"; M_LG_UID=557804105; M_LG_SALT=a5fe20e3366a60db050a7756121c39a9; MCITY=-%3A; validate=25655; ab_sr=1.0.1_NTNiZmVmNmQ3Yzc1MDNkYmM0ZjBjNWUyNWYxMzljOWRlY2YxNmM3YWRhMTU5YjMzM2Y2MGE0ZjE4MTE1OTAxOGEwZGI5ZDU5N2M1OWMyNmY1MmViZWZiNmUyY2ZlYmJmZmUwZmUwY2UzNmNhYjZmM2FkYjIxMjE3MWUyZjZjZWQ2N2U1ZjU2OGVmNTExYTIwYTYyOWFmNWE0Y2Q4NzhkZjQxZTg4N2ViZjFmOTY4ZjM3ZDZiMGJkYTQ0MTY1NzBh',
            'Referer': 'https://map.baidu.com/@11884636.125,3435440,19z',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        # address = '山东省威海市经济技术开发区黄岛路-1-1号'
        response = requests.get(
            'https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd=替换地址&c=132&src=0&wd2=&pn=0&sug=0&l=19&b=(11884316.125,3435300.25;11884956.125,3435579.75)&from=webmap&biz_forward={%22scaler%22:2,%22styles%22:%22pl%22}&sug_forward=&auth=RTG4VWNbXW7U54%3D54c0V1wUDFJ0yNd00uxNzETxVEzHt2OF%3D2UUF9FDFCwyS8v7uvkGcuVtvvhguVtvyheuVtvCMGuVtvCQMuVtvIPcuVtvYvjuVtvZgMuVtv%40vcuVtvc3CuVtcvY1SGpuxBt2o88GdFveGvuVtveh3uxtwiKDv7uvhgMuxVVtvrMhuVtGccZcuxtf0wd0vyOAFUy7FAI&seckey=C5HY%2F2XzySHqt3BGtBTgM8RTNLhRAr3YvXAa0s3daZ8%3D%2CfjvWt4Zn9rZe_NRN5j6ZlpP7jeQx1LC84Y7Y2EeJuChO59IiRZn38zYABmOdFppcKZlJLlIPF_BwvglEwW-Sc9HYMLxI5Jn59KjpTKBVsf8NWid8HbLv2D7hhbFEEfjMUyqwp5WD4X22ORF3jQTnp8FcYwO7QcZfy3LPUrYFVUYnDQr89lHAblVZN2T1EWeS&device_ratio=2&tn=B_NORMAL_MAP&nn=0&u_loc=12964711,4826154&ie=utf-8&t=1724910423385&newfrom=zhuzhan_webmap'.replace(
                '替换地址', address),
            cookies=cookies,
            headers=headers,
        )
        content = response.json()['content']
        for data in content:
            try:
                admin_info = data['admin_info']
                province_name = admin_info['province_name']
                city_name = admin_info['city_name']
                area_name = admin_info['area_name']
                area_id = admin_info['area_id']
                return f'{province_name}{city_name}{area_name}', area_id
            except:
                continue
    except Exception as e:
        # print(response.text)
        time.sleep(1)
    return '', ''


def get_data_from_doris():
    db_doris = pymysql.connect(host="10.32.48.61", user="yangxin", password="yangxinQ123", database="qd", charset="utf8")
    cursor_doris = db_doris.cursor()
    cursor_doris.execute("""SELECT 
        entid,
        case 
        when label = '迁入' THEN content_before
        else content_after end
        address
FROM `pro_qd_ztjc__pro_qd_dp_list_migrateinout_all` where c1=''""")
    data_list = cursor_doris.fetchall()
    for data in data_list:
        entid = data[0]
        address = data[1]
        migrate_name, migrate_code = get_address_info(address.replace('(', '').replace(')', '')[:20])
        print(id, address, migrate_name, migrate_code)
        sql = f"update pro_qd_ztjc__pro_qd_dp_list_migrateinout_all set migrate_name='{migrate_name}', migrate_code='{migrate_code}' where entid={entid}"
        # print(sql)
        cursor_doris.execute(sql)
        db_doris.commit()


def get_data_from_kafka():
    kafka_producer = KafkaProducer(
        bootstrap_servers=['10.32.50.101:9092', '10.32.50.102:9092', '10.32.50.103:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 将消息序列化为 JSON 格式
    )
    kafka_consumer = KafkaConsumer(
        'doris_qd_list_migrateinout',
        bootstrap_servers=['10.32.50.101:9092', '10.32.50.102:9092', '10.32.50.103:9092'],
        enable_auto_commit=True,
        group_id='doris_qd_list_migrateinout1',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    for message in kafka_consumer:
        try:
            data = json.loads(message.value)
            entid = data.get('entid', '')
            entname = data.get('entname', '')
            uniscid = data.get('uniscid', '')
            label = data.get('label', '')
            # migrate_code = data.get('migrate_code', '')
            # migrate_name = data.get('migrate_name', '')
            migrate_date = data.get('migrate_date', '')
            content_before = data.get('content_before', '')
            content_after = data.get('content_after', '')
            create_time = data.get('create_time', '')

            address = content_before if label == '迁入' else content_after
            migrate_name, migrate_code = get_address_info1(address.replace('(', '').replace(')', '')[:20])
            # print(entid, address, migrate_name, migrate_code)

            result = {
                "entid": entid,
                "entname": entname,
                "uniscid": uniscid,
                "label": label,
                "migrate_code": migrate_code,
                "migrate_name": migrate_name,
                "migrate_date": migrate_date,
                "content_before": content_before,
                "content_after": content_after,
                "create_time": create_time
            }
            kafka_producer.send(f'doris_qd_list_migrateinout_result', value=result)
            # print(result)
        except Exception as e:
            print(e)
            continue


if __name__ == '__main__':
    # get_data_from_kafka()
    print(get_address_info1('山东省青岛市莱西市河头店镇小莱路6号南工业园1145号 '))
