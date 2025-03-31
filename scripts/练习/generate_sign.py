# -*- coding:UTF-8 -*-
# @Time    : 2025/3/31 10:14
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : generate_sign.py
# @Project : pythonScript
# @Software: PyCharm
import hashlib
import time
import urllib.parse
import requests


def generate_api_sign(params, secret_key):
    filtered_paramms = {k: v for k, v in params.items() if v is not None and k!= 'sign'}
    sorted_params = sorted(filtered_paramms.items(), key=lambda x: x[0])
    query_string = '&'.join([f'{k}={v}' for k, v in sorted_params])
    timestamp = str(int(time.time()))

    raw_string = f'{query_string}&timestamp={timestamp}&secret_key={secret_key}'
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest()


def make_signed_request(url, params, secret_key):
    params['timestamp'] = int(time.time())

    sign = generate_api_sign(params, secret_key)
    params['sign'] = sign

    res = requests.get(url, params=params)
    return res.json()


if __name__ == '__main__':
    params = {
        'keyword': '小红书',
        'page': 1,
        'page_size': 20,
        'sort': 'popular',
    }
    SECRET_KEY = 'test_secret_key'
    API_URL = 'https://api.example.com/search'

    result = make_signed_request(API_URL, params, SECRET_KEY)
    print(result)
