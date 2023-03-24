# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> TranslateUtils         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/24 11:21
# @Software : win10 python3.6
import random
import requests
from settings import PROXY
from utils.CommonUtils import md5_string
import json


class BaiduTranslate(object):
    def __init__(self):
        self.app_id = '20230324001612644'
        self.key = 'chG18fmNB_boIhrLFTX9'
        self.salt = ''.join(str(random.choice(range(10))) for _ in range(10))

    def translate(self, text, to):
        self.sign = md5_string(self.app_id + text + self.salt + self.key)
        url = f'http://api.fanyi.baidu.com/api/trans/vip/translate?q={text}&from=auto&to={to}&appid={self.app_id}&salt={self.salt}&sign={self.sign}'
        res = requests.get(url, proxies=PROXY)
        dict_data = json.loads(res.text)
        return dict_data['trans_result'][0]['dst']

    # 自动解析语言，翻译成中文
    def translate_2_zh(self, text):
        return self.translate(text, 'zh')

    # 自动解析语言，翻译成中文
    def translate_2_en(self, text):
        return self.translate(text, 'en')


if __name__ == '__main__':
    baidu = BaiduTranslate()
    print(baidu.translate_2_en('智利'))
