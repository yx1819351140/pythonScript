# -*- coding:UTF-8 -*-
# @Time    : 2024/10/23 09:50
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : common_utils.py
# @Project : pythonScript
# @Software: PyCharm
import datetime
import json
import pymysql
import redis
import random
import os
import time
from kafka import KafkaProducer
from pprint import pprint
from Crypto.Cipher import AES


def table2kafka(table, database):
    key_list = execute_sql(sql=f"SELECT group_concat(COLUMN_NAME)FROM information_schema.columns WHERE table_name='{table}'", database=f'{database}', tidb=True)[0][0].split(',')
    for tup in execute_sql(sql=f'SELECT * FROM {database}.{table};', database=f'{database}', tidb=True):
        value_list = list(tup)
        dd = dict(zip(key_list, value_list))
        try:
            dd['spider_time'] = str(dd['create_time'])
            dd.pop('update_time')
            dd.pop('create_time')
        except:
            dd['spider_time'] = str(dd['created'])
            dd.pop('updated')
            dd.pop('created')
        print(dd)
        push_kafka(topic='collect_' + table, dict=dd)
        time.sleep(0.01)


def push_kafka(topic, dict):
    producer = KafkaProducer(bootstrap_servers='10.32.50.103:9092')
    producer.send(topic, value=json.dumps(dict, ensure_ascii=False).encode('utf8'))
    # producer.send(topic, value=json.dumps(dict).encode('utf8'))
    producer.flush()


def generate_ddl(table, dict):
    string = ''
    for key, value in dict.items():
        string = string + "`{}` varchar(512) DEFAULT NULL COMMENT '{}',\n".format(key, key)
    ddl_sql = """CREATE TABLE `{}` (\n`id` int(10) NOT NULL AUTO_INCREMENT,\n""".format(table) + string + \
          """`create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n`update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON 定时更新 CURRENT_TIMESTAMP COMMENT '更新时间',\nPRIMARY KEY (`id`),\nKEY `{}_create_time_IDX` (`create_time`),\nKEY `{}_update_time_IDX` (`update_time`)\n) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='{}'; """.format(table, table, table)


    return ddl_sql


def generate_sql(table, dict):
    key_string, value_string = '', ''
    for key, value in dict.items():
        if key_string and value_string:
            key_string = key_string + ', ' + key
            if value == None:
                value_string = value_string + ', ' + 'Null'
            else:
                value_string = value_string + ', "' + str(value).replace('"', "'") + '"'
        else:
            key_string = key
            if value == None:
                value_string = 'Null'
            else:
                value_string = '"' + str(value).replace('"', "'") + '"'
    return """insert into {}({}) values({});""".format(table, key_string, value_string)


def execute_sql(tidb, database, sql):
    try:
        if tidb:
            db = pymysql.connect(host="10.32.51.152", port=4000, user='tidb', password="tidb", database=database, charset='utf8')
        else:
            db = pymysql.connect(host="10.32.49.61", port=9030, user='yangxin', password="yangxinQ123", database=database, charset='utf8')
        cursor = db.cursor()
        cursor.execute(sql)
        db.commit()
        result = cursor.fetchall()
        cursor.close()
        db.close()
        return result
    except BaseException:
        print('---------------------', sql)


def get_proxies(name):
    while True:
        try:
            r = redis.Redis(connection_pool=redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True))
            proxies_list = r.zrange(name, 0, -1)

            if proxies_list == []:
                time.sleep(5)
            else:
                ip = proxies_list[random.randint(0, len(proxies_list)-1)]
                # return {"http": ip, "https": ip}
                return {"http": 'http://' + ip, "https": 'http://' + ip}

        except BaseException: time.sleep(1)


def get_proxies_num(name):
    while True:
        try:
            r = redis.Redis(connection_pool=redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True))
            proxies_list = r.zrange(name, 0, -1)

            if proxies_list == []:
                time.sleep(5)
            else: return len(proxies_list)

        except BaseException: time.sleep(1)


def run_py(file, day, hour):
    hour = int(time.strftime("%H", time.localtime()))
    os.system('python3 {}'.format(file))
    time.sleep(60*60*24*day)
    date = datetime.date.today()
    hour = int(time.strftime("%H", time.localtime()))
    print(date, hour)


class AESDecrypt:
    iv = '0123456789ABCDEF'
    key = 'jo8j9wGw%6HbxfFn'

    @classmethod
    def _pkcs7unpadding(cls, text):
        """
        处理使用PKCS7填充过的数据
        :param text: 解密后的字符串
        :return:
        """
        length = len(text)
        unpadding = ord(text[length - 1])
        return text[0:length - unpadding]

    @classmethod
    def decrypt(cls, content):
        """
        AES解密，模式cbc，去填充pkcs7
        :param content: 16进制编码的加密字符串
        :return: 返回解密后的字符串
        """
        key = bytes(cls.key, encoding='utf-8')
        iv = bytes(cls.iv, encoding='utf-8')
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypt_bytes = cipher.decrypt(bytes.fromhex(content))
        result = str(decrypt_bytes, encoding='utf-8')
        result = cls._pkcs7unpadding(result)
        return result


if __name__ == '__main__':
    table2kafka('enqa_real_estate_development', 'acq_com_gs')
