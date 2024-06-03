# -*- coding:UTF-8 -*-
# @Time    : 24.5.30 10:45
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : monitor_doris.py
# @Project : pythonScript
# @Software: PyCharm
# coding:utf-8
import pymysql
import requests
import time


webhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=224682e9-c0b2-4992-8055-622b09919574'
headers = {
    'Content-Type': 'application/json',
}


def monitor_routine_load():
    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="ods", charset="utf8", port=9030)
    cursor_doris = db_doris.cursor()

    cursor_doris.execute("""SHOW ROUTINE LOAD""")
    rows = cursor_doris.fetchall()
    content = ''
    for row in rows:
        label_name = row[[x[0] for x in cursor_doris.description].index('TableName')]
        label_status = row[[x[0] for x in cursor_doris.description].index('State')]
        if label_status != 'RUNNING':
            content += f'{label_name} is {label_status}\n'
            # try:
            #     cursor_doris.execute(f'RESUME ROUTINE LOAD FOR {label_name}')
            # except:
            #     continue
    if content:
        content += f'当前时间：{time.strftime("%Y-%m-%d %H:%M:%S")}'
        json_data = {
            'msgtype': 'text',
            'text': {
                'content': content,
            },
        }
        requests.post(webhook, headers=headers, json=json_data)

    db_doris.close()


if __name__ == '__main__':
    monitor_routine_load()
