# -*- coding:UTF-8 -*-
# @Time    : 24.5.30 10:45
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : resume_routine_load.py
# @Project : pythonScript
# @Software: PyCharm
# coding:utf-8
import pymysql


def resume_routine_load():
    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="ods", charset="utf8", port=9030)
    cursor_doris = db_doris.cursor()

    cursor_doris.execute("""SHOW ROUTINE LOAD FOR ods.ods_pingan_company_punishment_info_creditchina_di;""")
    rows = cursor_doris.fetchall()
    for row in rows:
        label_name = row[[x[0] for x in cursor_doris.description].index('TableName')]
        label_status = row[[x[0] for x in cursor_doris.description].index('State')]
        if label_status == 'State':
            print(f'{label_name} routine load 重启中...')
            cursor_doris.execute(f"""RESUME ROUTINE LOAD FOR {label_name};""")
            print(f'{label_name} 重启完毕！')

    db_doris.close()


if __name__ == '__main__':
    resume_routine_load()
