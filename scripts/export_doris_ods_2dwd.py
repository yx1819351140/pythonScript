# -*- coding:UTF-8 -*-
# @Time    : 24.5.17 10:45
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : export_doris_ods_2dwd.py
# @Project : pythonScript
# @Software: PyCharm
# coding:utf-8

import time
from datetime import datetime, timedelta
import pymysql


def ods2dwd():
    # 创建链接
    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="dwd", charset="utf8",
                               port=9030)
    cursor_doris = db_doris.cursor()

    begintime = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("开始本次dwd表工作:", begintime)
    # 创建分区字段
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    dt_partition = yesterday.strftime("%Y%m%d")

    #  设置开始循环，结束循环
    t_step_start = 0
    t_step_end = 1000

    while t_step_start < t_step_end:
        insert_dwd_sql = f"""INSERT INTO dwd.dwd_fp_jztk_mid
SELECT
    a.id,
    a.fp_id,
    a.class_no,
    a.pro_name,
    a.entid_g,
    a.company_id_g,
    a.company_name_g,
    c_g.nic_code AS nic_code_g,
    c_g.reg_capital_amt AS reg_capital_amt_g,
    c_g.es_dt AS es_dt_g,
    c_g.scale_code AS scale_code_g,
    a.entid_x,
    a.company_id_x,
    a.company_name_x,
    c_x.nic_code AS nic_code_x,
    a.etl_time
FROM
    dwd.dwd_fp_combined_details a
LEFT JOIN
    dwd.dwd_gs_company_di c_g
ON
    a.company_id_g = c_g.company_id
LEFT JOIN
    dwd.dwd_gs_company_di c_x
ON
    a.company_id_x = c_x.company_id
where
    a.id % 1000 = {t_step_start}
"""
        #  and a.id > {t_step_start} and a.id <= {t_step_start + t_step}
        #  print(insert_dwd_sql)
        cursor_doris.execute(insert_dwd_sql)
        t_step_start += 1
        print(f'完成结果表 第{t_step_start}次数据插入工作,', datetime.now())

    endtime = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('完成本次ods2dwd工作:', begintime, '开始执行，执行至', endtime, '结束执行')

    db_doris.close()


ods2dwd()
