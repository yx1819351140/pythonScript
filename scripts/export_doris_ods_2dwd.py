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
    t_step_start = 3410000
    t_step_end = 340136622
    t_step = 341000

    while t_step_start < t_step_end:
        insert_dwd_sql = f"""INSERT INTO dwd.dwd_gov_bid_list_di
SELECT
    t1.id,
    t1.unique_id,
    t1.company_id,
    t1.company_name,
    t1.bid_list_type,
    t1.publish_time,
    t1.province,
    t1.notice_type,
    t1.max_budget,
    t1.is_hide,
    t1.bid_id,
    t1.company_class_code,
    t1.create_time,
    t1.update_time
FROM
    (
        select * from dwd.dwd_gov_bid_list_di
    ) t1
inner join
    (
         SELECT
             a.id,
             a.unique_id,
             b.company_id AS company_id,
             a.party_name AS company_name,
             CASE
                 WHEN a.party_type = 1 THEN 1
                 WHEN a.party_type = 2 THEN 2
                 ELSE 4
                 END AS bid_list_type,
             a.publish_time,
             a.province,
             a.notice_type,
             a.max_budget,
             a.is_hide,
             md5(CONCAT(nvl(b.company_name, ''), nvl(b.company_id, ''), nvl(b.company_class_code, ''), nvl(a.party_type, ''))) AS bid_id,
             b.company_class_code AS company_class_code,
             a.create_time,
             a.update_time
         FROM
             ods.ods_pingan_company_bid_entity_list_di a
                 INNER JOIN rel.rel_company_name_hist b ON a.party_name = b.company_name
         WHERE a.use_flag = '0' and a.id >= {t_step_start} and a.id < {t_step_start + t_step} AND a.dt = 20240515 AND LENGTH(b.company_name) > 0
     ) t2
on t1.company_name = t2.company_name
    and t1.company_id = t2.company_id
    and t1.company_class_code = t2.company_class_code
    and t1.bid_list_type = t2.bid_list_type
union all
select
    nvl(t2.id,t1.id),
    nvl(t2.unique_id,t1.unique_id),
    nvl(t2.company_id,t1.company_id),
    nvl(t2.company_name,t1.company_name),
    nvl(t2.bid_list_type,t1.bid_list_type),
    nvl(t2.publish_time,t1.publish_time),
    nvl(t2.province,t1.province),
    nvl(t2.notice_type,t1.notice_type),
    nvl(t2.max_budget,t1.max_budget),
    nvl(t2.is_hide,t1.is_hide),
    nvl(t2.bid_id,t1.bid_id),
    nvl(t2.company_class_code,t1.company_class_code),
    nvl(t2.create_time,t1.create_time),
    nvl(t2.update_time,t1.update_time)
from
    (
        select * from dwd.dwd_gov_bid_list_di
    ) t1
full outer join
    (
        SELECT
            a.id,
            a.unique_id,
            b.company_id AS company_id,
            a.party_name AS company_name,
            CASE
                WHEN a.party_type = 1 THEN 1
                WHEN a.party_type = 2 THEN 2
                ELSE 4
                END AS bid_list_type,
            a.publish_time,
            a.province,
            a.notice_type,
            a.max_budget,
            a.is_hide,
            md5(CONCAT(nvl(b.company_name, ''), nvl(b.company_id, ''), nvl(b.company_class_code, ''), nvl(a.party_type, ''))) AS bid_id,
            b.company_class_code AS company_class_code,
            a.create_time,
            a.update_time
        FROM
            ods.ods_pingan_company_bid_entity_list_di a
                INNER JOIN rel.rel_company_name_hist b ON a.party_name = b.company_name
        WHERE a.use_flag = '0' AND a.dt = 20240515 and a.id >= {t_step_start} and a.id < {t_step_start + t_step} AND LENGTH(b.company_name) > 0
    ) t2
on t1.company_name = t2.company_name
    and t1.company_id = t2.company_id
    and t1.company_class_code = t2.company_class_code
    and t1.bid_list_type = t2.bid_list_type;"""
        #  print(insert_dwd_sql)
        cursor_doris.execute(insert_dwd_sql)
        t_step_start += t_step
        print(f'完成 dwd_gov_bid_list_di 表 第{t_step_start}条数据插入工作,', datetime.now())

    endtime = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('完成本次ods2dwd工作:', begintime, '开始执行，执行至', endtime, '结束执行')

    db_doris.close()


ods2dwd()
