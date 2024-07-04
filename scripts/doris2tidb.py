# coding:utf-8

import time, sys
import pymysql
from datetime import datetime


def doris_tidb():
    db_tidb = pymysql.connect(host="10.32.18.100", user="tidb_rw", password="rw_tidb@123", database="cloud_chain_v2",
                              charset="utf8", port=4900)
    cursor_tidb = db_tidb.cursor()

    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="app", charset="utf8",
                               port=9030)
    cursor_doris = db_doris.cursor()

    begintime = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # 拿到要执行的所有表
    sel_table = """
    select TABLE_SCHEMA,TABLE_NAME,TABLE_COMMENT from information_schema.TABLES where TABLE_SCHEMA='app' and 
    TABLE_NAME in ('gov_bid_list')
    ;
    """
    cursor_doris.execute(sel_table)
    source_bulks = cursor_doris.fetchall()
    tbl_num = 1
    for source_bulks_t in source_bulks:
        begin_tbl_time = datetime.now()
        # 拿到该表对应的列数
        column = "select count(*)-4 as col_cnt from information_schema.COLUMNS where  TABLE_SCHEMA ='app' and table_name = '%s' ;" % (
            source_bulks_t[1])
        cursor_doris.execute(column)
        column_cnt = cursor_doris.fetchone()

        # 获取列名
        column_name = "SELECT  GROUP_CONCAT(column_name) column_name FROM  information_schema.`COLUMNS` WHERE table_name = '%s' AND table_schema='app' ;" % (
            source_bulks_t[1])
        # 不同步'id', 'create_time', 'update_time', 'etl_time' 至tidb
        column_name = f"""
        SELECT  GROUP_CONCAT(column_name) column_name FROM  information_schema.`COLUMNS` 
        WHERE table_name = '{source_bulks_t[1]}' AND table_schema='app' 
        and column_name not in ('ID', 'id', 'create_time', 'update_time', 'etl_time')
        ;
        """
        cursor_doris.execute(column_name)
        columnsname = cursor_doris.fetchone()

        s = ','.join(["%s"] * (column_cnt[0]))

        sel_max_min_id = f"select min(id) as min_id,max(id) as max_id from app.{source_bulks_t[1]} ;"
        print(sel_max_min_id)
        cursor_doris.execute(sel_max_min_id)
        sel_max_min_id = cursor_doris.fetchone()
        t_ge_id = sel_max_min_id[0]
        t_le_id = t_ge_id + 100000
        t_end_id = sel_max_min_id[1]
        doris_tb_name = source_bulks_t[1] + '_dev'
        while t_ge_id < t_end_id:
            insert_sql = "replace into  %s(%s) values" % (doris_tb_name, columnsname[0])
            sql_doris = f"select {columnsname[0]} from {source_bulks_t[1]} a where  a.id between {t_ge_id} and {t_le_id};"
            cursor_doris.execute(sql_doris)
            source_bulks = cursor_doris.fetchall()
            insert_sql += "(%s)" % (s)
            if not insert_sql.endswith("values"):
                cursor_tidb.executemany(insert_sql + ";", source_bulks)
            cursor_tidb.execute("commit;")
            t_ge_id += 100000
            t_le_id += 100000
            print("执行", str(source_bulks_t[0]), "库,", f"第{tbl_num}张表，", str(source_bulks_t[1]),
                  f"表的插入工作！ a.id between {t_ge_id} and {t_le_id},max_id={t_end_id}", str(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        end_tbl_time = datetime.now()
        print(
            f"完成第{tbl_num}张表从doris app层同步至TiDB的操作，从{begin_tbl_time}到{end_tbl_time},共用时{(end_tbl_time - begin_tbl_time).total_seconds() / 60}分钟！")
        tbl_num += 1
    endtime = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('表更新操作完成，从', begintime, '开始执行，执行至', endtime, '结束执行')

    db_doris.close()
    db_tidb.close()

doris_tidb()
