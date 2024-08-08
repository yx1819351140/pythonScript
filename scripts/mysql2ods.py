# coding:utf-8

import time
import sys
import pymysql
from datetime import datetime, timedelta
import mysql_create_doris as mysql_create_doris


def mysql2doris(arg_mysql, arg_doris):
    # if len(sys.argv) != 3:
    #     print("执行错误，请按照如下格式执行脚本 \n", "Usage: python3 collectTidb2ods.py 手工表名称 ods层表名称")
    #     sys.exit(1)
    # # 接收参数
    # arg_mysql = sys.argv[1]
    # arg_doris = sys.argv[2]

    # 建立连接
    db_mysql = pymysql.connect(host="10.32.48.2", user="data_ware", password="data_ware@2021", database="el_no",
                               charset="utf8", port=3306)
    cursor_mysql = db_mysql.cursor()
    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="ods", charset="utf8",
                               port=9030)
    cursor_doris = db_doris.cursor()

    # 获取过去31天内新增的数据
    dt_mysql = (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    # ods层dt分区设置
    dt_ods = datetime.now().strftime("%Y%m%d")
    dt_ods_next = (datetime.now() - timedelta(days=-1)).strftime("%Y%m%d")

    begin_tbl_time = datetime.now()
    # 拿到该表对应的列数
    column = "select count(*)+1 as col_cnt from information_schema.COLUMNS where  TABLE_SCHEMA ='el_no' and table_name = '%s' ;" % (
        arg_mysql)
    cursor_mysql.execute(column)
    column_cnt = cursor_mysql.fetchone()
    # 获取列名
    column_name = f"""
    SELECT  GROUP_CONCAT(column_name) column_name FROM  information_schema.`COLUMNS` 
    WHERE table_name = '{arg_mysql}' AND table_schema='el_no' ;
    """
    cursor_mysql.execute(column_name)
    columnsname = cursor_mysql.fetchone()

    # 部分表存在update_time/updated,或都不存在,所以需要对where后sql做一个拼接
    where_condition = "1=1"
    if 'update_time' in columnsname[0]:
        where_condition = f"update_time > '{dt_mysql}'"
    if 'updated' in columnsname[0]:
        where_condition = f"updated > '{dt_mysql}'"
    # 拼接循环参数 部分字增为id，部分表字增为person_id
    id = 'id'
    if 'PERSON_ID' in columnsname[0]:
        id = 'PERSON_ID'

    # 创建表ods表(if not exists)
    ods_tbl_isexist = f"select count(*) as cnt from information_schema.`COLUMNS` WHERE table_name = '{arg_doris}' AND table_schema='ods' ;"
    cursor_doris.execute(ods_tbl_isexist)
    ods_tbl_isexist = cursor_doris.fetchone()
    print('ods_tbl_isexist[0]', ods_tbl_isexist[0])
    if ods_tbl_isexist[0] == 0:
        where_condition = "1=1"
        print('不存在该表')
        show_tbl_sql = f'show create table {arg_mysql};'
        cursor_mysql.execute(show_tbl_sql)
        mysql_create_table = cursor_mysql.fetchone()[1]
        create_table_sql = mysql_create_doris.mysql_to_doris(mysql_create_table, arg_doris, dt_ods, dt_ods_next)
        print(create_table_sql)
        cursor_doris.execute(create_table_sql)

    # 拼接% 参数
    s = ','.join(["%s"] * (column_cnt[0]))

    sel_max_min_id = f"select ifnull(min({id}),0) as min_id,ifnull(max({id}),0) as max_id from {arg_mysql} where {where_condition} ;"
    # sel_max_min_id = f"select ifnull(min({id}),0) as min_id,ifnull(max({id}),0) as max_id from {arg_mysql} ;"
    print(sel_max_min_id)
    cursor_mysql.execute(sel_max_min_id)
    sel_max_min_id = cursor_mysql.fetchone()
    t_ge_id = sel_max_min_id[0]
    t_le_id = t_ge_id + 10000
    t_end_id = sel_max_min_id[1]
    # 新建分区
    if t_ge_id > 0:
        add_partition = f"""
        -- ALTER TABLE ods.{arg_doris} SET ("dynamic_partition.enable" = "false");
        ALTER TABLE ods.{arg_doris} DROP PARTITION  if EXISTS  p{dt_ods};
        ALTER TABLE ods.{arg_doris} ADD PARTITION p{dt_ods} VALUES [("{dt_ods}"), ("{dt_ods_next}"));
        -- ALTER TABLE ods.{arg_doris} SET ("dynamic_partition.enable" = "true");
        """
        cursor_doris.executemany(add_partition, "")
    while t_ge_id < t_end_id:
        insert_sql = "insert into  ods.%s (%s,dt) values" % (arg_doris, columnsname[0])
        sql_mysql = f"select {columnsname[0]},'{dt_ods}' as dt from {arg_mysql} a where  a.{id} between {t_ge_id} and {t_le_id} and {where_condition} ;"
        cursor_mysql.execute(sql_mysql)
        source_bulks = cursor_mysql.fetchall()
        insert_sql += "(%s)" % (s)
        if not insert_sql.endswith("values"):
            # print(insert_sql)
            # print(source_bulks)
            cursor_doris.executemany(insert_sql + ";", source_bulks)
        cursor_doris.execute("commit;")
        print(f"执行{arg_doris}表的插入工作！ id between {t_ge_id} and {t_le_id},max_id={t_end_id}", datetime.now())
        t_ge_id += 10000
        t_le_id += 10000

    end_tbl_time = datetime.now()
    print(
        f"完成{arg_doris}表从mysql同步到Doris ODS层的操作，从{begin_tbl_time}到{end_tbl_time},共用时{(end_tbl_time - begin_tbl_time).total_seconds() / 60}分钟！")

    db_doris.close()
    db_mysql.close()


def run():
    table_name_list = ['ods_td_fwyent', 'ods_td_list_htech_hgro_as', 'ods_td_outsourcing_enterprise',
                       'ods_td_private_enterprise_top_100_list', 'ods_td_shandong_newmaterial', 'ods_td_top100_5g',
                       'ods_td_top100_ai', 'ods_td_top100_ai_ip', 'ods_td_top100_battery',
                       'ods_td_top100_belt_and_road', 'ods_td_top100_blockchain', 'ods_td_top100_build',
                       'ods_td_top100_catering', 'ods_td_top100_clothing', 'ods_td_top100_electronic',
                       'ods_td_top100_electronic_component', 'ods_td_top100_equipment_industry',
                       'ods_td_top100_internet', 'ods_td_top100_iot', 'ods_td_top100_mechanical_industry',
                       'ods_td_top100_medical_business', 'ods_td_top100_medical_industry',
                       'ods_td_top100_pesticide_sales', 'ods_td_top100_print_package', 'ods_td_top100_private_service',
                       'ods_td_top100_shandong', 'ods_td_top100_shanghai_company_2021',
                       'ods_td_top100_software_information_tech', 'ods_td_top100_strategic_emerging_industry',
                       'ods_td_top100_wuliu', 'ods_td_top2000', 'ods_td_top500_petrochemical_industry',
                       'ods_td_top500_private_produce', 'ods_td_top500_produce', 'ods_td_top_100_china_overseas_ent',
                       'ods_td_top50_edible_oil_processing', 'ods_td_top100_logistics_technology', 'ods_td_top100_b2b',
                       'ods_td_top100_geographic_information_industry', 'ods_td_top500_foreign_trade',
                       'ods_td_top100_fashion_retail', 'ods_td_top100_pcb', 'ods_td_top100_innovative_software',
                       'ods_td_top30_artificial_intelligence_robots', 'ods_td_top_automation_digital_branding',
                       'ods_td_top100_electronic_company', 'ods_td_top100_chemicals_company',
                       'ods_td_top100_medicine_manufacturing', 'ods_td_top50_wuliu',
                       'ods_td_top10_social_responsibility_private', 'ods_td_top50_light_and_equipment',
                       'ods_td_top100_electronics_and_semiconductors', 'ods_td_top30_chain_brand_quality',
                       'ods_td_top10_optical_communications', 'ods_td_top100_petroleum_and_chemical_industry',
                       'ods_td_top50_specialty_fertilizers', 'ods_td_top_pharmaceutical_industry',
                       'ods_td_top100_building_materials_private', 'ods_td_top10_industrial_internet',
                       'ods_td_top_electronic_circuits_industry', 'ods_td_top_100_china_qclbj',
                       'ods_td_top_100_world_qclbj', 'ods_td_list_gyyrj']
    for table_name in table_name_list:
        arg_mysql = table_name.replace('ods_td_', '')
        arg_doris = table_name.replace('ods_td_', 'ods_manual_')
        mysql2doris(arg_mysql, arg_doris)


if __name__ == "__main__":
    run()
