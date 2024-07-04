# -*- coding:UTF-8 -*-
# @Time    : 2024/7/3 14:38
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : create_doris_2tidb_conf.py.py
# @Project : pythonScript
# @Software: PyCharm
import pymysql


def get_sql(table_name):
    db_doris = pymysql.connect(host="10.32.49.61", user="root", password="@aGf5GPnS25k", database="app", charset="utf8",
                               port=9030)
    cursor_doris = db_doris.cursor()
    cursor_doris.execute(f"""SELECT  GROUP_CONCAT(column_name) column_name FROM  information_schema.`COLUMNS` 
        WHERE table_name = '{table_name}' AND table_schema='app' 
        and column_name not in ('ID', 'id', 'create_time', 'update_time', 'etl_time')""")
    column_name = ','.join(cursor_doris.fetchone())
    cursor_doris.execute(f"""SELECT  column_name FROM  information_schema.`COLUMNS` 
        WHERE table_name = '{table_name}' AND table_schema='app' and column_key = 'UNI'""")
    primary_keys = cursor_doris.fetchone()[0]
    result_conf = """env {
    job.name = "st_export_%s"
    job.mode = "BATCH"
}

source {
    jdbc {
        url = "jdbc:mysql://10.32.49.61:9030/ods?serverTimezone=GMT+8"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "@aGf5GPnS25k"
        query = "select %s from app.%s where etl_time >= '"${global_dt}"'"
  }
}

transform {
}

sink {
    jdbc {
        driver = "com.mysql.cj.jdbc.Driver"
        url = "jdbc:mysql://10.32.18.100:4900/cloud_chain_v2?&useConfigs=maxPerformance&useServerPrepStmts=true&prepStmtCacheSqlLimit=2048&prepStmtCacheSize=256&rewriteBatchedStatements=true&allowMultiQueries=true&sessionVariables=tidb_mem_quota_query=4294967296&sessionVariables=tidb_batch_insert=1&sessionVariables=tidb_dml_batch_size=100"
        user = "tidb_rw"
        password = "rw_tidb@123"
        support_upsert_by_query_primary_key_exist = true
        generate_sink_sql = true
        database = "cloud_chain_v2"
        table = "%s_dev"
        primary_keys = ["%s"]
    }
}""" % (table_name, column_name, table_name, table_name, primary_keys)
    print(result_conf.replace('GMT+8', 'GMT%2b8'))


get_sql('gov_bid_list')
