import pandas as pd
import pymysql

# Tidb 数据库配置
HOST = "10.32.50.11"
PORT = 3306
USER = "data_smy"
PASSWORD = "data_smy@2022"
DATABASE = "pro_cs"
CHARSET = "utf8"

# 建立连接
conn = pymysql.connect(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    charset=CHARSET,
    autocommit=False
)
cursor = conn.cursor()

# Doris 数据库配置
HOST = "10.32.49.61"
PORT = 9030
USER = "yangxin"
PASSWORD = "yangxinQ123"
DATABASE = "dim"
CHARSET = "utf8"

# 建立连接
conn1 = pymysql.connect(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
    database=DATABASE,
    charset=CHARSET,
    autocommit=False
)
cursor1 = conn1.cursor()

cursor.execute('truncate table pro_cs.industry_chain_detail_company')
cursor1.execute(
    "SELECT a.industry_code, b.company_id FROM dim.dim_p_industry_chain_company_cs2 a inner join dm.dm_cs_company_detail b on a.company_id=b.company_id WHERE a.region_code = '330822' and b.reg_status_code like '11%'"
)
result = cursor1.fetchall()
if result:
    for data in result:
        industry_code = data[0]
        company_id = data[1]
        cursor.execute(
            "INSERT INTO industry_chain_detail_company (industry_code, company_id) VALUES (%s, %s)",
            (industry_code, company_id)
        )
        print(f"[插入] {company_id} -> {industry_code}")

# 提交并关闭
conn.commit()
cursor.close()
cursor1.close()
conn.close()
conn1.close()
