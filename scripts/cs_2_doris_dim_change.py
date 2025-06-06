import pandas as pd
import pymysql
"""
跑完执行dolphinscheduler上面的：PRO_常山项目--dm_cs--pro_cs_industry_f，之后跑：PRO_常山项目--dm_cs--st_export_dm_cs_company_detail，向后执行
"""

# Doris 数据库配置
HOST = "10.32.49.61"
PORT = 9030
USER = "yangxin"
PASSWORD = "yangxinQ123"
DATABASE = "dim"
CHARSET = "utf8"

# Excel 文件路径
excel_path = "./data/常山-产业链企业清单提取--修改-20250328-v1.xlsx"

# 读取 Excel 两个 Sheet（假设两 sheet 结构相同，可合并处理）
sheet_names = ["芯片", "新能源电池"]
dfs = [pd.read_excel(excel_path, sheet_name=sn) for sn in sheet_names]
df_all = pd.concat(dfs, ignore_index=True)

# 将Excel关键字段统一重命名（注意实际列名可能有空格或特殊符号，根据实际情况调整）
# 假设：企业名称在“企业名称”列，删/增在“删/增”列，状态/备注在“状态/备注”列，特殊备注在“特殊备注”列
df_all = df_all.rename(columns={
    "一级": "level1",
    "二级": "level2",
    "三级": "level3",
    "四级": "level4",
    "五级": "level5",
    "企业名称": "company_name",
    "删/增": "operation",
    "状态/备注": "status_remark",
    "特殊备注": "remarks",
    "产业链编码": "industry_code"
})

# 建立 Doris 连接
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

for _, row in df_all.iterrows():
    try:
        if pd.isna(row["company_name"]):
            continue

        company_name = str(row["company_name"]).strip()
        operation = str(row["operation"]).strip()
        ic = str(row["industry_code"]).strip() if pd.notna(row["industry_code"]) else None

        # 查询 company_id 和 region_code
        cursor.execute(
            "SELECT company_id, region_code FROM app.company WHERE company_name = %s",
            (company_name,)
        )
        result = cursor.fetchone()
        if not result:
            print(f"[跳过] 找不到公司：{company_name}")
            continue
        company_id, region_code = result

        # 统一处理 level1~level5
        processed = False
        for level in ["level1", "level2", "level3", "level4", "level5"]:
            if pd.notna(row[level]) and str(row[level]).strip():
                chain_name = str(row[level]).strip()

                # 查 industry_code
                cursor.execute(
                    "SELECT code FROM dim_industry_chain_cs WHERE name = %s",
                    (chain_name,)
                )
                code_result = cursor.fetchone()
                if not code_result:
                    print(f"[跳过] 找不到产业链：{chain_name}")
                    continue
                industry_code = code_result[0]

                if operation != "删":
                    cursor.execute(
                        "INSERT INTO dim_p_industry_chain_company_cs2 (industry_code, company_id, region_code, industry_score, ic) VALUES (%s, %s, %s, %s, %s)",
                        (industry_code, company_id, region_code, '0.99', ic)
                    )
                    print(f"[新增] {company_name} {company_id} -> {chain_name} {industry_code}")
                else:
                    cursor.execute(
                        "DELETE FROM dim_p_industry_chain_company_cs2 WHERE industry_code = %s AND company_id = %s",
                        (industry_code, company_id)
                    )
                    print(f"[删除] {company_name} {company_id} -> {chain_name} {industry_code}")

                processed = True

        if not processed:
            print(f"[跳过] 没有可处理的产业链：{company_name}")

    except Exception as e:
        print(f"[错误] {company_name} - {e}")



# 提交并关闭连接
conn.commit()
cursor.close()
conn.close()
