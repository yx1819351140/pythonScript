import pandas as pd
import pymysql

# Doris 数据库配置
HOST = "10.32.49.61"
PORT = 9030
USER = "yangxin"
PASSWORD = "yangxinQ123"
DATABASE = "dm"
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

try:
    # 遍历 Excel 每一行
    for index, row in df_all.iterrows():
        comp_name = row["company_name"]
        oper = str(row["operation"]) if pd.notna(row["operation"]) else ""
        status_val = row["status_remark"] if pd.notna(row["status_remark"]) else None
        remark_val = row["remarks"] if pd.notna(row["remarks"]) else None
        industry_code = row["industry_code"] if pd.notna(row["industry_code"]) else None

        # 根据 F 列（企业名称）匹配处理
        if "删" in oper:
            # 查询当前link_industry_codes字段值
            select_sql = f"SELECT link_industry_codes FROM dm_cs_company_detail1 WHERE company_name = '{comp_name}'"
            cursor.execute(select_sql)
            result = cursor.fetchone()
            if result:
                current_codes = result[0]  # 假设返回的就是字符串，如 '482,527'
                if current_codes:
                    # 分割、过滤掉需要删除的industry_code
                    codes_list = [code.strip() for code in current_codes.split(',') if code.strip()]
                    # 如果industry_code在列表中，则移除
                    if str(industry_code) in codes_list:
                        codes_list.remove(str(industry_code))
                        # 重新拼接为字符串
                        new_codes = ','.join(codes_list)
                        update_sql = f"UPDATE dm_cs_company_detail1 SET link_industry_codes = '{new_codes}' WHERE company_name = '{comp_name}'"
                        cursor.execute(update_sql)
                        print(
                            f"在企业【{comp_name}】中删除industry_code【{industry_code}】，更新后的link_industry_codes：{new_codes}")
                    else:
                        print(f"企业【{comp_name}】中未找到industry_code【{industry_code}】，无需更新。")
                else:
                    print(f"企业【{comp_name}】的link_industry_codes为空。")
            else:
                print(f"企业【{comp_name}】未在dm_cs_company_detail1中找到记录。")
        elif "增" in oper:
            # 增操作：先查询记录是否存在
            select_sql = f"SELECT link_industry_codes FROM dm_cs_company_detail1 WHERE company_name = '{comp_name}'"
            cursor.execute(select_sql)
            result = cursor.fetchone()
            if result:
                # 数据已存在，检查link_industry_codes是否包含industry_code
                current_codes = result[0]
                if current_codes:
                    codes_list = set([code.strip() for code in current_codes.split(',') if code.strip()])
                else:
                    codes_list = set()
                if str(industry_code) not in codes_list:
                    codes_list.add(str(industry_code))
                    new_codes = ','.join(list(codes_list))
                    update_sql = f"UPDATE dm_cs_company_detail1 SET link_industry_codes = '{new_codes}' WHERE company_name = '{comp_name}'"
                    cursor.execute(update_sql)
                    print(
                        f"【增】企业【{comp_name}】已存在，新增industry_code【{industry_code}】，更新后的link_industry_codes：{new_codes}")
                else:
                    print(f"【增】企业【{comp_name}】已存在且已包含industry_code【{industry_code}】，无需更新。")
            else:
                # 数据不存在，执行新增操作
                insert_sql = f"""
                            INSERT INTO dm.dm_cs_company_detail1(
                                company_id, company_name, reg_status_code, reg_status_name, legal_entity_name,
                                company_phone, es_dt, reg_capital_amt, reg_capital, reg_addr, company_label,
                                scale_code, reg_dt_tag, reg_capital_tag, company_scale_tag, company_type_tag,
                                industry_field_tags, financing_stage_tag, listing_status_codes, enqa_names,
                                qualification_tags, company_stock_info, ip_patent_num, industry_status_tags,
                                link_industry_codes, enqa_order, create_time, update_time, etl_time
                            )
                            WITH com AS (
                                SELECT company_id, company_name, reg_status_code, reg_status_name, legal_entity_name,
                                       es_dt, reg_capital_amt, reg_capital, reg_addr, scale_code, reg_dt_tag, reg_capital_tag,
                                       qualification_tags, company_scale_tag, company_type_tag, industry_field_tags, financing_stage_tag,
                                       create_time, update_time
                                FROM app.company a WHERE a.company_name = '{comp_name}'
                            ), ip_patent_num AS (
                                SELECT a.company_id, COUNT(DISTINCT c.ip_patent_id) AS ip_patent_num 
                                FROM com a
                                INNER JOIN app.ip_patent_applicant_list b ON a.company_id = b.company_id
                                INNER JOIN app.ip_patent c ON b.ip_patent_id = c.ip_patent_id
                                WHERE c.pat_type_code IS NOT NULL
                                GROUP BY a.company_id
                            ), enqa_names AS (
                                SELECT 
                                    a.company_id, 
                                    GROUP_CONCAT(DISTINCT b.type_name) AS enqa_names 
                                FROM com a 
                                INNER JOIN (
                                    SELECT company_id, REPLACE(type_name, '上市公司', '上市企业') AS type_name  
                                    FROM stg.dm_cs_qua_auth_detail 
                                ) b ON CAST(a.company_id AS VARCHAR(32)) = CAST(b.company_id AS VARCHAR(32))
                                GROUP BY a.company_id, b.company_id
                            ), company_phone AS (
                                SELECT 
                                    a.company_id, 
                                    a.company_name,
                                    GROUP_CONCAT(DISTINCT b.company_phone) AS company_phone
                                FROM com a 
                                INNER JOIN app.company_tel b ON a.company_id = b.company_id
                                GROUP BY a.company_id, a.company_name
                            ), company_stock_info AS (
                                SELECT 
                                    a.company_id, 
                                    a.company_name,
                                    CONCAT_WS(',', COLLECT_SET(c.name)) AS listing_status_codes,
                                    CONCAT_WS(', ', COLLECT_SET(CONCAT(b.company_name_alias, ' ', b.stock_code))) AS company_stock_info
                                FROM com a 
                                INNER JOIN dwd.dwd_csrc_base b ON a.company_id = CAST(b.company_id AS VARCHAR(32))
                                INNER JOIN dim.dim_common_code c ON b.stock_type_code = c.code AND c.code_type = 'LISTING_STATUS'
                                WHERE b.stock_type_code != '20' AND b.stock_status_code = '0'
                                GROUP BY a.company_id, a.company_name
                            )
                            SELECT 
                                a.company_id,
                                a.company_name,
                                a.reg_status_code,
                                a.reg_status_name,
                                a.legal_entity_name,
                                e.company_phone,
                                a.es_dt,
                                a.reg_capital_amt,
                                a.reg_capital,
                                a.reg_addr,
                                '' AS company_label,
                                a.scale_code,
                                a.reg_dt_tag,
                                a.reg_capital_tag,
                                a.company_scale_tag,
                                a.company_type_tag,
                                a.industry_field_tags,
                                a.financing_stage_tag,
                                h.listing_status_codes,
                                d.enqa_names,
                                a.qualification_tags,
                                h.company_stock_info,
                                b.ip_patent_num,
                                '' AS industry_status_tags,
                                -- 使用初始industry_code
                                '{industry_code}' AS link_industry_codes,
                                '' AS enqa_order,
                                a.create_time,
                                a.update_time,
                                NOW() AS etl_time
                            FROM com a
                            LEFT JOIN ip_patent_num b ON CAST(a.company_id AS VARCHAR(32)) = CAST(b.company_id AS VARCHAR(32))
                            LEFT JOIN enqa_names d ON CAST(a.company_id AS VARCHAR(32)) = CAST(d.company_id AS VARCHAR(32))
                            LEFT JOIN company_phone e ON CAST(a.company_id AS VARCHAR(32)) = CAST(e.company_id AS VARCHAR(32))
                            LEFT JOIN company_stock_info h ON CAST(a.company_id AS VARCHAR(32)) = CAST(h.company_id AS VARCHAR(32));
                            """
                cursor.execute(insert_sql)
                print(f"【增】新增企业【{comp_name}】，初始industry_code【{industry_code}】")

        # 更新操作：如果状态/备注不为空，则更新reg_status_name字段
        if status_val:
            update_sql = f"UPDATE dm_cs_company_detail1 SET reg_status_name ='{status_val}' WHERE company_name = '{comp_name}'"
            cursor.execute(update_sql)
            print(f"更新企业[{comp_name}]的状态为：{status_val}")

    # 提交上面的增删改操作
    conn.commit()
    print("完成对dm_cs_company_detail1表的更新操作。")

    # ----------------------------
    # 新增特殊备注remarks列
    alter_sql = "ALTER TABLE dm_cs_company_detail1 ADD COLUMN remarks VARCHAR(500) NULL COMMENT '特殊备注'"
    cursor.execute(alter_sql)
    conn.commit()
    print("dm_cs_company_detail1中新增remarks字段。")

    # 根据Excel中记录的特殊备注，更新新表中的remarks字段
    # 这里遍历 Excel 数据，针对备注不为空的企业进行更新
    for index, row in df_all.iterrows():
        comp_name = row["company_name"]
        remark_val = row["remarks"] if pd.notna(row["remarks"]) else None
        if remark_val:
            update_remark_sql = f"UPDATE dm_cs_company_detail1 SET remarks = '{remark_val}' WHERE company_name = '{comp_name}'"
            cursor.execute(update_remark_sql)
            print(f"更新dm_cs_company_detail1中企业[{comp_name}]的remarks为：{remark_val}")
    conn.commit()
    print("所有操作执行完毕。")

except Exception as e:
    conn.rollback()
    print("执行出错，事务已回滚。错误信息：", e)
finally:
    cursor.close()
    conn.close()
