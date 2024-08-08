import re


def mysql_to_doris(mysql_create_table, ods_table_name, ods_dt, ods_dt_next):
    # 分割并清理MySQL建表语句
    lines = [line.strip() for line in mysql_create_table.split('\n') if line.strip()]

    # 提取表名
    # table_name_match = re.search(r'CREATE\s+TABLE\s+(\S+)\s*\(', '\n'.join(lines))
    # ods_table_name = table_name_match.group(1) if table_name_match else None

    # 创建Doris建表语句的基础结构
    doris_create_table = f"CREATE TABLE IF NOT EXISTS ods.{ods_table_name} (\n  `dt` int comment '分区，格式：yyyyMMdd', \n"

    comment_match = re.search(r'COMMENT=\'(.*)\'', mysql_create_table)
    if comment_match:
        comment = comment_match.group(1)
    else:
        comment = ''

    # 处理字段定义
    field_lines = []
    duplicate_key_clause = ''
    for line in lines:
        if 'PRIMARY KEY' in line:
            print('duplicate_key_clause =', duplicate_key_clause)
            pattern = r'\((.*?)\)'
            match = re.search(pattern, line)
            if match:
                duplicate_key_clause = ',' + match.group(1)
        if 'int(11) NOT NULL AUTO_INCREMENT' in line:
            line = line.replace('int(11) NOT NULL AUTO_INCREMENT', 'bigint')
        if 'varchar' in line:
            line = re.sub(r'varchar\(\d+\)', 'varchar(*)', line)
        if 'timestamp NULL DEFAULT' in line:
            line = line.replace('timestamp NULL DEFAULT', 'datetime  NOT NULL DEFAULT ')
        if 'ON UPDATE CURRENT_TIMESTAMP' in line:
            line = line.replace('ON UPDATE CURRENT_TIMESTAMP', '')
        if line.startswith('(') or line.endswith(')'):
            continue
        if 'CREATE TABLE' in line or 'PRIMARY KEY' in line or 'UNIQUE KEY' in line or 'KEY' in line or 'ENGINE=' in line or 'DEFAULT CHARSET=' in line:
            continue
        if 'AUTO_INCREMENT' in line:
            line = line.replace('AUTO_INCREMENT', '')
        # parts = line.split()
        # field_type = parts[1].split('(')[0]
        field_lines.append(line.replace('unsigned ', ''))

    # 添加字段到Doris建表语句
    for field_line in field_lines:
        doris_create_table += f"  {field_line}\n"

    # 添加Doris特有部分
    doris_create_table += f") ENGINE=OLAP\n"
    doris_create_table += f"DUPLICATE KEY(`dt`{duplicate_key_clause})\n"
    doris_create_table += f"COMMENT '{comment}'\n"
    doris_create_table += f"PARTITION BY RANGE(`dt`)\n(\n"
    doris_create_table += f'PARTITION p{ods_dt} VALUES [("{ods_dt}"), ("{ods_dt_next}")) \n'
    doris_create_table += f")\n"
    doris_create_table += f"DISTRIBUTED BY HASH(`dt`{duplicate_key_clause}) BUCKETS 4\n"
    doris_create_table += f"PROPERTIES (\n"
    doris_create_table += f"\"in_memory\" = \"false\",\n"
    doris_create_table += f"\"storage_format\" = \"V2\"\n"
    doris_create_table += f");\n"

    # 替换最后一列后的逗号
    engine_position = doris_create_table.find(') ENGINE=OLAP')
    comma_position = doris_create_table.rfind(',', 0, engine_position)
    return doris_create_table[:comma_position] + doris_create_table[comma_position + 1:engine_position] + doris_create_table[engine_position:]

    # return doris_create_table


if __name__ == "__main__":
    mysql_create_table = '''
    CREATE TABLE `person_talents_id_730` (
  `PERSON_ID` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `COMPANY_NAME` varchar(200) DEFAULT NULL,
  `STAFF_NAME` varchar(200) DEFAULT NULL,
  `IS_TYPE` int(11) DEFAULT '0' COMMENT '1、有企业名称分配；2、企业名称为null进行分配',
  `source_id` int(11) DEFAULT '0',
  `source_llj_id` int(11) DEFAULT '0',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`PERSON_ID`),
  UNIQUE KEY `COMPANY_NAME` (`COMPANY_NAME`,`STAFF_NAME`,`source_llj_id`),
  KEY `IS_TYPE` (`IS_TYPE`),
  KEY `source_id` (`source_id`),
  KEY `source_llj_id` (`source_llj_id`)
) ENGINE=InnoDB AUTO_INCREMENT=390739 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='#人-人才数据-730-ID分配'
    '''
    ods_table_name = "person_talents_id_730"
    ods_dt = '20240703'
    ods_dt_next = '20240704'
    print(mysql_to_doris(mysql_create_table, ods_table_name, ods_dt, ods_dt_next))
