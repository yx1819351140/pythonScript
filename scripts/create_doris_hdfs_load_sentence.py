# -*- coding:UTF-8 -*-
# @Time    : 2024/5/15 11:10
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : create_doris_hdfs_load_sentence.py
# @Project : pythonScript
# @Software: PyCharm
import re


def get_sentence(doris_table='', hive_db='ods_b10000001_00004', hive_table='', field_name=''):
    result_sent = """LOAD LABEL {}
(
     DATA INFILE("hdfs://10.32.49.19:8020/data/hive/warehouse/{}.db/{}/*/*")
     INTO TABLE {}
     COLUMNS TERMINATED BY ","
     FORMAT AS "parquet"
     {}
     SET
     {}
)
 WITH HDFS
 (
     "fs.defaultFS"="hdfs://10.32.49.19:8020",
     "hdfs_user"="hdfs"
 )
PROPERTIES
(
     "timeout"="3000",
     "max_filter_ratio"="0.1"
);"""

    field_name_list = [i.strip().split(' ')[0].replace('`', '').strip() for i in field_name.split(',')]
    # field_name_list.remove('dt')
    doris_field_name = '(dt="20240515",' + ','.join('{}={}'.format(i, i) for i in field_name_list) + ')'
    hive_field_name = '(' + ','.join(field_name_list) + ')'
    print(result_sent.format(doris_table, hive_db, hive_table, doris_table, hive_field_name, doris_field_name))


if __name__ == '__main__':
    field_name = """`company_id_x` varchar COMMENT '销方企业ID', 
	`company_id_g` varchar COMMENT '购方企业ID',
	`pro_name` varchar COMMENT '产品名称', 
	`fp_id` varchar COMMENT '供应ID', 
	`company_name_x` varchar COMMENT '销方企业名称',
	`company_name_g` varchar COMMENT '购方企业名称'
    """
    doris_table = 'dwd_fp_company_details'
    hive_db = 'dwd'
    hive_table = 'dwd_fp_company_details_distinct'
    get_sentence(doris_table, hive_db, hive_table, field_name)
