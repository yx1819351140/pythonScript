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
    field_name_list.remove('dt')
    doris_field_name = '(dt="20240515",' + ','.join('{}={}'.format(i, i) for i in field_name_list) + ')'
    hive_field_name = '(' + ','.join(field_name_list) + ')'
    print(result_sent.format(doris_table, hive_db, hive_table, doris_table, hive_field_name, doris_field_name))


if __name__ == '__main__':
    field_name = """
    `dt` INT NOT NULL COMMENT '分区，格式：yyyyMMdd',
	`id` bigint NOT NULL COMMENT "自增主键",    
  `company_id` VARCHAR COMMENT "对应主体唯一键", 
	`invalid_date` VARCHAR COMMENT "",                                                                   
  `invalid_reason` VARCHAR COMMENT "",                                                                 
  `is_history` bigint COMMENT "",                                                                     
  `pawnee` VARCHAR COMMENT "质权人",                                                                         
  `pawnee_identify_no` VARCHAR COMMENT "证照/证件号码",                                                             
  `pledge_code` VARCHAR COMMENT "登记编号",                                                                    
  `pledge_date` VARCHAR COMMENT "股权出质设立登记日期",                                                                    
  `pledge_equity` VARCHAR COMMENT "出质股权数额",                                                                  
  `pledge_record` VARCHAR COMMENT "记录",                                                                  
  `pledge_status` VARCHAR COMMENT "状态",                                                                  
  `pledgor_name_id` VARCHAR COMMENT '机构出质人的company_id',                                 
  `pledgor_is_personal` VARCHAR COMMENT '出质人类型，0：机构，1：自然人，2：非机构非自然人',       
  `pawnee_name_id` VARCHAR COMMENT '机构质权人的company_id',                                  
  `pawnee_is_personal` VARCHAR COMMENT '质权人类型，0：机构，1：自然人，2：非机构非自然人',        
  `pledgor` VARCHAR COMMENT "出质人",                                                                        
  `pledgor_identify_no` VARCHAR COMMENT "证照/证件号码",                                                            
  `province_short` VARCHAR COMMENT "省份",                                                                 
  `public_date` VARCHAR COMMENT "公示日期",                                                                    
  `revoke_date` VARCHAR COMMENT "注销日期",                                                                    
  `revoke_reason` VARCHAR COMMENT "注销原因", 
  `create_time` VARCHAR COMMENT "创建时间",                    
  `update_time` VARCHAR COMMENT "更新时间",      
  `use_flag` bigint COMMENT "使用标记",        
	`_operation_type` VARCHAR COMMENT "操作类型：insert/update", 
	`_sort_id` bigint COMMENT "排序ID" 
    """
    doris_table = 'ods_pingan_company_pledge_di'
    hive_db = 'ods_b10000001_00004'
    hive_table = 'ods_td_company_pledge'
    get_sentence(doris_table, hive_db, hive_table, field_name)
