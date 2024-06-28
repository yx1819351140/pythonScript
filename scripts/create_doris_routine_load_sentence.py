# -*- coding:UTF-8 -*-
# @Time    : 2024/5/28 11:01
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : create_doris_routine_load_sentence.py
# @Project : pythonScript
# @Software: PyCharm


def get_sentence(db_name='ods', table_name='', kafka_topic='', field_name=''):
    result_sent = """CREATE ROUTINE LOAD {}.{} on {}
COLUMNS TERMINATED BY ",",
COLUMNS (dt = CURDATE() + 0,{})
PROPERTIES
(
  "desired_concurrent_number" = "3",
  "max_batch_interval" = "20",
  "max_batch_rows" = "300000",
  "max_batch_size" = "209715200",
  "strict_mode" = "false",
  "format" = "json",
  "max_error_number" = "5000"
)
FROM KAFKA
(
  "kafka_broker_list" = "10.32.50.101:9092",
  "kafka_topic" = "{}",
  "property.group.id" = "{}_group",
  "property.client.id" = "{}_client",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

SHOW ROUTINE LOAD FOR ods.{};

select * from ods.{} where dt='20240528' limit 100;

PAUSE ROUTINE LOAD FOR ods.{};

ALTER ROUTINE LOAD FOR ods.{}
FROM KAFKA
(
  "property.kafka_default_offsets" = "OFFSET_END"
);

RESUME ROUTINE LOAD FOR ods.{};

SHOW ROUTINE LOAD FOR ods.{};"""

    field_name_list = [i.strip().split(':')[0].replace('"', '').strip() for i in field_name.replace('{', '').replace('}', '').split(',')]
    doris_field_name = ','.join(field_name_list)
    # doris_field_name = 'id,company_id,is_hide,max_budget,notice_type,party_name,party_type,province,publish_time,unique_id,create_time,update_time,use_flag,_operation_type,_sort_id'
    print(result_sent.format(db_name, table_name, table_name, doris_field_name, kafka_topic, kafka_topic, kafka_topic, table_name, table_name, table_name, table_name, table_name, table_name))


if __name__ == '__main__':
    field_name = """{
	"change_record": "187f85b6f6d5aa37b2aecd7a1bf8e81b",
	"change_type": "",
	"id": 660845071,
	"company_id": "8a93c805413f26a860a5a593b5e43bfc",
	"content_before": "章程备案",
	"update_time": "2024-05-26 23:59:58",
	"_operation_type": "insert",
	"change_field": "章程备案",
	"change_date": "2024-05-24",
	"create_time": "2024-05-26 23:59:58",
	"use_flag": 0,
	"_sort_id": "42487563257",
	"content_after": "章程备案"
}"""
    db_name = 'ods'
    table_name = 'ods_pingan_company_change_di'
    kafka_topic = 'company_change'
    get_sentence(db_name=db_name, table_name=table_name, kafka_topic=kafka_topic, field_name=field_name)
