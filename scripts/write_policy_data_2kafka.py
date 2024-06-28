# -*- coding:UTF-8 -*-
# @Time    : 2024/6/28 10:49
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : write_policy_data_2kafka.py
# @Project : pythonScript
# @Software: PyCharm
import pymysql
from kafka import KafkaProducer
import json

MYSQL_HOST = '10.32.51.151'
MYSQL_PORT = 4000
MYSQL_USER = 'root'
MYSQL_PASSWORD = '6U=0@w#9m152nivS^C'
MYSQL_DB = 'z_wdd'
MYSQL_TABLE = 'policy_data_copy'

# Kafka servers
KAFKA_SERVERS = ['10.32.50.101:9092', '10.32.50.102:9092', '10.32.50.103:9092']
KAFKA_TOPIC = 'st_policy'  # 替换为你的Kafka主题名称

# Batch size
BATCH_SIZE = 162000
TOTAL_ROWS = 1620000
NUM_BATCHES = TOTAL_ROWS // BATCH_SIZE

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Connect to MySQL
connection = pymysql.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)

try:
    with connection.cursor() as cursor:
        # Get column names
        cursor.execute(f"SELECT * FROM {MYSQL_TABLE} LIMIT 1")
        columns = [desc[0] for desc in cursor.description]

        for batch_num in range(NUM_BATCHES):
            offset = batch_num * BATCH_SIZE
            sql = f"SELECT * FROM {MYSQL_TABLE} LIMIT {BATCH_SIZE} OFFSET {offset}"
            cursor.execute(sql)
            rows = cursor.fetchall()

            # Send data to Kafka
            for row in rows:
                data = dict(zip(columns, row))
                producer.send(KAFKA_TOPIC, data)

            # Flush the producer after each batch
            producer.flush()
            print(f"Batch {batch_num + 1}/{NUM_BATCHES} processed.")

    # Process remaining rows if any
    remaining_rows = TOTAL_ROWS % BATCH_SIZE
    if remaining_rows > 0:
        sql = f"SELECT * FROM {MYSQL_TABLE} LIMIT {remaining_rows} OFFSET {NUM_BATCHES * BATCH_SIZE}"
        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            data = dict(zip(columns, row))
            producer.send(KAFKA_TOPIC, data)

        producer.flush()
        print(f"Remaining {remaining_rows} rows processed.")

finally:
    connection.close()
    producer.close()