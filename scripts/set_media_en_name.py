# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> set_media_en_name         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/1/12 9:21
# @Software : win10 python3.6
"""
事件系统mysql媒体表设置媒体英文名称
mysql地址：192.168.12.240:3306
mysql账号：root mysql密码：123456
表名：global_event.media_info
column：name_en
"""
import pymysql
from pymysql.converters import escape_string

from utils.ESUtils import ElasticSearchUtils

db = pymysql.connect(host='192.168.12.240', user='root', password='123456', database='global_event', charset='utf8')
cursor = db.cursor()
cursor.execute("select `id`,name from media_info")
result = cursor.fetchall()
es = ElasticSearchUtils('192.168.12.220')
for i in result:
    media_id = i[0]
    media_name = i[1]
    media_name_en = None
    query = {
      "_source": ["related_media"],
      "size": 1,
      "query": {
        "bool": {
          "must": [
            {
              "nested": {
                "path": "related_media",
                "query": {
                  "bool": {
                    "must": [
                      {
                        "match": {
                          "related_media.media_name_zh": media_name
                        }
                      }
                    ]
                  }
                }
              }
            }
          ]
        }
      }
    }
    temp_data = es.search_data_by_query('hot_news', query)
    media_data_list = temp_data['hits']['hits'][0]['_source']['related_media']
    media_name_en = media_data_list[0]['media_name']
    print(f'中文名：{media_name}，英文名：{media_name_en}')
    sql = f"""update media_info set name_en='{escape_string(media_name_en)}' where `id`={media_id}"""
    cursor.execute(sql)
    db.commit()
cursor.close()
db.close()
