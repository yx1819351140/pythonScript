# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> group_news_by_news_category         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/22 13:28
# @Software : win10 python3.6
from utils.ESUtils import ElasticSearchUtils
"""
统计24小时新闻数据分类情况
"""

# query = {
#     "_source": ["news_category"],
#     "size": 7200,
#     "query": {
#         "range": {
#             "pub_time_timestamp": {
#                 "gte": 1679374800000,
#                 "lte": 1679461200000
#             }
#         }
#     }
# }
#
# result_dict = {}
#
# es = ElasticSearchUtils('192.168.12.220,192.168.12.221,192.168.12.222,192.168.12.223')
# data_list = es.search_data_by_query('hot_news', query)['hits']['hits']
# for data in data_list:
#     news_category = data['_source']['news_category']
#     for category in news_category:
#         try:
#             result_dict[category] += 1
#         except:
#             result_dict[category] = 1
# print(sorted(result_dict.items(), key=lambda x: x[1], reverse=True))

result_list = [('military', 7115), ('politics', 6443), ('economy', 6290), ('technology', 3946), ('policesystem', 3415), ('environment', 3247), ('media/internet', 2558), ('healthcare', 2177), ('education', 1781), ('domesticeconomy', 1641), ('civilliberties', 1566), ('immigration/refugees', 1409), ('terrorism', 1409), ('drug', 761), ('racism', 418), ('taxes', 233), ('trade', 202), ('unemployment', 99), ('internationalrelations', 84), ('electionfraud', 49), ('guncontrol', 49)]

for result in result_list:
    print(result[1])
