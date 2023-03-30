# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> group_news_by_related_media         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/30 16:56
# @Software : win10 python3.6
from utils.ESUtils import ElasticSearchUtils

# query = {
#   "from": 0,
#   "size": 0,
#   "version": True,
#   "query": {
#     "nested": {
#       "path": "related_media",
#       "query": {
#         "bool": {
#           "must_not": [
#             {
#               "term": {
#                 "related_media.media_country.keyword": ""
#               }
#             }
#           ],
#           "filter": {
#             "exists": {
#               "field": "related_media.media_country"
#             }
#           }
#         }
#       }
#     }
#   },
#   "aggregations": {
#     "related_media_agg": {
#       "nested": {
#         "path": "related_media"
#       },
#       "aggregations": {
#         "related_media_media_name_keyword_agg": {
#           "terms": {
#             "field": "related_media.media_name_zh.keyword",
#             "size": 500,
#             "min_doc_count": 1,
#             "shard_min_doc_count": 0,
#             "show_term_doc_count_error": False,
#             "order": [
#               {
#                 "_count": "desc"
#               },
#               {
#                 "_key": "asc"
#               }
#             ]
#           }
#         }
#       }
#     }
#   }
# }
# es = ElasticSearchUtils('192.168.12.220,192.168.12.221,192.168.12.222,192.168.12.223')
# data_list = es.search_data_by_query('hot_news', query)['hits']['hits']
# print(data_list)
import pymysql

data_list = [
          {
            "key" : "MSN",
            "doc_count" : 39743
          },
          {
            "key" : "menafn.com",
            "doc_count" : 35332
          },
          {
            "key" : "Mail Online",
            "doc_count" : 30071
          },
          {
            "key" : "news.yahoo.com",
            "doc_count" : 29966
          },
          {
            "key" : "The Times of India",
            "doc_count" : 17322
          },
          {
            "key" : "thehindu.com",
            "doc_count" : 17137
          },
          {
            "key" : "the Guardian",
            "doc_count" : 14190
          },
          {
            "key" : "Yahoo News",
            "doc_count" : 13356
          },
          {
            "key" : "allAfrica.com",
            "doc_count" : 13209
          },
          {
            "key" : "business-standard.com",
            "doc_count" : 12625
          },
          {
            "key" : "www.theepochtimes.com",
            "doc_count" : 12222
          },
          {
            "key" : "福克斯新闻频道",
            "doc_count" : 11703
          },
          {
            "key" : "International Business Times",
            "doc_count" : 11547
          },
          {
            "key" : "mirror",
            "doc_count" : 10689
          },
          {
            "key" : "黎明报 (巴基斯坦)",
            "doc_count" : 10457
          },
          {
            "key" : "The Economic Times",
            "doc_count" : 10354
          },
          {
            "key" : "NDTV.com",
            "doc_count" : 10267
          },
          {
            "key" : "华盛顿邮报",
            "doc_count" : 10206
          },
          {
            "key" : "Mirage News",
            "doc_count" : 9902
          },
          {
            "key" : "prnewswire.com",
            "doc_count" : 9382
          },
          {
            "key" : "Newsweek",
            "doc_count" : 9056
          },
          {
            "key" : "nytimes.com",
            "doc_count" : 8982
          },
          {
            "key" : "The Telegraph",
            "doc_count" : 8127
          },
          {
            "key" : "The Star",
            "doc_count" : 8122
          },
          {
            "key" : "timesofisrael.com",
            "doc_count" : 8115
          },
          {
            "key" : "Breitbart",
            "doc_count" : 8023
          },
          {
            "key" : "Tribuneindia News Service",
            "doc_count" : 7688
          },
          {
            "key" : "Vanguard News",
            "doc_count" : 7637
          },
          {
            "key" : "The Express Tribune",
            "doc_count" : 7603
          },
          {
            "key" : "CBC",
            "doc_count" : 7576
          },
          {
            "key" : "telegraphindia.com",
            "doc_count" : 7534
          },
          {
            "key" : " The Jerusalem Post | JPost.com ",
            "doc_count" : 7498
          },
          {
            "key" : "thenews.com.pk",
            "doc_count" : 7402
          },
          {
            "key" : "cbsnews.com",
            "doc_count" : 7187
          },
          {
            "key" : "Israel National News",
            "doc_count" : 7139
          },
          {
            "key" : "日本时报",
            "doc_count" : 7096
          },
          {
            "key" : "Free Press Journal",
            "doc_count" : 7069
          },
          {
            "key" : "iol.co.za",
            "doc_count" : 6980
          },
          {
            "key" : "Devdiscourse",
            "doc_count" : 6916
          },
          {
            "key" : "Dailystar.co.uk",
            "doc_count" : 6830
          },
          {
            "key" : "Punch Newspapers",
            "doc_count" : 6795
          },
          {
            "key" : "BNN",
            "doc_count" : 6781
          },
          {
            "key" : "yahoo.com",
            "doc_count" : 6749
          },
          {
            "key" : "Sputnik International",
            "doc_count" : 6557
          },
          {
            "key" : "aljazeera.com",
            "doc_count" : 6516
          },
          {
            "key" : "Manila Bulletin",
            "doc_count" : 6409
          },
          {
            "key" : "Arkansas Online",
            "doc_count" : 6348
          },
          {
            "key" : "Forbes",
            "doc_count" : 6294
          },
          {
            "key" : "The Daily Star",
            "doc_count" : 6197
          },
          {
            "key" : "castanet.net",
            "doc_count" : 6000
          },
          {
            "key" : "LatestLY",
            "doc_count" : 5885
          },
          {
            "key" : "news.webindia123.com",
            "doc_count" : 5797
          },
          {
            "key" : "BBC News",
            "doc_count" : 5792
          },
          {
            "key" : "StreetInsider.com",
            "doc_count" : 5762
          },
          {
            "key" : "Europe Sun",
            "doc_count" : 5677
          },
          {
            "key" : "The Standard",
            "doc_count" : 5607
          },
          {
            "key" : "The Washington Times",
            "doc_count" : 5600
          },
          {
            "key" : "news.am",
            "doc_count" : 5561
          },
          {
            "key" : "BSS",
            "doc_count" : 5425
          },
          {
            "key" : "Zee News",
            "doc_count" : 5305
          },
          {
            "key" : "PressTV",
            "doc_count" : 5177
          },
          {
            "key" : "阿拉伯新闻",
            "doc_count" : 5129
          },
          {
            "key" : "BBC英国广播公司",
            "doc_count" : 5113
          },
          {
            "key" : "市场观察",
            "doc_count" : 5076
          },
          {
            "key" : "伦敦大轰炸",
            "doc_count" : 5068
          },
          {
            "key" : "VOA",
            "doc_count" : 5043
          },
          {
            "key" : "The Heritage Foundation",
            "doc_count" : 4950
          },
          {
            "key" : "华盛顿观察家报",
            "doc_count" : 4925
          },
          {
            "key" : "Russia Herald",
            "doc_count" : 4790
          },
          {
            "key" : "Online Free Press release news distribution - TopWireNews.com",
            "doc_count" : 4781
          },
          {
            "key" : "Digital Journal",
            "doc_count" : 4753
          },
          {
            "key" : "法国24",
            "doc_count" : 4726
          },
          {
            "key" : "Asharq AL-awsat",
            "doc_count" : 4700
          },
          {
            "key" : "马尼拉时报",
            "doc_count" : 4687
          },
          {
            "key" : "dailycaller.com",
            "doc_count" : 4677
          },
          {
            "key" : "Ahram Online ",
            "doc_count" : 4670
          },
          {
            "key" : "Middle East Monitor",
            "doc_count" : 4648
          },
          {
            "key" : "INQUIRER.net",
            "doc_count" : 4628
          },
          {
            "key" : "RadioFreeEurope/RadioLiberty",
            "doc_count" : 4626
          },
          {
            "key" : "India Gazette",
            "doc_count" : 4618
          },
          {
            "key" : "FinanzNachrichten.de",
            "doc_count" : 4591
          },
          {
            "key" : "Philstar.com",
            "doc_count" : 4575
          },
          {
            "key" : "Modern Ghana",
            "doc_count" : 4546
          },
          {
            "key" : "The Nation Newspaper",
            "doc_count" : 4529
          },
          {
            "key" : "ABC News",
            "doc_count" : 4522
          },
          {
            "key" : "Pakistan Observer",
            "doc_count" : 4479
          },
          {
            "key" : "Daily Post Nigeria",
            "doc_count" : 4455
          },
          {
            "key" : "Daily Record",
            "doc_count" : 4451
          },
          {
            "key" : "New Delhi Times",
            "doc_count" : 4448
          },
          {
            "key" : "Tribune Online",
            "doc_count" : 4446
          },
          {
            "key" : "AP NEWS",
            "doc_count" : 4443
          },
          {
            "key" : "Dunya News",
            "doc_count" : 4339
          },
          {
            "key" : "BirminghamLive",
            "doc_count" : 4271
          },
          {
            "key" : "Local News 8",
            "doc_count" : 4271
          },
          {
            "key" : "WalesOnline",
            "doc_count" : 4253
          },
          {
            "key" : "Daily Times",
            "doc_count" : 4203
          },
          {
            "key" : "Los Angeles Times",
            "doc_count" : 4197
          },
          {
            "key" : "prokerala.com",
            "doc_count" : 4196
          },
          {
            "key" : "IANS Live",
            "doc_count" : 4118
          },
          {
            "key" : "Manchester Evening News",
            "doc_count" : 4109
          },
          {
            "key" : "https://www.bangkokpost.com",
            "doc_count" : 4047
          },
          {
            "key" : "marketscreener.com",
            "doc_count" : 3989
          },
          {
            "key" : "NBC News",
            "doc_count" : 3957
          },
          {
            "key" : "gulfnews.com",
            "doc_count" : 3915
          },
          {
            "key" : "geo.tv",
            "doc_count" : 3909
          },
          {
            "key" : "Big News Network.com",
            "doc_count" : 3896
          },
          {
            "key" : "charter97.org",
            "doc_count" : 3889
          },
          {
            "key" : "Colorado Springs Gazette",
            "doc_count" : 3799
          },
          {
            "key" : "JD Supra",
            "doc_count" : 3792
          },
          {
            "key" : "KTVZ",
            "doc_count" : 3775
          },
          {
            "key" : "The South African",
            "doc_count" : 3773
          },
          {
            "key" : "thisdaylive.com",
            "doc_count" : 3758
          },
          {
            "key" : "News24",
            "doc_count" : 3730
          },
          {
            "key" : "TASS",
            "doc_count" : 3684
          },
          {
            "key" : "Metro US",
            "doc_count" : 3666
          },
          {
            "key" : "Irish Mirror",
            "doc_count" : 3580
          },
          {
            "key" : "euronews",
            "doc_count" : 3558
          },
          {
            "key" : "DNA India",
            "doc_count" : 3544
          },
          {
            "key" : "RTE.ie",
            "doc_count" : 3533
          },
          {
            "key" : "拉美社",
            "doc_count" : 3502
          },
          {
            "key" : "thefrontierpost.com",
            "doc_count" : 3499
          },
          {
            "key" : "Jammu Kashmir Latest News | Tourism | Breaking News J&K",
            "doc_count" : 3494
          },
          {
            "key" : "ZeroHedge",
            "doc_count" : 3491
          },
          {
            "key" : "saharareporters.com",
            "doc_count" : 3453
          },
          {
            "key" : "Stars and Stripes",
            "doc_count" : 3400
          },
          {
            "key" : "armenpress.am",
            "doc_count" : 3395
          },
          {
            "key" : "Liverpool Echo",
            "doc_count" : 3338
          },
          {
            "key" : "Interfax-Ukraine",
            "doc_count" : 3332
          },
          {
            "key" : "koreatimes",
            "doc_count" : 3324
          },
          {
            "key" : "I24NEWS",
            "doc_count" : 3299
          },
          {
            "key" : "Stuff",
            "doc_count" : 3298
          },
          {
            "key" : "Al-Monitor: Independent",
            "doc_count" : 3242
          },
          {
            "key" : "政客 (杂志)",
            "doc_count" : 3234
          },
          {
            "key" : "Premium Times Nigeria",
            "doc_count" : 3206
          },
          {
            "key" : "Bleeding Cool News And Rumors",
            "doc_count" : 3178
          },
          {
            "key" : "ITV News",
            "doc_count" : 3167
          },
          {
            "key" : "Channels Television",
            "doc_count" : 3048
          },
          {
            "key" : "澳洲金融时报",
            "doc_count" : 3048
          },
          {
            "key" : "HullLive",
            "doc_count" : 3015
          },
          {
            "key" : "Jamaica Observer",
            "doc_count" : 3008
          },
          {
            "key" : "Bloomberg.com",
            "doc_count" : 2987
          },
          {
            "key" : "hotair.com",
            "doc_count" : 2986
          },
          {
            "key" : "WTOP News",
            "doc_count" : 2977
          },
          {
            "key" : "thepeninsulaqatar.com",
            "doc_count" : 2974
          },
          {
            "key" : "Star Tribune",
            "doc_count" : 2950
          },
          {
            "key" : "TheCable",
            "doc_count" : 2948
          },
          {
            "key" : "EIN News",
            "doc_count" : 2931
          },
          {
            "key" : "The Sydney Morning Herald",
            "doc_count" : 2927
          },
          {
            "key" : "dw.com",
            "doc_count" : 2913
          },
          {
            "key" : "ABC17NEWS",
            "doc_count" : 2890
          },
          {
            "key" : "New York Daily News",
            "doc_count" : 2890
          },
          {
            "key" : "The Age",
            "doc_count" : 2821
          },
          {
            "key" : "San Diego Union-Tribune",
            "doc_count" : 2820
          },
          {
            "key" : "HuffPost",
            "doc_count" : 2814
          },
          {
            "key" : "The Guardian Nigeria News - Nigeria and World News",
            "doc_count" : 2789
          },
          {
            "key" : "South East Asia Post",
            "doc_count" : 2760
          },
          {
            "key" : "We Got This Covered",
            "doc_count" : 2754
          },
          {
            "key" : "freerepublic.com",
            "doc_count" : 2753
          },
          {
            "key" : "CTVNews",
            "doc_count" : 2711
          },
          {
            "key" : "9news.com.au",
            "doc_count" : 2702
          },
          {
            "key" : "tmcnet.com",
            "doc_count" : 2700
          },
          {
            "key" : "马耳他时报",
            "doc_count" : 2697
          },
          {
            "key" : "KXLY",
            "doc_count" : 2696
          },
          {
            "key" : "Khaleej Times",
            "doc_count" : 2694
          },
          {
            "key" : "MDJOnline.com",
            "doc_count" : 2684
          },
          {
            "key" : "India Today",
            "doc_count" : 2670
          },
          {
            "key" : "News Ghana",
            "doc_count" : 2645
          },
          {
            "key" : "The Motley Fool",
            "doc_count" : 2619
          },
          {
            "key" : "布里斯本时报",
            "doc_count" : 2591
          },
          {
            "key" : "BusinessGhana",
            "doc_count" : 2590
          },
          {
            "key" : "hellenicshippingnews.com",
            "doc_count" : 2590
          },
          {
            "key" : "The Korea Herald",
            "doc_count" : 2576
          },
          {
            "key" : "New Age | The Most Popular Outspoken English Daily in Bangladesh",
            "doc_count" : 2560
          },
          {
            "key" : "SundayWorld.com",
            "doc_count" : 2551
          },
          {
            "key" : "RNZ",
            "doc_count" : 2527
          },
          {
            "key" : "today.az",
            "doc_count" : 2525
          },
          {
            "key" : "TheJournal.ie",
            "doc_count" : 2516
          },
          {
            "key" : "STLtoday.com",
            "doc_count" : 2512
          },
          {
            "key" : "KVIA",
            "doc_count" : 2499
          },
          {
            "key" : "www.thesundaily.my",
            "doc_count" : 2492
          },
          {
            "key" : "ukrinform.net",
            "doc_count" : 2488
          },
          {
            "key" : "Peacefmonline.com - Ghana news",
            "doc_count" : 2476
          },
          {
            "key" : "UrduPoint",
            "doc_count" : 2473
          },
          {
            "key" : "Channel3000.com",
            "doc_count" : 2461
          },
          {
            "key" : "The National",
            "doc_count" : 2460
          },
          {
            "key" : "UPI",
            "doc_count" : 2460
          },
          {
            "key" : "Northwest Georgia News",
            "doc_count" : 2430
          },
          {
            "key" : "ewn.co.za",
            "doc_count" : 2408
          },
          {
            "key" : "USA TODAY",
            "doc_count" : 2406
          },
          {
            "key" : "WAtoday",
            "doc_count" : 2384
          },
          {
            "key" : "Mediaite",
            "doc_count" : 2324
          },
          {
            "key" : "NBC Chicago",
            "doc_count" : 2317
          },
          {
            "key" : "finance.yahoo.com",
            "doc_count" : 2287
          },
          {
            "key" : "island.lk",
            "doc_count" : 2286
          },
          {
            "key" : "The Sun Nigeria",
            "doc_count" : 2276
          },
          {
            "key" : "KESQ",
            "doc_count" : 2259
          },
          {
            "key" : "Press and Journal",
            "doc_count" : 2247
          },
          {
            "key" : "独行日报",
            "doc_count" : 2243
          },
          {
            "key" : "blogs.timesofisrael.com",
            "doc_count" : 2209
          },
          {
            "key" : "NBC Connecticut",
            "doc_count" : 2208
          },
          {
            "key" : "WRAL.com",
            "doc_count" : 2203
          },
          {
            "key" : "Newsbusters",
            "doc_count" : 2193
          },
          {
            "key" : "NaturalNews.com",
            "doc_count" : 2191
          },
          {
            "key" : "redstate.com",
            "doc_count" : 2184
          },
          {
            "key" : "Middle East Eye",
            "doc_count" : 2180
          },
          {
            "key" : "Mareeg.com somalia",
            "doc_count" : 2138
          },
          {
            "key" : "Post and Courier",
            "doc_count" : 2114
          },
          {
            "key" : "medicalxpress.com",
            "doc_count" : 2095
          },
          {
            "key" : " Sports",
            "doc_count" : 2091
          },
          {
            "key" : "everything2.com",
            "doc_count" : 2086
          },
          {
            "key" : "The New Arab",
            "doc_count" : 2078
          },
          {
            "key" : "thehindubusinessline.com",
            "doc_count" : 2078
          },
          {
            "key" : "talkvietnam.com",
            "doc_count" : 2076
          },
          {
            "key" : "mint",
            "doc_count" : 2062
          },
          {
            "key" : "jewishpress.com",
            "doc_count" : 2051
          },
          {
            "key" : "NBC 5 Dallas-Fort Worth",
            "doc_count" : 2050
          },
          {
            "key" : "KRDO",
            "doc_count" : 2049
          },
          {
            "key" : "亚洲第一站",
            "doc_count" : 2037
          },
          {
            "key" : "ZB",
            "doc_count" : 2036
          },
          {
            "key" : "POLITICO",
            "doc_count" : 2030
          },
          {
            "key" : "Africanews",
            "doc_count" : 2024
          },
          {
            "key" : " trusted coverage of the Middle East",
            "doc_count" : 2020
          },
          {
            "key" : "UnionLeader.com",
            "doc_count" : 2010
          },
          {
            "key" : "azertag.az",
            "doc_count" : 2003
          },
          {
            "key" : "National Accord Newspaper",
            "doc_count" : 1996
          },
          {
            "key" : "aa.com.tr",
            "doc_count" : 1985
          },
          {
            "key" : "The Wire",
            "doc_count" : 1981
          },
          {
            "key" : "Al Arabiya English",
            "doc_count" : 1978
          },
          {
            "key" : "世界社会主义者网站",
            "doc_count" : 1969
          },
          {
            "key" : "WEIS | Local & Area News",
            "doc_count" : 1968
          },
          {
            "key" : "NBC Los Angeles",
            "doc_count" : 1966
          },
          {
            "key" : "townhall.com",
            "doc_count" : 1961
          },
          {
            "key" : "Malay Mail ",
            "doc_count" : 1955
          },
          {
            "key" : "WKZO | Everything Kalamazoo | 590 AM · 106.9 FM",
            "doc_count" : 1955
          },
          {
            "key" : "Saudigazette",
            "doc_count" : 1949
          },
          {
            "key" : "daily sun",
            "doc_count" : 1948
          },
          {
            "key" : "NBC New York",
            "doc_count" : 1944
          },
          {
            "key" : "JNS.org",
            "doc_count" : 1934
          },
          {
            "key" : "laprensalatina.com",
            "doc_count" : 1934
          },
          {
            "key" : "Press Herald",
            "doc_count" : 1931
          },
          {
            "key" : "PRWeb",
            "doc_count" : 1928
          },
          {
            "key" : "Cleveland Jewish News",
            "doc_count" : 1924
          },
          {
            "key" : "AceShowbiz",
            "doc_count" : 1922
          },
          {
            "key" : "TribLIVE.com",
            "doc_count" : 1904
          },
          {
            "key" : "Leadership News",
            "doc_count" : 1891
          },
          {
            "key" : "TimesLIVE",
            "doc_count" : 1864
          },
          {
            "key" : "Karen News",
            "doc_count" : 1845
          },
          {
            "key" : "GlobeNewswire News Room",
            "doc_count" : 1844
          },
          {
            "key" : "OK! Magazine",
            "doc_count" : 1834
          },
          {
            "key" : "Infowars",
            "doc_count" : 1823
          },
          {
            "key" : "The Northern Echo",
            "doc_count" : 1812
          },
          {
            "key" : "斯塔布鲁克新闻",
            "doc_count" : 1812
          },
          {
            "key" : "NBC 7 San Diego",
            "doc_count" : 1801
          },
          {
            "key" : "Sott.net",
            "doc_count" : 1794
          },
          {
            "key" : "NBC10 Philadelphia",
            "doc_count" : 1752
          },
          {
            "key" : "BioPrepWatch",
            "doc_count" : 1750
          },
          {
            "key" : "Investing.com",
            "doc_count" : 1742
          },
          {
            "key" : "The Christian Post",
            "doc_count" : 1742
          },
          {
            "key" : "ynetnews",
            "doc_count" : 1741
          },
          {
            "key" : "Chicago Sun-Times",
            "doc_count" : 1738
          },
          {
            "key" : "Isle of Wight Radio",
            "doc_count" : 1737
          },
          {
            "key" : "加拿大的AOL(美国在线)",
            "doc_count" : 1736
          },
          {
            "key" : "NBC 6 South Florida",
            "doc_count" : 1729
          },
          {
            "key" : "Chicago Tribune",
            "doc_count" : 1728
          },
          {
            "key" : "home.nzcity.co.nz",
            "doc_count" : 1720
          },
          {
            "key" : "Capital News",
            "doc_count" : 1717
          },
          {
            "key" : "Military.com",
            "doc_count" : 1715
          },
          {
            "key" : "The Herald",
            "doc_count" : 1714
          },
          {
            "key" : "americanfreepress.net",
            "doc_count" : 1707
          },
          {
            "key" : "Lancashire Telegraph",
            "doc_count" : 1705
          },
          {
            "key" : "Naharnet",
            "doc_count" : 1702
          },
          {
            "key" : "MyCentralOregon.com",
            "doc_count" : 1699
          },
          {
            "key" : "Philippine Times",
            "doc_count" : 1691
          },
          {
            "key" : "isp.netscape.com",
            "doc_count" : 1684
          },
          {
            "key" : "Mid-day",
            "doc_count" : 1682
          },
          {
            "key" : "NBC4 Washington",
            "doc_count" : 1676
          },
          {
            "key" : "CNSNews.com",
            "doc_count" : 1668
          },
          {
            "key" : "Shore News Network",
            "doc_count" : 1665
          },
          {
            "key" : "Algemeiner.com",
            "doc_count" : 1660
          },
          {
            "key" : "skynews",
            "doc_count" : 1657
          },
          {
            "key" : "novinite.com",
            "doc_count" : 1655
          },
          {
            "key" : "The Yeshiva World",
            "doc_count" : 1654
          },
          {
            "key" : "Northlines - The Newspaper of  Substance",
            "doc_count" : 1652
          },
          {
            "key" : "The Argus",
            "doc_count" : 1650
          },
          {
            "key" : "investmentwatchblog.com",
            "doc_count" : 1645
          },
          {
            "key" : "Deccan Chronicle",
            "doc_count" : 1637
          },
          {
            "key" : "Japan Today",
            "doc_count" : 1637
          },
          {
            "key" : "NBC Bay Area",
            "doc_count" : 1629
          },
          {
            "key" : "ChronicleLive",
            "doc_count" : 1628
          },
          {
            "key" : "WSAU News/Talk 550 AM · 99.9 FM | Wausau",
            "doc_count" : 1624
          },
          {
            "key" : "tellerreport.com",
            "doc_count" : 1619
          },
          {
            "key" : "Cambodian Times",
            "doc_count" : 1592
          },
          {
            "key" : "WND",
            "doc_count" : 1587
          },
          {
            "key" : "HNGN - Headlines & Global News",
            "doc_count" : 1586
          },
          {
            "key" : "pjmedia.com",
            "doc_count" : 1583
          },
          {
            "key" : "News Channel 3-12",
            "doc_count" : 1580
          },
          {
            "key" : "Scroll.in",
            "doc_count" : 1577
          },
          {
            "key" : "WFMZ.com",
            "doc_count" : 1569
          },
          {
            "key" : "PinkNews | Latest lesbian",
            "doc_count" : 1564
          },
          {
            "key" : "Syrian Arab News Agency",
            "doc_count" : 1563
          },
          {
            "key" : "Lewiston Sun Journal",
            "doc_count" : 1555
          },
          {
            "key" : "DW.COM",
            "doc_count" : 1528
          },
          {
            "key" : "CINEMABLEND",
            "doc_count" : 1527
          },
          {
            "key" : "Arab Herald",
            "doc_count" : 1522
          },
          {
            "key" : "FijiTimes",
            "doc_count" : 1522
          },
          {
            "key" : " & Weather - Southern",
            "doc_count" : 1520
          },
          {
            "key" : " Country",
            "doc_count" : 1520
          },
          {
            "key" : "WV News",
            "doc_count" : 1517
          },
          {
            "key" : "HITC",
            "doc_count" : 1512
          },
          {
            "key" : "Yahoo Finance",
            "doc_count" : 1509
          },
          {
            "key" : "americanthinker.com",
            "doc_count" : 1509
          },
          {
            "key" : "GreekReporter.com",
            "doc_count" : 1507
          },
          {
            "key" : "Eastern Daily Press",
            "doc_count" : 1506
          },
          {
            "key" : "Jordan Times",
            "doc_count" : 1494
          },
          {
            "key" : "Mizzima Myanmar News and Insight",
            "doc_count" : 1491
          },
          {
            "key" : "Antara News",
            "doc_count" : 1485
          },
          {
            "key" : "twitchy.com",
            "doc_count" : 1485
          },
          {
            "key" : "News-Medical.net",
            "doc_count" : 1483
          },
          {
            "key" : "Baltimore Sun",
            "doc_count" : 1476
          },
          {
            "key" : "EconoTimes",
            "doc_count" : 1476
          },
          {
            "key" : "newsnow.co.uk",
            "doc_count" : 1476
          },
          {
            "key" : "gulftoday.ae",
            "doc_count" : 1474
          },
          {
            "key" : "英文台湾日报",
            "doc_count" : 1474
          },
          {
            "key" : "NOLA.com",
            "doc_count" : 1470
          },
          {
            "key" : "NECN",
            "doc_count" : 1466
          },
          {
            "key" : "福斯财经网",
            "doc_count" : 1466
          },
          {
            "key" : "Glasgow Times",
            "doc_count" : 1465
          },
          {
            "key" : "Encyclopedia Britannica",
            "doc_count" : 1463
          },
          {
            "key" : "Techdirt",
            "doc_count" : 1456
          },
          {
            "key" : "Swarajyamag",
            "doc_count" : 1453
          },
          {
            "key" : "The Asahi Shimbun",
            "doc_count" : 1453
          },
          {
            "key" : "DailyRidge.com",
            "doc_count" : 1446
          },
          {
            "key" : "Daily Herald",
            "doc_count" : 1435
          },
          {
            "key" : "商界 (菲律宾报纸)",
            "doc_count" : 1432
          },
          {
            "key" : "编年史新闻",
            "doc_count" : 1430
          },
          {
            "key" : "HeraldScotland",
            "doc_count" : 1425
          },
          {
            "key" : "autoevolution",
            "doc_count" : 1421
          },
          {
            "key" : "Bradford Telegraph and Argus",
            "doc_count" : 1419
          },
          {
            "key" : "phys.org",
            "doc_count" : 1412
          },
          {
            "key" : " gay",
            "doc_count" : 1411
          },
          {
            "key" : "HipHopDX",
            "doc_count" : 1406
          },
          {
            "key" : "Janes.com",
            "doc_count" : 1400
          },
          {
            "key" : "NottinghamshireLive",
            "doc_count" : 1397
          },
          {
            "key" : "bdnews24.com",
            "doc_count" : 1395
          },
          {
            "key" : "Celeb Dirty Laundry",
            "doc_count" : 1391
          },
          {
            "key" : "La Prensa Latina Media",
            "doc_count" : 1387
          },
          {
            "key" : "PBS NewsHour",
            "doc_count" : 1383
          },
          {
            "key" : "The Courier",
            "doc_count" : 1383
          },
          {
            "key" : "RFI",
            "doc_count" : 1379
          },
          {
            "key" : "ksl.com",
            "doc_count" : 1377
          },
          {
            "key" : "Pakistan Telegraph",
            "doc_count" : 1371
          },
          {
            "key" : "Kelowna Capital News",
            "doc_count" : 1368
          },
          {
            "key" : "The Daily News",
            "doc_count" : 1363
          },
          {
            "key" : "Kennebec Journal and Morning Sentinel",
            "doc_count" : 1362
          },
          {
            "key" : "mondaq.com",
            "doc_count" : 1352
          },
          {
            "key" : "MyNorthwest.com",
            "doc_count" : 1350
          },
          {
            "key" : "thediplomat.com",
            "doc_count" : 1348
          },
          {
            "key" : "WION",
            "doc_count" : 1344
          },
          {
            "key" : "The Chronicle",
            "doc_count" : 1340
          },
          {
            "key" : "IBTimes India",
            "doc_count" : 1338
          },
          {
            "key" : "Caledonian Record",
            "doc_count" : 1335
          },
          {
            "key" : "FOX 5 Atlanta",
            "doc_count" : 1335
          },
          {
            "key" : "foreignaffairs.co.nz",
            "doc_count" : 1330
          },
          {
            "key" : "Beijing Bulletin",
            "doc_count" : 1329
          },
          {
            "key" : "东非人报",
            "doc_count" : 1325
          },
          {
            "key" : "1330 & 101.5 WHBL",
            "doc_count" : 1324
          },
          {
            "key" : "The Good Men Project",
            "doc_count" : 1321
          },
          {
            "key" : "markets.businessinsider.com",
            "doc_count" : 1319
          },
          {
            "key" : "News",
            "doc_count" : 1318
          },
          {
            "key" : "伊洛瓦底",
            "doc_count" : 1318
          },
          {
            "key" : "TheWrap",
            "doc_count" : 1316
          },
          {
            "key" : "BizPac Review",
            "doc_count" : 1312
          },
          {
            "key" : "York Press",
            "doc_count" : 1310
          },
          {
            "key" : "Daily Republic",
            "doc_count" : 1308
          },
          {
            "key" : "pmnewsnigeria.com",
            "doc_count" : 1304
          },
          {
            "key" : "https://www.outlookindia.com/",
            "doc_count" : 1302
          },
          {
            "key" : "newsx.com",
            "doc_count" : 1302
          },
          {
            "key" : "The Atlantic",
            "doc_count" : 1299
          },
          {
            "key" : " World News and Opinion.",
            "doc_count" : 1295
          },
          {
            "key" : "The Economist",
            "doc_count" : 1292
          },
          {
            "key" : "Morningstar",
            "doc_count" : 1290
          },
          {
            "key" : "ARY NEWS",
            "doc_count" : 1288
          },
          {
            "key" : "News and Star",
            "doc_count" : 1288
          },
          {
            "key" : "Buffalo News",
            "doc_count" : 1287
          },
          {
            "key" : "Ghanaian Times",
            "doc_count" : 1278
          },
          {
            "key" : "SBS News",
            "doc_count" : 1278
          },
          {
            "key" : "850 WFTL",
            "doc_count" : 1277
          },
          {
            "key" : "South Wales Argus",
            "doc_count" : 1277
          },
          {
            "key" : "Vernon Morning Star",
            "doc_count" : 1272
          },
          {
            "key" : "ucanews.com",
            "doc_count" : 1272
          },
          {
            "key" : "The Bobby Bones Show",
            "doc_count" : 1270
          },
          {
            "key" : "navhindtimes.in",
            "doc_count" : 1270
          },
          {
            "key" : "The Week",
            "doc_count" : 1269
          },
          {
            "key" : "GOV.UK",
            "doc_count" : 1268
          },
          {
            "key" : "Al Bawaba",
            "doc_count" : 1264
          },
          {
            "key" : "Hope Standard",
            "doc_count" : 1263
          },
          {
            "key" : "苏城日报",
            "doc_count" : 1263
          },
          {
            "key" : "Latin Post - Latin news",
            "doc_count" : 1260
          },
          {
            "key" : "radio.gov.pk",
            "doc_count" : 1253
          },
          {
            "key" : "Albany Herald",
            "doc_count" : 1248
          },
          {
            "key" : "catholicworldreport.com",
            "doc_count" : 1248
          },
          {
            "key" : "亚利桑那每日星报",
            "doc_count" : 1248
          },
          {
            "key" : "KELO-AM",
            "doc_count" : 1247
          },
          {
            "key" : "NPR.org",
            "doc_count" : 1247
          },
          {
            "key" : "SUNSTAR",
            "doc_count" : 1246
          },
          {
            "key" : "Connect FM | Local News Radio | Dubois",
            "doc_count" : 1240
          },
          {
            "key" : "Yahoo Entertainment",
            "doc_count" : 1237
          },
          {
            "key" : "Belarusian Telegraph Agency",
            "doc_count" : 1232
          },
          {
            "key" : "Crooks and Liars",
            "doc_count" : 1232
          },
          {
            "key" : "The Fandom Post",
            "doc_count" : 1231
          },
          {
            "key" : "Agassiz Harrison Observer",
            "doc_count" : 1228
          },
          {
            "key" : "Eye Radio",
            "doc_count" : 1228
          },
          {
            "key" : "The Advocate",
            "doc_count" : 1228
          },
          {
            "key" : "Trinidad and Tobago Newsday -",
            "doc_count" : 1224
          },
          {
            "key" : "Middle East Star",
            "doc_count" : 1223
          },
          {
            "key" : "TeessideLive",
            "doc_count" : 1218
          },
          {
            "key" : "FOX 32 Chicago",
            "doc_count" : 1215
          },
          {
            "key" : "Echo",
            "doc_count" : 1214
          },
          {
            "key" : "INFOnews",
            "doc_count" : 1213
          },
          {
            "key" : "Orlando Sentinel",
            "doc_count" : 1213
          },
          {
            "key" : "StokeonTrentLive",
            "doc_count" : 1212
          },
          {
            "key" : "VietnamPlus",
            "doc_count" : 1208
          },
          {
            "key" : "LeicestershireLive",
            "doc_count" : 1205
          },
          {
            "key" : "MMA News",
            "doc_count" : 1203
          },
          {
            "key" : "Worldcrunch",
            "doc_count" : 1200
          },
          {
            "key" : "Charleston Gazette-Mail",
            "doc_count" : 1196
          },
          {
            "key" : "Ground News",
            "doc_count" : 1196
          },
          {
            "key" : "Anime",
            "doc_count" : 1193
          },
          {
            "key" : "CP24",
            "doc_count" : 1186
          },
          {
            "key" : "article.wn.com",
            "doc_count" : 1175
          },
          {
            "key" : "Mission City Record",
            "doc_count" : 1174
          },
          {
            "key" : " immigration",
            "doc_count" : 1167
          },
          {
            "key" : " politics",
            "doc_count" : 1167
          },
          {
            "key" : "Salmon Arm Observer",
            "doc_count" : 1167
          },
          {
            "key" : "WKBT",
            "doc_count" : 1166
          },
          {
            "key" : "NewZimbabwe.com",
            "doc_count" : 1165
          },
          {
            "key" : "Perez Hilton",
            "doc_count" : 1164
          },
          {
            "key" : "The Lawton Constitution",
            "doc_count" : 1164
          },
          {
            "key" : "The News-Gazette",
            "doc_count" : 1164
          },
          {
            "key" : "phnompenhpost.com",
            "doc_count" : 1163
          },
          {
            "key" : "The Business Standard",
            "doc_count" : 1160
          },
          {
            "key" : "有线电视新闻网",
            "doc_count" : 1157
          },
          {
            "key" : "http://www.radiojamaicanewsonline.com",
            "doc_count" : 1148
          },
          {
            "key" : " Health",
            "doc_count" : 1146
          },
          {
            "key" : "Sun Sentinel",
            "doc_count" : 1146
          },
          {
            "key" : "Times of India Blog",
            "doc_count" : 1144
          },
          {
            "key" : "International Business Times UK",
            "doc_count" : 1142
          },
          {
            "key" : "Kuwait Times",
            "doc_count" : 1142
          },
          {
            "key" : "Trinidad Express Newspapers",
            "doc_count" : 1141
          },
          {
            "key" : "The Bolton News",
            "doc_count" : 1136
          },
          {
            "key" : "eWrestlingNews.com",
            "doc_count" : 1136
          },
          {
            "key" : "tribune242.com",
            "doc_count" : 1136
          },
          {
            "key" : "TAP",
            "doc_count" : 1134
          },
          {
            "key" : "erienewsnow.com",
            "doc_count" : 1134
          },
          {
            "key" : "The Media Line",
            "doc_count" : 1131
          },
          {
            "key" : "indiatvnews.com",
            "doc_count" : 1127
          },
          {
            "key" : "OilPrice.com",
            "doc_count" : 1123
          },
          {
            "key" : "theday.com",
            "doc_count" : 1119
          },
          {
            "key" : "TOLOnews",
            "doc_count" : 1118
          },
          {
            "key" : "Entertainment Tonight",
            "doc_count" : 1115
          },
          {
            "key" : "HuffPost UK",
            "doc_count" : 1114
          },
          {
            "key" : "MSNBC.com",
            "doc_count" : 1114
          },
          {
            "key" : "哈特福时报",
            "doc_count" : 1113
          },
          {
            "key" : "WDRB",
            "doc_count" : 1105
          },
          {
            "key" : " and American Proud!",
            "doc_count" : 1098
          },
          {
            "key" : "The National Law Review",
            "doc_count" : 1090
          },
          {
            "key" : "Kat Country 98.9 | KTCO",
            "doc_count" : 1087
          },
          {
            "key" : "Watermark Online",
            "doc_count" : 1087
          },
          {
            "key" : "海峡时报",
            "doc_count" : 1084
          },
          {
            "key" : "Iraqi News",
            "doc_count" : 1082
          },
          {
            "key" : "Al Mayadeen English",
            "doc_count" : 1081
          },
          {
            "key" : "Daily Echo",
            "doc_count" : 1081
          },
          {
            "key" : "www.independentsentinel.com",
            "doc_count" : 1078
          },
          {
            "key" : "The Killeen Daily Herald",
            "doc_count" : 1077
          },
          {
            "key" : "Macau Business",
            "doc_count" : 1074
          },
          {
            "key" : "Medscape",
            "doc_count" : 1074
          },
          {
            "key" : "wallstreet-online.de",
            "doc_count" : 1074
          },
          {
            "key" : "timesfreepress.com",
            "doc_count" : 1072
          },
          {
            "key" : "Stiri pe surse",
            "doc_count" : 1071
          },
          {
            "key" : "BOL News",
            "doc_count" : 1069
          },
          {
            "key" : "Democracy Now!",
            "doc_count" : 1068
          },
          {
            "key" : "investegate.co.uk",
            "doc_count" : 1066
          },
          {
            "key" : "BelfastLive",
            "doc_count" : 1064
          },
          {
            "key" : "thisisoxfordshire",
            "doc_count" : 1064
          },
          {
            "key" : "Shanghai Sun",
            "doc_count" : 1061
          },
          {
            "key" : "CoventryLive",
            "doc_count" : 1060
          },
          {
            "key" : "MercoPress",
            "doc_count" : 1058
          },
          {
            "key" : "SWI swissinfo.ch",
            "doc_count" : 1057
          },
          {
            "key" : "Radio Free Asia",
            "doc_count" : 1056
          },
          {
            "key" : "The UK News",
            "doc_count" : 1056
          },
          {
            "key" : "northafricapost.com",
            "doc_count" : 1056
          },
          {
            "key" : "East Anglian Daily Times",
            "doc_count" : 1052
          },
          {
            "key" : "Frontpage Mag",
            "doc_count" : 1052
          },
          {
            "key" : "CounterPunch.org",
            "doc_count" : 1051
          },
          {
            "key" : "The Leader",
            "doc_count" : 1050
          },
          {
            "key" : "Deseret News",
            "doc_count" : 1047
          },
          {
            "key" : "vladtv.com",
            "doc_count" : 1046
          },
          {
            "key" : "Las Vegas Sun",
            "doc_count" : 1043
          },
          {
            "key" : " Stevens Point",
            "doc_count" : 1042
          },
          {
            "key" : "News/Talk 1540 KXEL",
            "doc_count" : 1039
          }
        ]

is_write = 0
f = open('data/all_media_count.csv', 'w', encoding='utf-8')
f.write('mediaNameZh,count\n')
db = pymysql.connect(host='192.168.12.222', user='root', password='123456', database='data_service', charset='utf8')
cursor = db.cursor()
cursor.execute("select `domain`,`mediaNameZh` from t_media_info")
result = cursor.fetchall()
# for domain_name in result:
#     print(domain_name)
#     domain = domain_name[0]
#     name = domain_name[1]
#     for data in data_list:
#         if data['key'] == name:
#             f.write(name + ',' + domain + ',' + str(data['doc_count']) + '\n')
#             is_write = 1
#             break

for data in data_list:
    name = data['key']
    count = data['doc_count']
    f.write(name + ',' + str(data['doc_count']) + '\n')
