# -*- coding:UTF-8 -*-
# @Time    : 24.10.9 16:24
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : 1688_spider.py
# @Project : pythonScript
# @Software: PyCharm
import requests
from loguru import logger
from lxml import etree
from pypinyin import pinyin, Style
import redis
import random
import hashlib
import pymysql
import urllib.parse


class L688Spider(object):

    def __init__(self):
        self.cookies = {
            'cna': 'GdyMH/Ws+n4CAXa6C6ejkDPJ',
        }
        self.pool = redis.ConnectionPool(host="10.32.51.2", port=6379, db=15, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        self.proxy = self.get_proxy()
        self.db_doris = pymysql.connect(host="10.32.49.61", user="yangxin", password="yangxinQ123", database="ods", charset="utf8", port=9030)
        self.cursor_doris = self.db_doris.cursor()
        self.cursor_doris.execute("""
           CREATE TABLE IF NOT EXISTS ods.ods_1688_company_info (
            `company_name` VARCHAR COMMENT '企业名称',
            `keyword` VARCHAR COMMENT '检索关键词',
            `boss_online` VARCHAR COMMENT '厂长在线状态',
            `buyer_will_cnt` VARCHAR COMMENT '买家意向数量',
            `city` VARCHAR COMMENT '城市',
            `compliance_rate` VARCHAR COMMENT '合规率',
            `employees_count` VARCHAR COMMENT '员工数量',
            `eurl` VARCHAR COMMENT '企业网址',
            `factory_cover_pic` VARCHAR COMMENT '工厂封面图',
            `factory_detail_url` VARCHAR COMMENT '工厂详情链接',
            `factory_level` VARCHAR COMMENT '工厂等级',
            `factory_panorc_pic` VARCHAR COMMENT '工厂全景图',
            `factory_size` VARCHAR COMMENT '工厂规模',
            `impression_eurl` VARCHAR COMMENT '印象网址',
            `is_factory` VARCHAR COMMENT '是否工厂',
            `login_id` VARCHAR COMMENT '登录ID',
            `member_tags` VARCHAR COMMENT '会员标签',
            `offer_id` VARCHAR COMMENT '产品ID',
            `owner_avatar` VARCHAR COMMENT '负责人头像',
            `production_service` VARCHAR COMMENT '生产服务',
            `p4p_track_info` VARCHAR COMMENT 'P4P追踪信息',
            `pic_url` VARCHAR COMMENT '图片链接',
            `repeat_rate` VARCHAR COMMENT '复购率',
            `safe_purchase` VARCHAR COMMENT '安全购买',
            `send_days` VARCHAR COMMENT '发货天数',
            `shili` VARCHAR COMMENT '实力',
            `short_video_cover` VARCHAR COMMENT '短视频封面',
            `short_video_duration` VARCHAR COMMENT '短视频时长',
            `short_video_url` VARCHAR COMMENT '短视频链接',
            `show_rectangle_img` VARCHAR COMMENT '是否展示矩形图',
            `super_factory` VARCHAR COMMENT '超级工厂',
            `tp_num` VARCHAR COMMENT 'TP数量',
            `tp_service_year` VARCHAR COMMENT 'TP服务年限',
            `type` VARCHAR COMMENT '企业类型',
            `user_id` VARCHAR COMMENT '用户ID',
            `ww_response_rate` VARCHAR COMMENT '旺旺响应率',
            `create_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            `update_time` DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
        ) ENGINE=OLAP
        UNIQUE KEY(`company_name`, `keyword`)
        COMMENT "1688-公司信息表"
        DISTRIBUTED BY HASH(`company_name`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.online: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
        """)

    def start_request(self):
        with open('data/澄海玩具名单.txt', 'r', encoding='utf-8') as f:
            company_names = f.readlines()
        for company_name in company_names:
            begin_page = 1
            self.parse(company_name, begin_page)

    def parse(self, company_name, begin_page):
        # company_name = '乐迪玩具'
        company_name_encode = self.url_encode_string(company_name)
        url = f'https://search.1688.com/service/companyInfoSearchDataService?keywords={company_name_encode}&beginPage={begin_page}&pageSize=20&startIndex=6&pageName=findPCFactory'
        try:
            response = self.get_response(url)
            try:
                data_list = response.json().get('data').get('data').get('companyWithOfferLists')
            except:
                data_list = []
            if data_list:
                for data in data_list:
                    self.parse_data(data, company_name, begin_page)
                # begin_page += 1
                # self.parse(company_name, begin_page)
            else:
                logger.error(f'{company_name} 采集完毕！')
        except Exception as e:
            logger.error(f'[1688Spider]解析公司列表页失败，请求网址：{url}，失败原因：{e}\n')

    def parse_data(self, item, company_name, begin_page):
        try:
            factory_info = item.get('factoryInfo', {})
            if factory_info:
                data = {}
                data['keyword'] = company_name.strip()
                data['boss_online'] = str(factory_info.get('bossOnline', ''))
                data['buyer_will_cnt'] = factory_info.get('buyerWillCnt', '')
                data['city'] = factory_info.get('city', '')
                data['company_name'] = factory_info.get('company', '')
                data['compliance_rate'] = factory_info.get('complianceRate', '')
                data['employees_count'] = factory_info.get('employeesCount', '')
                data['eurl'] = factory_info.get('eurl', '')
                data['factory_cover_pic'] = factory_info.get('factoryCoverPic', '')
                data['factory_detail_url'] = factory_info.get('factoryDetailUrl', '')
                data['factory_level'] = factory_info.get('factoryLevel', '')
                data['factory_panorc_pic'] = factory_info.get('factoryPanorcPic', '')
                data['factory_size'] = factory_info.get('factorySize', '')
                data['impression_eurl'] = factory_info.get('impressionEurl', '')
                data['is_factory'] = factory_info.get('isFactory', '')
                data['login_id'] = factory_info.get('loginId', '')
                data['member_tags'] = factory_info.get('memberTags', '')
                data['offer_id'] = str(factory_info.get('offerId', ''))
                data['owner_avatar'] = factory_info.get('ownerAvatar', '')
                data['production_service'] = factory_info.get('productionService', '')
                data['p4p_track_info'] = factory_info.get('p4pTrackInfo', '')
                data['pic_url'] = factory_info.get('picUrl', '')
                data['repeat_rate'] = factory_info.get('repeatRate', '')
                data['safe_purchase'] = str(factory_info.get('safePurchase', ''))
                data['send_days'] = str(factory_info.get('sendDays', ''))
                data['shili'] = str(factory_info.get('shili', ''))
                data['short_video_cover'] = factory_info.get('shortVideoCover', '')
                data['short_video_duration'] = factory_info.get('shortVideoDuration', '')
                data['short_video_url'] = factory_info.get('shortVideoUrl', '')
                data['show_rectangle_img'] = str(factory_info.get('showRectangleImg', ''))
                data['super_factory'] = str(factory_info.get('superFactory', ''))
                data['tp_num'] = str(factory_info.get('tpNum', ''))
                data['tp_service_year'] = str(factory_info.get('tpServiceYear', ''))
                data['type'] = factory_info.get('type', '')
                data['user_id'] = factory_info.get('userId', '')
                data['ww_response_rate'] = factory_info.get('wwResponseRate', '')
                print(data)
                self.save_data(data, 'ods_1688_company_info')
        except Exception as e:
            logger.error(f'[1688Spider]解析公司列表页失败，失败公司：{company_name}，失败页数：{begin_page}，失败原因：{e}\n')

    def save_data(self, item, table_name):
        try:
            # 获取表的列名
            self.cursor_doris.execute(f'desc `{table_name}`')
            columns = [row[0] for row in self.cursor_doris.fetchall()]
            # 准备插入数据
            data = {}
            for column in columns:
                if column in item:
                    data[column] = item[column]
            if not data:
                logger.error(f'【{table_name}】映射数据表为空：{item}')
            # 构建SQL语句
            columns_str = ", ".join(f"`{col}`" for col in data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            values = tuple(data.values())
            # 执行SQL插入
            self.cursor_doris.execute(sql, values)
            self.db_doris.commit()
            logger.info(f"【{table_name}】数据插入到 Tidb 成功!")
        except Exception as e:
            logger.error(f'【{table_name}】插入数据失败，失败原因：{e}')

    def get_response(self, url):
        fail_num = 0
        while True:
            if fail_num >= 100:
                logger.warning(f'代理池连续失败100次，本次请求不设置代理')
                return requests.get(url, headers=self.headers)
            else:
                try:
                    res = requests.get(url, cookies=self.cookies, proxies=self.proxy, timeout=10)
                    try:
                        text = res.content.decode('utf-8')
                    except:
                        text = res.content.decode('gbk')
                    if 'Too Many Requests' in text or '请验证' in text or res.status_code >= 300:
                        logger.warning(f'{url} 访问失败，失败计数+1，更换代理重新请求中...')
                        fail_num += 1
                        self.proxy = self.get_proxy()
                    else:
                        return res
                except:
                    logger.warning(f'{url} 访问失败，失败计数+1，更换代理重新请求中...')
                    fail_num += 1
                    self.proxy = self.get_proxy()

    # 获取代理ip
    def get_proxy(self):
        proxies_list = self.r.zrange("WholeJuLiangDaiLi", 0, -1)
        random_number = random.randint(0, len(proxies_list) - 1)
        ip = proxies_list[random_number].split('@')[1]
        return {"http": f'http://{ip}', "https": f'http://{ip}'}

    # 获取md5加密字符串
    @staticmethod
    def md5_string(in_str):
        md5 = hashlib.md5()
        md5.update(in_str.encode("utf8"))
        result = md5.hexdigest()
        return result

    @staticmethod
    def url_encode_string(in_str):
        try:
            # 先将字符串编码为 GB2312 字节
            gb2312_encoded_bytes = in_str.encode('gb2312')
            # 然后进行 URL 编码
            encoded_string = urllib.parse.quote(gb2312_encoded_bytes)
            return encoded_string
        except:
            return in_str

    def run(self):
        self.start_request()


if __name__ == '__main__':
    l688 = L688Spider()
    l688.start_request()
