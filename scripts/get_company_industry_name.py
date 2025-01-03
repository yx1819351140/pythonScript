# -*- coding:UTF-8 -*-
# @Time    : 24.8.22 14:14
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : get_company_industry_name.py
# @Project : pythonScript
# @Software: PyCharm
import requests
import pymysql


def get_industry_name():
    url = "https://api.openai.com/v1/chat/completions"  # 可以替换为任何代理的接口
    OPENAI_API_KEY = ""  # openai官网获取key
    header = {"Content-Type": "application/json", "Authorization": "Bearer " + OPENAI_API_KEY}
    data = {
        "model": "gpt-3.5-turbo",
        # "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "给你一批公司，用你的判断来给我返回对应的行业分类，只返回json格式的文本数据，格式为：[{\"company_name\": \"四川川南减震器集团有限公司\", \"industry_name\": \"汽车零部件, 减震器, 机械制造\"},{\"company_name\": \"江苏恒瑞医药股份有限公司\", \"industry_name\": \"制药, 生物制药, 医疗器械, 临床试验\"},{\"company_name\": \"京东方现代（北京）显示技术有限公司 \", \"industry_name\": \"显示技术, 半导体, 消费电子, 工业显示\"},]"
            },
            {
                "role": "user",
                "content": "北京鑫盛源通科技有限公司,北京中天恒晟文化传媒有限公司,北京旺有源科技发展有限公司,北京市宏力兴汽车维修有限公司,北京中奥创业进出口贸易有限公司,中国远洋运输有限公司,智鼎国研（北京）信息咨询有限公司,北京信诺快捷物流有限公司,北京欣欣东成物业服务有限公司,北京海坦基业电器有限公司,曙光五星（北京）商贸有限公司,北京宝成建业科贸有限公司,锐格（北京）国际广告有限公司,易贸丰源国际贸易（北京）有限责任公司,北京夏都百泸商贸有限公司,北京新雅房地产开发咨询服务公司,天津通新置业有限公司,北京市惠康民社区服务有限责任公司,北京富多鑫商贸中心,北京中涂华北化工产品贸易有限公司,北京日新达工程技术有限公司,北京盛发房地产开发有限公司,北京华翰文苑文化发展有限公司,北京易达风科技发展有限公司,北京今赢世际旅游文化有限公司第二分公司,北京金日佳信科技开发有限责任公司,北京富利雅科贸有限公司河北分公司,北京环亚大有文化发展有限公司,北京卓盛一品文化传媒有限公司,北京市法早传媒广告有限公司,中亚开阳（北京）广告有限公司,北京华数文化传媒有限公司,北京科达恒睿科技有限公司,北京国天利达科技发展有限公司,北京亚泰阳光科技发展有限公司,北京市通州区红旗印刷有限公司,北京缘来欣欣生鲜超市有限公司,北京金汉狮知识产权服务有限公司,北京鸿飞世纪科贸有限责任公司,北京杰淋机电设备科技开发中心,北京弘方德祥建筑装饰有限公司,山东省房地产开发集团青岛公司,北京乐柏奇科技有限公司,北京康普特文化发展公司,北京金圣益工贸有限公司,北京爱牧技术开发有限公司,新拓尼克（北京）科技研发中心有限公司,天津安达集团股份有限公司,北京鑫立翔宇商贸有限公司,北京市顺义医药药材有限公司瑞丰堂医保全新大药房,北京时代友信家具有限责任公司,北京凤翔投资策划有限公司,北京蓝海传奇科技有限责任公司,北京亿信互动网络科技有限公司,北京康牧众诚动物药品有限公司,北京张坊磨盘科技发展有限公司,北京市昌平县京阳综合加工厂,北京嘉腾企业管理咨询有限公司,北京许继金业电气有限公司,北京环宇宏业科技开发有限公司,北京嘉德新业科技发展有限公司,中国进出口银行,中纺信远资产管理有限公司,北京德诚国信生物科技有限公司,北京妃思客栈有限公司,北京链家置地房地产经纪有限公司东城安乐林路第一分店,北京科富兴科技有限公司,北京洛讯科技发展有限公司,北京市昌盛工程机械修理厂,北京欣比克电气设备有限责任公司,北京万松林文化发展有限公司,北京华泰物业管理有限责任公司,北京维尔森科技发展有限公司,北京世纳广告有限责任公司,北京诺佳华成商贸有限公司,北京东方鸿运租赁有限公司东窑设备租赁分公司,东方大洋（北京）国际贸易有限公司,北京吉昌食用菌有限公司,北京展鹏仓储物流有限公司,北京华都畜牧贸易公司饲料加工厂,北京振华服装厂,北京金石湾投资有限公司,北京诚信之旅汽车租赁有限公司,北京康健源科技有限公司,北京和联天海物业管理有限责任公司,北京汉邦无限科技有限公司,陕西建工金牛集团股份有限公司,中建云通科技（北京）有限公司,北京中高德汇投资管理中心（有限合伙）,北京正友首福国际贸易有限责任公司,中营天祥（北京）健康管理有限公司,北京中奥大巢国际服饰有限公司,陕西华凌科贸有限公司,北京百纳新艺文化发展有限公司,北京永辉商业有限公司,北京欣龙华成商贸有限公司,北京深蓝兄弟网络科技有限公司,北京美松体育文化发展有限公司,北京市熊猫烟花有限公司房山区第七十零售点,北京优盾环境工程有限公司"
            }
        ],
        "temperature": 0,
        "stream": False
    }
    response = requests.post(url=url, headers=header, json=data, proxies={'https':'127.0.0.1:7890'}).json()
    print(response)
    return response


if __name__ == '__main__':
    get_industry_name()