# -*- coding:UTF-8 -*-
# @Time    : 2024/10/23 09:37
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : ticaiku_spider.py
# @Project : pythonScript
# @Software: PyCharm
import requests
import pymysql

headers = {
    'Authorization': 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJsb2dpbl91c2VyX2tleSI6Ind4X21pbmlfYXBwOjI1MjA2NDplMmZkMjEzNy0yNzc2LTQ5ODMtOWUzOS1kMWQ1Yzk5ZjY0N2YifQ.sD9kNaZz3UdLeeeMFUkuOcxDG3tCLDXeWfaLvYzd54LfsmyU5h_c5KLYlA8faA6y24l0iTKXPf2nDlOZluTIcA',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090c11)XWEB/11275',
    'Content-Type': 'application/json',
    'Accept': '*/*',
}
connection = pymysql.connect(host="127.0.0.1", port=3306, user='root', password="Root@123456", database='economic_news', charset='utf8')
cursor = connection.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS `theme` (
   `id` INT AUTO_INCREMENT COMMENT '主键ID',
    `ancestors` VARCHAR(255) NOT NULL COMMENT '祖先节点',
    `createBy` VARCHAR(255) DEFAULT NULL COMMENT '创建者',
    `createTime` DATETIME DEFAULT NULL COMMENT '创建时间',
    `detail` TEXT DEFAULT NULL COMMENT '详细信息',
    `firstLetter` VARCHAR(10) NOT NULL COMMENT '首字母',
    `importance` INT NOT NULL COMMENT '重要性',
    `leadTimes` VARCHAR(255) DEFAULT NULL COMMENT '领先时间',
    `level` INT NOT NULL COMMENT '级别',
    `limitUpTimes` VARCHAR(255) DEFAULT NULL COMMENT '涨停次数',
    `name` VARCHAR(255) NOT NULL COMMENT '名称',
    `parentId` INT NOT NULL COMMENT '父级ID',
    `pctChg` DECIMAL(5, 2) NOT NULL COMMENT '涨幅',
    `reason` TEXT NOT NULL COMMENT '原因',
    `remark` TEXT DEFAULT NULL COMMENT '备注',
    `sort` INT NOT NULL COMMENT '排序',
    `status` VARCHAR(10) NOT NULL COMMENT '状态',
    `stockCount` INT DEFAULT NULL COMMENT '股票数量',
    `subjectId` BIGINT NOT NULL COMMENT '主题ID',
    `type` VARCHAR(255) DEFAULT NULL COMMENT '类型',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `UNI_ID` (`parentId`, `subjectId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci AUTO_INCREMENT=1 COMMENT='题材库-题材类目信息采集';""")

cursor.execute('''CREATE TABLE IF NOT EXISTS `theme_children` (
    `id` INT AUTO_INCREMENT COMMENT '主键ID',
    `ancestors` VARCHAR(255) NOT NULL COMMENT '祖先节点',
    `createBy` VARCHAR(255) DEFAULT NULL COMMENT '创建者',
    `createTime` DATETIME DEFAULT NULL COMMENT '创建时间',
    `detail` TEXT DEFAULT NULL COMMENT '详细信息',
    `firstLetter` VARCHAR(10) NOT NULL COMMENT '首字母',
    `importance` INT NOT NULL COMMENT '重要性',
    `leadTimes` VARCHAR(255) DEFAULT NULL COMMENT '领先时间',
    `level` INT NOT NULL COMMENT '级别',
    `limitUpTimes` VARCHAR(255) DEFAULT NULL COMMENT '涨停次数',
    `name` VARCHAR(255) NOT NULL COMMENT '名称',
    `parentId` BIGINT NOT NULL COMMENT '父级ID',
    `parentName` VARCHAR(255) DEFAULT NULL COMMENT '父级名称',
    `pctChg` DECIMAL(5, 2) NOT NULL COMMENT '涨幅',
    `reason` TEXT NOT NULL COMMENT '原因',
    `remark` TEXT DEFAULT NULL COMMENT '备注',
    `selectReason` TEXT DEFAULT NULL COMMENT '选择原因',
    `sort` INT NOT NULL COMMENT '排序',
    `status` VARCHAR(10) NOT NULL COMMENT '状态',
    `stockCount` INT DEFAULT NULL COMMENT '股票数量',
    `stocks` JSON DEFAULT NULL COMMENT '股票信息',
    `subjectId` BIGINT NOT NULL COMMENT '主题ID',
    `type` VARCHAR(255) DEFAULT NULL COMMENT '类型',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `UNI_ID` (`parentId`, `subjectId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci AUTO_INCREMENT=1 COMMENT='题材库-子主题信息';''')

cursor.execute('''CREATE TABLE IF NOT EXISTS `theme_detail` (
    `id` INT AUTO_INCREMENT COMMENT '主键ID',
    `ancestors` VARCHAR(255) NOT NULL COMMENT '祖先节点',
    `createBy` VARCHAR(255) DEFAULT NULL COMMENT '创建者',
    `createTime` DATETIME DEFAULT NULL COMMENT '创建时间',
    `detail` TEXT DEFAULT NULL COMMENT '详细信息',
    `firstLetter` VARCHAR(10) NOT NULL COMMENT '首字母',
    `importance` INT NOT NULL COMMENT '重要性',
    `leadTimes` VARCHAR(255) DEFAULT NULL COMMENT '领先时间',
    `level` INT NOT NULL COMMENT '级别',
    `limitUpTimes` VARCHAR(255) DEFAULT NULL COMMENT '涨停次数',
    `name` VARCHAR(255) NOT NULL COMMENT '名称',
    `parentId` INT NOT NULL COMMENT '父级ID',
    `parentName` VARCHAR(255) DEFAULT NULL COMMENT '父级名称',
    `pctChg` DECIMAL(5, 2) NOT NULL COMMENT '涨幅',
    `reason` TEXT NOT NULL COMMENT '原因',
    `remark` TEXT DEFAULT NULL COMMENT '备注',
    `selectReason` TEXT DEFAULT NULL COMMENT '选择原因',
    `sort` INT NOT NULL COMMENT '排序',
    `status` VARCHAR(10) NOT NULL COMMENT '状态',
    `stockCount` INT DEFAULT NULL COMMENT '股票数量',
    `subjectId` BIGINT NOT NULL COMMENT '主题ID',
    `type` VARCHAR(255) DEFAULT NULL COMMENT '类型',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `UNI_ID` (`parentId`, `subjectId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci AUTO_INCREMENT=1 COMMENT='题材库-题材详细信息';''')

cursor.execute('''CREATE TABLE IF NOT EXISTS `stock_detail` (
    `id` INT AUTO_INCREMENT COMMENT '主键ID',
    `createTime` DATETIME NOT NULL COMMENT '创建时间',
    `importance` INT NOT NULL COMMENT '重要性',
    `name` VARCHAR(255) NOT NULL COMMENT '股票名称',
    `pctChg` DECIMAL(5, 2) NOT NULL COMMENT '涨幅',
    `reason` TEXT NOT NULL COMMENT '原因',
    `remark` TEXT DEFAULT NULL COMMENT '备注',
    `selectedId` INT NOT NULL COMMENT '选中ID',
    `sort` INT NOT NULL COMMENT '排序',
    `stockId` VARCHAR(20) NOT NULL COMMENT '股票ID',
    `subjectId` BIGINT NOT NULL COMMENT '主题ID',
    `top` INT NOT NULL COMMENT '是否置顶',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `UNI_ID` (`stockId`, `subjectId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci AUTO_INCREMENT=1 COMMENT='题材库-股票详细信息';''')


def save_data(item, table_name):
    # 获取表的列名
    cursor.execute(f'desc `{table_name}`')
    columns = [row[0] for row in cursor.fetchall()]
    # 准备插入数据
    data = {}
    for column in columns:
        if column in item:
            data[column] = item[column]
    if not data:
        print(f'{table_name}：映射数据表为空：{item}')
    # 构建SQL语句
    columns_str = ", ".join(f"`{col}`" for col in data.keys())
    placeholders = ", ".join(["%s"] * len(data))

    # 构建ON DUPLICATE KEY UPDATE部分
    update_str = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in data.keys()])

    sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"
    values = tuple(data.values())
    # 执行SQL插入
    try:
        cursor.execute(sql, values)
        connection.commit()
    except Exception as e:
        raise e


def get_theme():
    url = 'https://miniapp.guniuniu.com/api/app/subject/list/'
    res = requests.get(url, headers=headers)
    for data in res.json().get('data'):
        save_data(data, 'theme')

        name = data.get('name', '')
        subject_id = data.get('subjectId', '')
        if subject_id:
            url = f'https://miniapp.guniuniu.com/api/app/subject/child-tree/{subject_id}'
            print(f'{name} 开始采集...')
            get_sub_theme(url, name)
            print(f'{name} 采集完成！')


def get_sub_theme(url, name):
    res = requests.get(url, headers=headers)
    for data in res.json().get('data'):
        save_data(data, 'theme_children')

        sub_name = data.get('name', '')
        subject_id = data.get('subjectId', '')
        if subject_id:
            url = f'https://miniapp.guniuniu.com/api/app/subject/child-stock-tree/{subject_id}'
            print(f'{name}--{sub_name} 开始采集...')
            get_theme_detail(url)
            print(f'{name}--{sub_name} 采集完成！')


def get_theme_detail(url):
    res = requests.get(url, headers=headers)
    for data in res.json().get('data'):
        for children_data in data.get('children'):
            stock_datas = children_data.pop('stocks')
            for stock_data in stock_datas:
                save_data(stock_data, 'stock_detail')

            save_data(children_data, 'theme_detail')


if __name__ == '__main__':
    get_theme()
