# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> dapengjiaoyu_m3u8_spider         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2022/12/29 11:38
# @Software : win10 python3.6
"""
大鹏教育爬虫，将解密m3u8中AES-128加密的ts文件
"""
import re
import subprocess
import time
import requests
import json
import os


class DaPengSpider(object):

    def __init__(self):
        self.path = 'C:/Users/yang/Downloads/大鹏教育/'
        # 需要登录手动复制cookie
        self.headers = {'Cookie': '_uab_collina=167222057824439104834507; Hm_lvt_92a5d4e0ba2140a5aa6001c88a65ef97=1672220581; Hm_lvt_5253ded03765ddd71ca75302ab1e548d=1672220581; looyu_id=af43aec9fd7f94262618c4f88deecd7b_20004236%3A1; redirect_url=https%3A%2F%2Fwww.dapengjiaoyu.cn%2F; dptoken=82ade978-a50a-4ffb-85e1-21b0dcb09424; userinfo=%7B%22userId%22%3A%22knloia2xxz%22%2C%22nickname%22%3A%22%E6%B7%BB%E6%98%9F%E8%BF%BD%E8%89%BA%22%2C%22avatar%22%3A%22https%3A%2F%2Fimage.dapengjiaoyu.com%2Fimages%2Favatars%2F48avatar.jpg%22%2C%22dpAccount%22%3A%22dp90206689%22%2C%22mobile%22%3A%2218943692469%22%2C%22loginName%22%3A%22%E6%B7%BB%E6%98%9F%E8%BF%BD%E8%89%BA%22%2C%22studentSatusId%22%3A2021042101187%7D; _99_mon=%5B0%2C0%2C0%5D; _pk_ref.3.fd4d=%5B%22%22%2C%22%22%2C1672284084%2C%22%2Fdetails%2Fcourse%3Ftype%3DVIP%26courseId%3D04230a31e78b48c9b9ef14379ece0a47%26state%3DVOD%22%5D; Hm_lpvt_92a5d4e0ba2140a5aa6001c88a65ef97=1672284977; Hm_lpvt_5253ded03765ddd71ca75302ab1e548d=1672284977; _pk_id.3.fd4d=c2e75eb623f20279.1672220581.3.1672284977.1672280948.; looyu_20004236=v%3Aaf43aec9fd7f94262618c4f88deecd7b%2Cref%3A%2Cr%3A%2Cmon%3A//m6817.talk99.cn/monitor%2Cp0%3Ahttps%253A//www.dapengjiaoyu.cn/'}

    def start_requests(self):
        page = 1
        url = f'https://www.dapengjiaoyu.cn/api/old/courses/open?type=VIP&collegeId=j5m484sh&page={page}&size=10'
        while True:
        # if 1:
            res = requests.get(url, headers=self.headers)
            data_list = json.loads(res.text)
            if not data_list:
                break
            page += 1
            for data in data_list[7:]:
                course_id = data['id']
                course_name = data['title']
                self.parse_course(course_id, course_name)

    def parse_course(self, course_id, course_name):
        path = self.path + course_name
        if not os.path.exists(path):
            os.mkdir(path)
        url = f'https://www.dapengjiaoyu.cn/dp-course/api/courses/{course_id}/vods'
        res = requests.get(url, headers=self.headers)
        data_list = json.loads(res.text).get('courseVodContents')
        if data_list:
            for data in data_list[0]['lectures']:
                lesson_id = data['videoContent']['vid']
                lesson_name = data['title']
                self.parse_lesson(lesson_id, course_name, lesson_name)
        else:
            url = f'https://www.dapengjiaoyu.cn/api/old/courses/stages?courseId={course_id}'
            res = requests.get(url, headers=self.headers)
            id = json.loads(res.text)['playbackStage'][0]['id']
            page = 1
            while True:
                url = f'https://www.dapengjiaoyu.cn/api/old/courses/stages/{id}/chapters?courseId={course_id}&page={page}'
                res = requests.get(url, headers=self.headers)
                data_list = json.loads(res.text)
                if data_list:
                    for data in data_list:
                        lesson_id = data['videoContent']['vid']
                        lesson_name = data['title']
                        if '作业点评' in lesson_name:
                            continue
                        self.parse_lesson(lesson_id, course_name, lesson_name)
                    page += 1
                else:
                    break

    def parse_lesson(self, lesson_id, course_name, lesson_name):
        path = self.path + course_name + '/' + lesson_name
        if not os.path.exists(path):
            os.mkdir(path)
        url = f'https://hls.videocc.net/ef4825bc7e/f/{lesson_id.replace("_e", "_3.m3u8")}'
        m3u8_data = requests.get(url).text
        # 提取m3u8里边的ts文件的url
        ts_urls = re.findall('(https:.*?\.ts)', m3u8_data)
        if not ts_urls:
            url = f'https://hls.videocc.net/ef4825bc7e/f/{lesson_id.replace("_e", "_2.m3u8")}'
            m3u8_data = requests.get(url).text
            # 提取m3u8里边的ts文件的url
            ts_urls = re.findall('(https:.*?\.ts)', m3u8_data)
            if not ts_urls:
                url = f'https://hls.videocc.net/ef4825bc7e/f/{lesson_id.replace("_e", "_1.m3u8")}'
                m3u8_data = requests.get(url).text
                # 提取m3u8里边的ts文件的url
                ts_urls = re.findall('(https:.*?\.ts)', m3u8_data)
        for ts_url in ts_urls:
            print(ts_url)
            file_name = re.findall('_[1-3]_(\d+\.ts)', ts_url)[0]
            ts = requests.get(ts_url).content
            self.write(path, ts, file_name)
            m3u8_data = m3u8_data.replace(ts_url, file_name)
        if 'URI' in m3u8_data:  # 判断是否有密钥，有就提取url下载key
            key_url = re.findall('URI="(.*?key)', m3u8_data)[0]
            key = requests.get(url=key_url).content
            with open(path + '\\' + 'key.m3u8', 'wb') as f:
                f.write(key)
            # 将kye的url替换成本地的key密钥，方便合并
            m3u8_data = m3u8_data.replace(key_url, 'key.m3u8')
        self.write(path, m3u8_data)
        # 获取当前程序路径
        old_path = os.getcwd()
        # 将程序路径改为视频保存的路径
        os.chdir(path)
        # 利用ffmpeg合并视频的函数，下边有解释
        self.merge(lesson_name)
        # 沉睡10秒，让视频有足够的时间合并
        time.sleep(60)
        # 删除除mp4文件以为的文件的函数，下边解释
        self.remove()
        # 再将程序路径改回来
        os.chdir(old_path)
        print(f'合成完毕:{course_name}:{lesson_name}')

    # ffmpeg转换
    def merge(self, lesson_name):
        c = f'C:/ffmpeg/bin/ffmpeg.exe -allowed_extensions ALL -i index.m3u8 -c copy {lesson_name}.mp4'
        # 在终端中输入ffmpeg的命令，合并视频
        subprocess.Popen(c, shell=True)

    # 保存文件的函数
    def write(self, path, data, file_name=''):
        # 创建文件夹
        if not os.path.exists(path):
            os.mkdir(path)
        # 因为保存的格式不一样，需要判断一下
        if type(data) == str:
            # 这是m3u8文件
            with open(path + '\\' + 'index.m3u8', 'w') as f1:
                f1.write(data)
        else:
            # 这是ts文件，index是上边的序号
            with open(path + '\\' + file_name, 'wb') as f2:
                f2.write(data)

    def remove(self):
        is_loop = True
        while is_loop:
            files = os.listdir()
            for file in files:
                if 'mp4' in file:
                    for file_name in files:
                        if 'mp4' not in file_name:
                            os.remove(file_name)
                    is_loop = False
                    break

    def run(self):
        self.start_requests()


if __name__ == '__main__':
    dp = DaPengSpider()
    dp.run()
