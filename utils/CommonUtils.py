# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> CommonUtils         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/3/24 11:26
# @Software : win10 python3.6
import hashlib
import io
import random
import time
import uuid
import requests
import docx2pdf
from PIL import Image, UnidentifiedImageError
from settings import PROXY, MINIO_BUCKET, MINIO_URL, USER_AGENT_LIST


def generate_uuid():
    return str(uuid.uuid1()).replace('-', '')


# word转pdf
def word_to_pdf(word_path, pdf_path):
    docx2pdf.convert(word_path, pdf_path)


# MD5加密
def md5_string(in_str):
    md5 = hashlib.md5()
    md5.update(in_str.encode("utf8"))
    result = md5.hexdigest()
    return result


# 当前系统时间
def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S')


# 图片存入minio
def save_to_minio(remote_img_url, minio, logger):
    if remote_img_url:
        try:
            # minio = Minio(endpoint=MINIO_URL, access_key=MINIO_USER, secret_key=MINIO_PWD, secure=False)
            proxies = {'https': f'http://{PROXY}', 'http': f'http://{PROXY}'}
            img_response = requests.get(remote_img_url, proxies=proxies, timeout=20, headers={'user-agent': random.choice(USER_AGENT_LIST)}, verify=False)
            if img_response.status_code == 200:
                # 将图片内容转换为 Bytes
                bytes_img_content = io.BytesIO(img_response.content)
                # 过滤小于1024 Bytes的图片
                if len(bytes_img_content.getvalue()) > 1024:
                    # 获取图片文件格式
                    try:
                        img_postfix = Image.open(bytes_img_content).format.lower()
                    except UnidentifiedImageError:
                        logger.warn('识别图片格式异常 url:' + remote_img_url)
                        return remote_img_url
                    # 图片文件名称
                    img_full_name = 'newsimg_' + time.strftime("%Y_%m_%d_") + \
                                    str(int(round(time.time() * 1000))) + '.' + img_postfix
                    minio.put_object(bucket_name=MINIO_BUCKET, object_name=img_full_name,
                                     data=io.BytesIO(img_response.content),
                                     length=-1, content_type='image/png', part_size=10 * 1024 * 1024)
                    local_img_url = f'http://{MINIO_URL}/{MINIO_BUCKET}/{img_full_name}'
                    return local_img_url
                else:
                    logger.warn('图片大小小于1024Bytes url:' + remote_img_url)
                    return remote_img_url
            else:
                logger.warn('获取图片数据失败 url:' + remote_img_url)
                return remote_img_url
        except Exception as e:
            logger.warn(f'获取图片数据失败 {e} url:' + remote_img_url)
            return remote_img_url
    else:
        logger.warn('图片链接为空')
        return remote_img_url

