# -*- coding:UTF-8 -*-
# @Time    : 2024/5/14 09:51
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : get_tyc_cookie.py
# @Project : pythonScript
# @Software: PyCharm
'''
用于天眼查自动登录，解决滑块验证问题
'''
from selenium import webdriver
import time
from PIL import Image, ImageGrab
from io import BytesIO
from selenium.webdriver.common.action_chains import ActionChains
import os
import sys
import re
import xlwt
import urllib
import datetime
import json


def get_track(distance):
    """
    根据偏移量获取移动轨迹
    :param distance: 偏移量
    :return: 移动轨迹
    """
    # 移动轨迹
    track = []
    # 当前位移
    current = 0
    # 减速阈值
    mid = distance * 2 / 5
    # 计算间隔
    t = 0.2
    # 初速度
    v = 1

    while current < distance:
        if current < mid:
            # 加速度为正2
            a = 5
        else:
            # 加速度为负3
            a = -2
        # 初速度v0
        v0 = v
        # 当前速度v = v0 + at
        v = v0 + a * t
        # 移动距离x = v0t + 1/2 * a * t^2
        move = v0 * t + 1 / 2 * a * t * t
        # 当前位移
        current += move
        # 加入轨迹
        track.append(round(move))
    return track


def autologin(account, password):
    driver = webdriver.Chrome(executable_path='D:/python/chromedriver.exe')

    # driver.get('https://www.tianyancha.com/?jsid=SEM-BAIDU-PP-SY-000873&bd_vid=7864822754227867779')
    # # time.sleep(1)
    # try:
    #     driver.find_element_by_xpath('//div[@class="close"]').click()
    # except:
    #     pass
    # driver.find_element_by_xpath('//span[@class="tyc-nav-user-btn"]').click()
    # # time.sleep(1)
    # # 这里点击密码登录时用id去xpath定位是不行的，因为这里的id是动态变化的，所以这里换成了class定位
    # driver.find_element_by_xpath('//div[@class="tyc-modal-body"]/div/div[2]').click()
    # driver.find_element_by_xpath('//div[@class="sign-in sign-in-mobile"]/div/div[2]').click()
    # time.sleep(1)
    # accxp = '//div[@class="sign-password"]/div/input[@type="text"]'
    # pasxp = '//div[@class="sign-password"]/div/input[@type="password"]'
    # driver.find_element_by_xpath(accxp).send_keys(account)
    # driver.find_element_by_xpath(pasxp).send_keys(password)
    # driver.find_element_by_xpath('//div[@class="login-bottom"]/input[@type="checkbox"]').click()
    # driver.find_element_by_xpath('//button/span[contains(text(), "登录")]').click()
    # time.sleep(10)
    # cookies = driver.get_cookies()
    # # 将cookies保存在本地
    # with open('data/cookies.txt', 'w') as f:
    #     f.write(json.dumps(cookies))
    # # 关闭浏览器
    # driver.close()

    # cookies = json.load(open('data/cookies.txt', 'r'))
    driver.get(
        'https://www.tianyancha.com/search?key=%E5%95%86%E5%8A%A1%E6%9C%8D%E5%8A%A1&sessionNo=1715589535.73024627')
    cookies = [{'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'HWWAFSESID', 'value': 'dc0b72024addd26e68d'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'HWWAFSESTIME', 'value': '1715587925562'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'csrfToken', 'value': 'UgxbwS8pa9hGdRrh7el-MCn-'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'TYCID', 'value': '7ef9b160110011efbc16018473108730'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'CUID', 'value': 'de5ef619830f282a0e9e4c0334ba2596'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'Hm_lvt_e92c8d65d92d534b0fc290df538b4758', 'value': '1715587932'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'bannerFlag', 'value': 'true'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'ssuid', 'value': '710140368'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': '_ga', 'value': 'GA1.2.2074213740.1715587935'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': '_gid', 'value': 'GA1.2.1205315446.1715587935'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'sensorsdata2015jssdkcross', 'value': '%7B%22distinct_id%22%3A%22322546017%22%2C%22first_id%22%3A%2218f71025fb3fd-04aacea045abdc4-26001a51-921600-18f71025fb4991%22%2C%22props%22%3A%7B%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMThmNzEwMjVmYjNmZC0wNGFhY2VhMDQ1YWJkYzQtMjYwMDFhNTEtOTIxNjAwLTE4ZjcxMDI1ZmI0OTkxIiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiMzIyNTQ2MDE3In0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22322546017%22%7D%2C%22%24device_id%22%3A%2218f71025fb3fd-04aacea045abdc4-26001a51-921600-18f71025fb4991%22%7D'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'tyc-user-phone', 'value': '%255B%252217600372502%2522%255D'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'searchSessionId', 'value': '1715589535.73024627'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'Hm_lpvt_e92c8d65d92d534b0fc290df538b4758', 'value': '1715656291'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': '_gat_gtag_UA_123487620_1', 'value': '1'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'tyc-user-info', 'value': '{%22state%22:%220%22%2C%22vipManager%22:%220%22%2C%22mobile%22:%2217600372502%22%2C%22userId%22:%22322546017%22}'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'tyc-user-info-save-time', 'value': '1715656305071'}, {'secure': False, 'httpOnly': False, 'domain': 'tianyancha.com', 'path': '/', 'name': 'auth_token', 'value': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNzYwMDM3MjUwMiIsImlhdCI6MTcxNTY1NjMwNSwiZXhwIjoxNzE4MjQ4MzA1fQ.a_vmG8KVWj_r2cVPaLmYNPjOQnZjB7IuFSEi68IBANa01ahg6KzrA79viHcPzzohHxXlRiMbpoOIM9CmEOZQaQ'}]
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.get('https://www.tianyancha.com/search?key=%E5%95%86%E5%8A%A1%E6%9C%8D%E5%8A%A1&sessionNo=1715589535.73024627')
    time.sleep(10)
    # # 点击登录之后开始截取验证码图片
    # time.sleep(2)
    # img = driver.find_element_by_xpath('/html/body/div[10]/div[2]/div[2]/div[1]/div[2]/div[1]')
    # time.sleep(0.5)
    # # 获取图片位子和宽高
    # location = img.location
    # size = img.size
    # # 返回左上角和右下角的坐标来截取图片
    # top, bottom, left, right = location['y'], location['y'] + size['height'], location['x'], location['x'] + size[
    #     'width']
    # # 截取第一张图片(无缺口的)
    # screenshot = driver.get_screenshot_as_png()
    # screenshot = Image.open(BytesIO(screenshot))
    # captcha1 = screenshot.crop((left, top, right, bottom))
    # print('--->', captcha1.size)
    # captcha1.save('captcha1.png')
    # # 截取第二张图片(有缺口的)
    # driver.find_element_by_xpath('/html/body/div[10]/div[2]/div[2]/div[2]/div[2]').click()
    # time.sleep(4)
    # img1 = driver.find_element_by_xpath('/html/body/div[10]/div[2]/div[2]/div[1]/div[2]/div[1]')
    # time.sleep(0.5)
    # location1 = img1.location
    # size1 = img1.size
    # top1, bottom1, left1, right1 = location1['y'], location1['y'] + size1['height'], location1['x'], location1['x'] + \
    #                                size1['width']
    # screenshot = driver.get_screenshot_as_png()
    # screenshot = Image.open(BytesIO(screenshot))
    # captcha2 = screenshot.crop((left1, top1, right1, bottom1))
    # captcha2.save('captcha2.png')
    # # 获取偏移量
    # left = 55  # 这个是去掉开始的一部分
    # for i in range(left, captcha1.size[0]):
    #     for j in range(captcha1.size[1]):
    #         # 判断两个像素点是否相同
    #         pixel1 = captcha1.load()[i, j]
    #         pixel2 = captcha2.load()[i, j]
    #         threshold = 60
    #         if abs(pixel1[0] - pixel2[0]) < threshold and abs(pixel1[1] - pixel2[1]) < threshold and abs(
    #                 pixel1[2] - pixel2[2]) < threshold:
    #             pass
    #         else:
    #             left = i
    # print('缺口位置', left)
    # # 减去缺口位移
    # left -= 52
    # # 开始移动
    # track = get_track(left)
    # print('滑动轨迹', track)
    # # track += [5,4,5,-6, -3,5,-2,-3, 3,6,-5, -2,-2,-4]  # 滑过去再滑过来，不然有可能被吃
    # # 拖动滑块
    # slider = driver.find_element_by_xpath('/html/body/div[10]/div[2]/div[2]/div[2]/div[2]')
    # ActionChains(driver).click_and_hold(slider).perform()
    # for x in track:
    #     ActionChains(driver).move_by_offset(xoffset=x, yoffset=0).perform()
    # time.sleep(0.2)
    # ActionChains(driver).release().perform()
    # time.sleep(1)
    # try:
    #     if driver.find_element_by_xpath('/html/body/div[10]/div[2]/div[2]/div[2]/div[2]'):
    #         print('能找到滑块，重新试')
    #         # driver.delete_all_cookies()
    #         # driver.refresh()
    #         # autologin(driver, account, password)
    #     else:
    #         print('validate success')
    # except:
    #     print('validate success')
    #
    # time.sleep(0.2)


if __name__ == '__main__':
    autologin('17600372502', '161611090518yx')