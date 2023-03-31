# -*- coding: UTF-8 -*-
# @Project -> File     ：pythonScript -> flash_bilibili_video_playback         
# @IDE      ：PyCharm
# @Author   ：yangxin
# @Date     ：2023/2/3 10:01
# @Software : win10 python3.6
import time
import requests
from lxml import etree
from selenium import webdriver
"""
b站刷播放量
"""


def get_ip_list(url, headers):
    res = requests.get(url, headers=headers)
    html = etree.HTML(res.content.decode('gbk'))
    result_list = []
    ip_list = html.xpath('//table/tr/td[1]/text()')[3:]
    port_list = html.xpath('//table/tr/td[2]/text()')[1:]
    for i in range(len(ip_list)):
        ip = ip_list[i]
        port = port_list[i]
        result_list.append(f'http://{ip}:{port}')
    return result_list


def get_proxy_from_66():
    ip_list = []
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'www.66ip.cn',
        'If-None-Match': 'W/"b077743016dc54409ebe6b86ba7a869b"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36',
    }

    for i in range(1, 20):
        url = 'http://www.66ip.cn/' + str(i) + '.html'
        ip_list += (get_ip_list(url, headers))
    return ip_list


def get_proxy_from_89():
    result_list = []
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'www.66ip.cn',
        'If-None-Match': 'W/"b077743016dc54409ebe6b86ba7a869b"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36',
    }

    for i in range(1, 17):
        url = 'https://www.89ip.cn/index_' + str(i) + '.html'
        res = requests.get(url)
        html = etree.HTML(res.text)
        ip_list = html.xpath('//table/tbody/tr/td[1]/text()')
        port_list = html.xpath('//table/tbody/tr/td[2]/text()')
        for i in range(len(ip_list)):
            ip = ip_list[i]
            port = port_list[i]
            result_list.append(f'http://{ip.strip()}:{port.strip()}')
    return result_list


def Change_The_Time_Type(Video_Time):
    Digital_Video_Time = time.strptime(Video_Time, "%M:%S")
    Total_Second = Digital_Video_Time.tm_min * 60 + Digital_Video_Time.tm_sec
    return Total_Second


def Auto_Like_Your_Video(proxy):
    try:
        # 使用代理ip
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_argument(
            "--proxy-server=" + str(proxy))  # 一定要注意，=两边不能有空格，不能是这样--proxy-server = http://202.20.16.82:10152
        driver = webdriver.Chrome(options=chromeOptions)

        # 打开视频播放页
        driver.get("https://www.bilibili.com/video/BV11c411V7MX")
        # time.sleep(5)
        # driver.minimize_window()
        time.sleep(30)

        # # 获取视频时长
        # Video_Time = driver.find_element_by_xpath("//div[@class='bpx-player-ctrl-time-label']/span[3]").text
        # Total_Second = Change_The_Time_Type(Video_Time)
        #
        # # 两倍速
        # element = driver.find_element_by_xpath("//div[@class='bpx-player-ctrl-playbackrate-result']")
        # webdriver.ActionChains(driver).move_to_element(element).click(element).perform()
        # element = driver.find_element_by_xpath("//ul[@class='bpx-player-ctrl-playbackrate-menu']/li[1]")
        # webdriver.ActionChains(driver).move_to_element(element).click(element).perform()
        #
        # # # 点击播放
        # # element = driver.find_element_by_xpath(
        # #     "//button[@class='bilibili-player-iconfont bilibili-player-iconfont-start']")
        # # webdriver.ActionChains(driver).move_to_element(element).click(element).perform()
        #
        # 页面最小化
        # driver.minimize_window()
        #
        # # 看完视频
        # time.sleep(Total_Second / 2)

        # 关闭页面
        driver.close()
    except:
        pass


if __name__ == '__main__':
    # proxy_list = get_proxy_from_66()
    # proxy_list = get_proxy_from_89()
    # for proxy in proxy_list:


    for i in range(100):
        proxy = requests.get('http://proxy.siyetian.com/apis_get.html?token=gHbi1iT61UMOpXWx0EVrNTTB1STqFUeNpXQ51ERNFTTq1UNORUT49ERVNTT6NWe.wN3IjMwQTN3YTM&limit=1&type=0&time=10&split=1&split_text=&area=0&repeat=0&isp=0').text
        print(proxy)
        try:
            res = requests.get('https://www.bilibili.com/', proxies={'https': proxy, 'http': proxy}, timeout=3)
            print(res.status_code)
            if res.status_code == 200:
                Auto_Like_Your_Video(proxy)
        except:
            continue

