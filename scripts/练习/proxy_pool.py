# -*- coding:UTF-8 -*-
# @Time    : 2025/3/28 13:49
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : proxy_pool.py
# @Project : pythonScript
# @Software: PyCharm
import requests
import random
import time
from threading import Thread, Lock


class ProxyPool:
    def __init__(self, init_proxies=None, check_url='http://httpbin.org/get'):
        self.good_proxies = set()
        self.bad_proxies = set()
        self.check_url = check_url
        self.lock = Lock()
        if init_proxies:
            self.add_proxies(init_proxies)

        self._running = True
        self.thread = Thread(target=self._check_proxies_loop, daemon=True)
        self.thread.start()

    def add_proxies(self, proxies):
        with self.lock:
            for proxy in proxies:
                if proxy not in self.bad_proxies:
                    self.good_proxies.add(proxy)

    def get_random_proxy(self):
        with self.lock:
            if not self.good_proxies:
                return None
            return random.choice(list(self.good_proxies))

    def mark_bad(self, proxy):
        with self.lock:
            if proxy in self.good_proxies:
                self.good_proxies.remove(proxy)
            self.bad_proxies.add(proxy)

    def _check_proxy(self, proxy):
        proxies = {'http': f'http://{proxy}', 'https': f'http://{proxy}'}
        try:
            response = requests.get(self.check_url, proxies=proxies, timeout=10)
            if response.status_code == 200:
                return True
        except:
            return False
        return False

    def _refill_proxies(self):
        new_proxies = requests.get('').json()
        print(f'补充新代理: {new_proxies}')
        self.add_proxies(new_proxies)

    def _check_proxies_loop(self):
        while self._running:
            time.sleep(60)

            with self.lock:
                to_check = list(self.good_proxies)

            for proxy in to_check:
                if not self._check_proxy(proxy):
                    self.mark_bad(proxy)

            with self.lock:
                if len(self.good_proxies) < 5:
                    self._refill_proxies()

    def stop(self):
        self._running = False
        self.thread.join()


if __name__ == '__main__':
    init_proxies = requests.get('').json()
    proxy_pool = ProxyPool(init_proxies)

    for i in range(10):
        proxy = proxy_pool.get_random_proxy()
        if not proxy:
            print('没有可用的代理')
            time.sleep(5)
            continue

        print(f'使用代理: {proxy}')
        try:
            proxies = {'http': f'http://{proxy}', 'https': f'http://{proxy}'}
            res = requests.get('http://httpbin.org/get', proxies=proxies, timeout=5)
            print('请求成功', res.json())
        except Exception as e:
            print(f'代理 {proxy} 请求失败', str(e))
            proxy_pool.mark_bad(proxy)

        time.sleep(1)

    proxy_pool.stop()