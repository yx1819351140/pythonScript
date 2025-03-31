# -*- coding:UTF-8 -*-
# @Time    : 2025/3/31 10:24
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : scrape_dynamic_page.py
# @Project : pythonScript
# @Software: PyCharm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random


def create_stealth_driver():
    """创建一个隐藏WebDriver特征的浏览器实例"""
    options = webdriver.ChromeOptions()

    # 1. 基本反检测设置
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # 2. 禁用自动化控制标志
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")

    # 3. 随机化用户代理
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    # 4. 初始化浏览器
    service = Service(executable_path='/path/to/chromedriver')  # 替换为你的chromedriver路径
    driver = webdriver.Chrome(service=service, options=options)

    # 5. 执行JavaScript代码修改navigator属性
    with open('stealth.min.js', 'r') as f:  # 需要准备stealth.js文件
        js_code = f.read()
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": js_code
    })

    return driver


def scrape_dynamic_page(url, wait_for=None, timeout=10):
    """采集动态渲染的页面"""
    driver = None
    try:
        driver = create_stealth_driver()
        driver.get(url)

        # 等待页面加载完成
        if wait_for:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_for))
            )

        # 获取页面内容
        page_source = driver.page_source
        # 这里可以添加具体的解析逻辑

        # 模拟人类行为：随机滚动和暂停
        for _ in range(random.randint(2, 5)):
            driver.execute_script("window.scrollBy(0, window.innerHeight/2)")
            time.sleep(random.uniform(0.5, 2))

        return page_source
    except Exception as e:
        print(f"Error during scraping: {e}")
        return None
    finally:
        if driver:
            driver.quit()


# 测试代码
if __name__ == "__main__":
    TARGET_URL = "https://www.xiaohongshu.com/explore"  # 示例URL
    result = scrape_dynamic_page(TARGET_URL, wait_for=".feeds-container")
    if result:
        print("Page source length:", len(result))
        # 这里可以添加HTML解析代码