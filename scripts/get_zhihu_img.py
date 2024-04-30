"""
下载知乎图片
"""
import requests
from lxml import etree


class Zhihu:

    def parse_img_urls(self, url):
        res = requests.get(url)
        html = etree.HTML(res.text)
        url_list = html.xpath('//figure[@data-size="normal"]//img/@src')
        for url in url_list:
            self.save_img(url)

    def save_img(self, url):
        if 'picx' in url:
            file_name = url.split('/')[-1].split('?')[0].replace('v2-', '').replace('_720w', '')
            with open(f'C:/Users/admin/Pictures/表情包/{file_name}', 'wb') as f:
                f.write(requests.get(url).content)

    def run(self):
        url = 'https://www.zhihu.com/question/564970989/answer/2765106417'
        self.parse_img_urls(url)


if __name__ == '__main__':
    zhihu = Zhihu()
    zhihu.run()
