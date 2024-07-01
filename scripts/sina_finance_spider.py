# -*- coding:UTF-8 -*-
# @Time    : 24.7.1 09:49
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : sina_finance_spider.py
# @Project : pythonScript
# @Software: PyCharm
import traceback
import requests
import json
import redis
import re
from lxml import etree
import logging
import time


class SinaFinanceSpider(object):
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Referer': 'https://finance.sina.com.cn/realstock/company/sz002444/nc.shtml',
        'Sec-Fetch-Dest': 'script',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    headers1 = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    }
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.r = redis.Redis(connection_pool=redis.ConnectionPool(host='10.32.51.2', port=6379, db=7, decode_responses=True))

    @staticmethod
    def market_value_url(rn, code, b_code, h_code):
        if b_code and h_code:
            url = f'https://hq.sinajs.cn/rn={rn}&list={b_code},{h_code},{code},{code}_i,fx_shkdcny'
        elif b_code and not h_code:
            url = f'https://hq.sinajs.cn/rn={rn}&list={b_code},{code},{code}_i,fx_shkdcny'
        elif not b_code and h_code:
            url = f'https://hq.sinajs.cn/rn={rn}&list={h_code},{code},{code}_i,fx_shkdcny'
        else:
            url = f'https://hq.sinajs.cn/rn={rn}&list={code},{code}_i,fx_shkdcny'
            # url = f'https://hq.sinajs.cn/rn={int(time.time() * 1000)}&list={code}'
        return url

    @staticmethod
    def html_data(html):
        elem = etree.HTML(html)
        stock_code = ''.join(elem.xpath('//h1[@id="stockName"]/span/text()')).replace('(', '').replace(')', '').strip()
        re_str = r'var {} = (.*?);'
        regex_flag = re.compile(re_str.format("flag"))
        regex_cur = re.compile(re_str.format("currcapital"))
        regex_cua = re.compile(re_str.format("curracapital"))
        regex_cub = re.compile(re_str.format("currbcapital"))
        regex_mgz = re.compile(re_str.format("mgjzc"))
        regex_stt = re.compile(re_str.format("stockType"))
        regex_bcu = re.compile(re_str.format("b_currency"))
        regex_prf = re.compile(re_str.format("profit_four"))
        regex_exc = re.compile(re_str.format("exchangerate"))

        # var a_totalcapital A股总股本  b_totalcapital B股总股本   h_totalcapital H股总股本 用来计算总市值的和
        regex_atc = re.compile(re_str.format("a_totalcapital"))
        regex_btc = re.compile(re_str.format("b_totalcapital"))
        regex_ctc = re.compile(re_str.format("h_totalcapital"))

        # 获取b股代码和H股代码
        regex_bgc = re.compile(re_str.format("b_code"))
        regex_hgc = re.compile(re_str.format("corr_hkstock"))

        flag_list = regex_flag.findall(html)
        cur_list = regex_cur.findall(html)
        cua_list = regex_cua.findall(html)
        cub_list = regex_cub.findall(html)
        mgz_list = regex_mgz.findall(html)
        stt_list = regex_stt.findall(html)
        bcu_list = regex_bcu.findall(html)
        prf_list = regex_prf.findall(html)
        exc_list = regex_exc.findall(html)
        atc_list = regex_atc.findall(html)
        btc_list = regex_btc.findall(html)
        ctc_list = regex_ctc.findall(html)
        bc_list = regex_bgc.findall(html)
        hc_list = regex_hgc.findall(html)

        flag = flag_list[0] if len(flag_list) else ''
        curr_capital = cur_list[0] if len(cur_list) else ''
        curr_a_capital = cua_list[0] if len(cua_list) else ''
        curr_b_capital = cub_list[0] if len(cub_list) else ''
        mg_jzc = mgz_list[0] if len(mgz_list) else ''
        stock_type = stt_list[0].strip("'") if len(stt_list) else ''
        b_currency = bcu_list[0].strip("'") if len(bcu_list) else ''
        profit_four = prf_list[0] if len(prf_list) else ''
        exchange_rate = exc_list[0] if len(exc_list) else ''
        a_total_capital = atc_list[0] if len(atc_list) else ''
        b_total_capital = btc_list[0] if len(btc_list) else ''
        h_total_capital = ctc_list[0] if len(ctc_list) else ''
        b_code = bc_list[0].strip("'") if len(bc_list) else ''
        h_code = hc_list[0].strip("'") if len(hc_list) else ''

        return stock_code, flag, curr_capital, curr_a_capital, curr_b_capital, mg_jzc, stock_type, b_currency, profit_four, exchange_rate, a_total_capital, b_total_capital, h_total_capital, b_code, h_code

    def make_data(self, id_, code, content, args):
        """
        企业名称、股票代码、涨停价、跌停价、日期、当日开盘价、当日最高价、当日最低价、昨日收盘价、成交量、成交额、总市值、流通值、总股本、流通股、振幅、换手率、市净率、市盈率
        :return:
        """
        stock_code, flag, html_curr_capital, html_curr_a_capital, html_curr_b_capital, mg_jzc, stock_type, b_currency, profit_four, exchange_rate, a_total_capital, b_total_capital, h_total_capital, b_code, h_code = args
        if h_code:
            h_code = 'hk' + h_code
        regex = re.compile(fr'var hq_str_{code}="(.*)"')
        regex_i = re.compile(fr'var hq_str_{code}_i="(.*)"')
        regex_fx = re.compile(r'var hq_str_fx_shkdcny="(.*)"')

        try:
            hq_str = regex.search(content).group(1)
            hq_str_i = regex_i.search(content).group(1)
            hq_str_fx = regex_fx.search(content).group(1)

            hq_list = hq_str.split(',')
            hq_i_list = hq_str_i.split(',')
            hq_fx_list = hq_str_fx.split(',')
            item = dict()
            item["code"] = id_
            # 企业名称
            item["qymc"] = hq_list[0]
            if item["qymc"]:
                # 股票代码
                item["gpdm"] = stock_code
                # 涨停价
                item["ztj"] = hq_i_list[24].split('|')[0]
                # 跌停价
                item["dtj"] = hq_i_list[24].split('|')[1]
                # 日期
                item["date"] = hq_list[30] + ' ' + hq_list[31]
                # 昨日收盘价
                zrspj = hq_list[2]
                item["zrspj"] = zrspj
                # 当前价格
                dqjg = float(hq_list[3]) or zrspj
                item["dqjg"] = dqjg
                # 当日开盘价
                item["drkpj"] = hq_list[1]
                # 当日最高价
                drzgj = hq_list[4]
                item["drzgj"] = drzgj
                # 当日最低价
                drzdj = hq_list[5]
                item["drzdj"] = drzdj
                # 成交量
                cjl = round(float(hq_list[8]) / 100)
                item["cjl"] = cjl
                # 成交额
                item["cje"] = hq_list[9]

                # 两种情况
                # 1.总市值 _price *   * 10000  _price = ((_data.now * 1) || _data.preClose)  hq_list[3]
                # zsz = float(hq_list[3]) * float(hq_i_list[7]) * 10000
                # 2.各个板块的市值总和*人民币汇率(hq_fx_list[8])
                try:
                    zsz = 0
                    fl = float(hq_fx_list[8])
                    if a_total_capital and float(a_total_capital) > 0:
                        zsz += float(a_total_capital) * 10000 * float(dqjg)

                    if b_total_capital and float(b_total_capital) > 0:
                        regex_b = re.compile(fr'var hq_str_{b_code}="(.*)"')
                        hq_str_b = regex_b.search(content).group(1)
                        hq_list_b = hq_str_b.split(',')[3]
                        zsz += float(b_total_capital) * 10000 * float(hq_list_b) * fl

                    if h_code and h_total_capital and float(h_total_capital) > 0:
                        regex_h = re.compile(fr'var hq_str_{h_code}="(.*)"')
                        hq_str_h = regex_h.search(content).group(1)
                        hq_list_h = hq_str_h.split(',')[6]
                        zsz += float(h_total_capital) * 10000 * float(hq_list_h) * fl
                    item["zsz"] = zsz
                except Exception as e:
                    item["zsz"] = float(hq_list[3]) * float(hq_i_list[7]) * 10000

                # 总股本  _price > 0 ? _price * totalcapital * 10000
                try:
                    item["zgb"] = float(hq_i_list[7]) * 10000 or ''
                except ValueError:
                    item["zgb"] = hq_i_list[7]
                # 振幅
                try:
                    item["zf"] = 100 * ((float(drzgj) - float(drzdj)) / float(zrspj)) / 100
                except ZeroDivisionError:
                    item["zf"] = ''
                except ValueError:
                    item["zf"] = ''
                    # 换手率 turnover = cjl / curr_capital / 10000 * 100;
                # currcapital 根据flag(判断标志)的值: flag == 1:currcapital = _data_i ? _data_i[8] : window.currcapital; flag等2或者3 用html的currcapital

                curr_capital = hq_i_list[
                    8] if flag == "1" else html_curr_a_capital if flag == "2" else html_curr_b_capital
                try:
                    item["hsl"] = float(cjl) / float(curr_capital) / 10000 * 100
                except ZeroDivisionError:
                    item["hsl"] = ''
                except ValueError:
                    item["hsl"] = ''

                # 流通股 1 * curr_capital * 10000
                try:
                    item["ltg"] = float(curr_capital) * 10000
                except ValueError:
                    item["ltg"] = ''
                # 流通值 _price * curr_capital * 10000
                try:
                    item["ltz"] = float(dqjg) * float(curr_capital) * 10000
                except ValueError:
                    item["ltz"] = ''

                    # 市净率  价格/(每股净资产*转换比例)  price / (mgjzc*zhbl);
                zhbl = eval(hq_i_list[29])
                try:
                    item["sjl"] = float(dqjg) / (float(mg_jzc) * zhbl)
                except ZeroDivisionError:
                    item["sjl"] = ''
                except ValueError:
                    item["sjl"] = ''
                try:
                    syl_zsz = float(dqjg) * float(hq_i_list[7]) * 10000
                    # 市盈率 (stockType == 'B' && b_currency != 'CNY') ? _data.totalShare*exchangerate / profit_four / 100000000 : _data.totalShare / profit_four / 100000000;
                    # 如果syl<0 为 sz002565
                    syl = syl_zsz * float(exchange_rate) / float(
                        profit_four) / 100000000 if stock_type == 'B' and b_currency != 'CNY' and float(
                        profit_four) else syl_zsz / float(profit_four) / 100000000 if float(profit_four) else 0
                    item["syl"] = syl if syl > 0 else '--'
                except ValueError:
                    item["syl"] = ''
                self.logger.info(f"task over: {str(item)[:50]}")
                return item
            else:
                self.logger.info(f"no get data task over : {str(item)[:50]}")
                return ''
        except Exception as e:
            self.logger.info(traceback.print_exc())
            return ''

    def start_requests(self, dict_data):
        id_ = dict_data['code']
        code = dict_data['symbol']
        company_url = f'http://finance.sina.com.cn/realstock/company/{code}/nc.shtml'
        html = requests.get(company_url).content.decode('gbk')
        data_tuple = self.html_data(html)
        b_code = data_tuple[-2]
        h_code = 'hk' + data_tuple[-1] if data_tuple[-1] else ''
        rn = '1719287000000'
        market_value_url = self.market_value_url(rn, code, b_code, h_code)
        print(market_value_url)
        html = requests.get(market_value_url, headers=self.headers).content.decode('gbk')
        print(html)
        result_data = self.make_data(id_, code, html, data_tuple)
        print(result_data)

    def run(self):
        data_list = self.r.smembers('sina_a_copy')
        for data in list(data_list)[:1]:
            self.start_requests(json.loads(data))


if __name__ == '__main__':
    sina = SinaFinanceSpider()
    sina.run()
