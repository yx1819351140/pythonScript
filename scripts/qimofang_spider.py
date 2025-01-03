import requests
import openpyxl
import time


def get_data(headers):
    page = 0
    result = []
    while 1:
        params = {
            'size': '20',
            'page': str(page),
        }
        json_data = {
            'grabTimeType': 3,
            'grabType': None,
            'enterpriseName': '',
            'beginTime': '',
            'endTime': '',
            'contactTypes': [],
            'numberLabel': None,
            'telrobot': None,
        }
        response = requests.post('https://api.qike366.com/api/aicustomers/contacts/query', params=params, headers=headers, json=json_data)

        enterprise_group = response.json().get('enterpriseGroup', [])
        if not enterprise_group:
            break
        for enterprise in enterprise_group:
            enterprise_name = enterprise.get('enterpriseName', '')
            contact_list = enterprise.get('contactList', [])
            if contact_list:
                for contact in contact_list:
                    tags = ' '.join(contact.get('numberTags', []))
                    contact_no = contact.get('contactNo', '')
                    grab_time = contact.get('grabTime', '')
                    remark = contact.get('remark', '')
                    temp_result = [enterprise_name, tags, contact_no, '我领取的', grab_time, remark]
                    result.append(temp_result)
        page += 1
    if result:
        write_excel(result)


def write_excel(result):
    wb = openpyxl.Workbook()

    # 选择活动的工作表
    ws = wb.active
    ws.title = 'Contacts Data'

    # 设置表头
    ws_headers = ['企业名称', '标签', '联系方式', '获取方式', '获取时间', '跟进记录']
    ws.append(ws_headers)

    # 写入数据
    for row in result:
        ws.append(row)

    # 保存Excel文件
    file_name = f'contacts_data_{time.strftime("%Y%m%d")}.xlsx'
    wb.save(file_name)
    print(f"数据已成功写入文件 {file_name}")


if __name__ == '__main__':
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiIxMDY2NzUzOTk1NDM1NjE0MjA4IiwidXNlcjpqd3R2OjEwNjY3NTM5OTU0MzU2MTQyMDgiOiI5ZTFlNzA5Zi02ZWIxLTQ4OWMtOGVhOC01YjQwMWRkYTUzOGYiLCJzY29wZSI6WyJyZWFkIiwid3JpdGUiXSwiZXhwIjoxNzM1MjI3MzU3LCJhdXRob3JpdGllcyI6WyJ1c2VyIl0sImp0aSI6ImJYRmpPT19fSmN6UTBkOVpHNHJWTkUtaFdpdyIsImNsaWVudF9pZCI6Im1hcGxlY2xvdWR5In0.qrq_3VllSbku0rm03x6WSE9SAmc3x9PbNtxzViNKWdWGitmisWcjub3qULwyLDjmbm2AF5Judh2axBvxDBgFBjbSsDiKtgONJBWqiPyztpSFydqelCfWeZSr8PJihAP1yUvSbRj_-fM1voaD9ztp-cUiuS6kGKBKGk03yMqvb30mYm6Z6z3CUJX5m7zYDgw-KTT1kpybseZe6SarO6wD_8mdg8fXlq0IoZI8rWv98Y5dcxOYvvFYJeWWv01pUPJoQsDd2dirt1GrKWfnbu7JlHURPmsrtrElPN43YxrtqqzELFiVh84nAOr_g_sOY3sRtIue6hiRDQAKi8ni70_Oew',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://scrm.qike366.com',
        'Qksourcetype': 'PC',
        'Referer': 'https://scrm.qike366.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'uac': 'true',
        'uac_corp_id': 'cpId_1086988524058382336',
        'uac_tenant_id': 'QK9332364',
    }
    get_data(headers)
