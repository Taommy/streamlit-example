import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from eastmoneyapi import EastmoneyApi
from extractor import *
from processor import *
api = EastmoneyApi()
session = requests.Session()
code = '007119'
quarter = '2023Q2'
sl = [code]
fields = ['基金名称', '季度', '股票代码', '股票名称','占净值比例', '持仓市值(亿元)','最新价','持股数（万股）','股息率', "市盈(动)","所属行业"]


def get_fund_basic_info(code = '007119') -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
    url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"

    res = requests.get(url, headers=headers)
    text_data = res.text

    # 提取所有的变量赋值语句
    pattern = r'var (\w+)\s*=\s*(.*?);(?=\s*var|\s*/\*|$)'
    matches = re.findall(pattern, text_data, re.DOTALL)

    data_dict = {}
    for match in matches:
        key, value = match
        # 去掉多余的空白和注释
        clean_value = re.sub(r'/\*.*?\*/', '', value).strip()

        # 尝试将值解析为JSON，如果失败，则直接使用字符串值
        try:
            if clean_value.startswith('"') and clean_value.endswith('"'):
                clean_value = clean_value[1:-1]
            json_value = json.loads(clean_value)
            data_dict[key] = json_value
        except json.JSONDecodeError:
            data_dict[key] = clean_value

    return data_dict

def fund_company_by_manager(input = '傅鹏博') -> pd.DataFrame:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36"
    }
    url = "https://fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx"
    params = {
        "dt": "14",
        "mc": "returnjson",
        "ft": "hh",
        "pn": "30",
        "pi": "1",
        'sc':'netnav',
        'st':'desc',
        'input':input
    }
    res = requests.get(url, headers=headers, params=params)
    text_data = res.text.replace("var returnjson= ", "")
    json_str = re.sub(r'(\b\w+\b):', r'"\1":', text_data).replace("'", '"')
    data_dict = json.loads(json_str)
    data_list = data_dict["data"]
    if data_list:
        return {"公司名称": data_list[0][3],'公司id':data_list[0][2], "在管基金": data_list[0][5]}
    return {}

    # 删除不需要的列
#    columns_to_drop = ["现任基金最佳回报1", "现任基金最佳回报2"]  # 可以按需增加或删除列名称
#    temp_df = temp_df.drop(columns=columns_to_drop)
    return temp_df[["公司名称",'公司id',"基金名称列表"]]

def extract_manager_info(manager):
    manager_data = {
         'id': manager['id'],
        '图片': manager['pic'],
        '基金经理': manager['name'],
        '工作时间': manager['workTime'],
        '基金规模': manager['fundSize']
    }
    company_info = fund_company_by_manager(manager['id'])
    manager_data.update(company_info)
    return manager_data

import requests
import json
import pandas as pd
def fetch_fund_data(fund_code):
    url = f"https://fund.10jqka.com.cn/interface/fund/managerInfo?code={fund_code}"

    # Making a GET request
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises a HTTPError if the response status is 4xx, 5xx
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

    # Proceed if the response status was 200 (OK)
    if response.status_code == 200:
        return response.json()
    

def get_servey_data(receive_start_date = "2023-01-01",RECEIVE_OBJECT = "睿远基金",columns = "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_END_DATE,RECEIVE_OBJECT,RECEIVE_PLACE,RECEIVE_WAY_EXPLAIN,INVESTIGATORS,RECEPTIONIST,NUMBERNEW,CONTENT,ORG_TYPE",pagesize = 20):
    base_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    # 查询参数
    params = {
        "reportName": "RPT_ORG_SURVEY",
        "columns": columns,
#        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,RECEIVE_START_DATE,RECEIVE_OBJECT,NUM,CONTENT",
        "quoteColumns": "f2~01~SECURITY_CODE~CLOSE_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE",
        "sortColumns": "RECEIVE_START_DATE",
        "sortTypes": "-1",
        "pageSize": f"{pagesize}",
        "filter": f'(IS_SOURCE="1")(RECEIVE_OBJECT="{RECEIVE_OBJECT}")(RECEIVE_START_DATE>= \'{receive_start_date}\')',
#        "filter": f'(IS_SOURCE="1")(RECEIVE_OBJECT="{RECEIVE_OBJECT}")',
    }

    # 发送请求
    response = session.get(base_url, params=params)
#    print(response.url)
    # 解析返回的数据
    try:
        df = json.loads(response.text[response.text.index("{"):response.text.rindex("}")+1])
    except json.JSONDecodeError as e:
        print(f"解析JSON时出错: {e}")

#    return data['result']['data']

    return pd.DataFrame(df['result']['data'])

import requests
import pandas as pd

def get_gscc_data(gs_id="80672691", year="2023", quarter="2", ftype="0"):
    # API的基本URL
    base_url = "https://fund.eastmoney.com/Company/tzzh/GsccQuarter"

    # 查询参数
    params = {
        "gsId": gs_id,      # 公司ID
        "year": year,      # 年份
        "quarter": quarter,# 季度
        "ftype": ftype     # 文件类型
    }

    # 发送请求
    response = requests.get(base_url, params=params)

    # 使用pandas直接从HTML解析表格
    tables = pd.read_html(response.text)
    if not tables:
        print("No table found in the provided URL.")
        return pd.DataFrame()

    df = tables[0]
    df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
    df['持仓市值(万元)'] = round(df['持仓市值(万元)']/10000,2)
    df.rename(columns = {'持仓市值(万元)':'持仓市值(亿元)'}, inplace=True)
    return df[['股票代码', '股票名称', '本公司持有基金数', '占总净值比例', '持股数(万股)', '持仓市值(亿元)']]


#放入函数，用于查看基金持仓以及持仓个股的实时行情数据、以及行业数据
import operator
import requests
import json
import pandas as pd
import yaml
from bs4 import BeautifulSoup as bs
from multiprocessing.dummy import Pool
import pandas as pd
import re
import time
import requests
import operator
import yaml
import json
import csv
import time
import pandas as pd
import os
import json
import concurrent.futures
import pandas as pd
url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc'
def get_api_data(r):
    '''
    Get formatted js dict from raw text.
    '''
    apidata = r[:-1]
    apidata = apidata.replace('var apidata=','')
    for s in ['content','arryear','curyear']:
        apidata = apidata.replace(s+':',s+': ')
    y = yaml.safe_load(apidata)
    #dict_keys(['content', 'arryear', 'curyear'])
    return y

columns=[['基金名称','季度','股票代码','股票名称','占净值比例','持股数','持仓市值']]
def get_fund_holdings(code=None,year=2023,top=20,max_year=1):
    '''
    Get fund holding data.

    Parameters:
    - code: fund code. e.g.007119
    - year: certain year. Get all annual data by default.
    - top: top n stocks. 20 by default.

    Return:
    A list in [['基金名称','季度','股票代码','股票名称','占净值比例','持股数','持仓市值']].
    e.g. ['睿远成长价值混合A', '2020年1季度股票投资明细', '688002', '睿创微纳', '0.002', '62.02', '2378.98']
    '''
    #Get available years
    r = requests.get(url+f'&code={code}')
    y=get_api_data(r.text)
    ret = []
    years = y['arryear'][:max_year] #assuming arryear is sorted descendently
    for ann in years:
        ret.extend(get_annual_data(code=code,year=ann,top=top))
#    data.extend(ret)
#    time.sleep(0.2)
    return ret
def get_annual_data(code,year,top):
    '''
    Parameters:
    - y: dict

    Returns:
    list
    '''
    r = requests.get(url+f'&code={code}&topline={top}&year={year}')
    assert r.status_code == 200, "Invalid webpage return result; try again later."
    y=get_api_data(r.text)
    soup=bs(y['content'], features="lxml")
    quarters = soup.findAll('div',{"class":"box"})
    ret = []
    for quarter in quarters:
        name = quarter.a.text #睿远成长价值混合A
        quarter_name = quarter.label.text[-len('2020年4季度股票投资明细'):] #2020年4季度股票投资明细
        for stock in quarter.table.tbody.children:
            l = list(map(lambda x:x.text,stock('td')))
            if l[3] == '':
                #最新持仓预留当前股价占位.
                #[序号,股票代码,股票名称,最新价,涨跌幅,相关资讯,占净值比例,持股数（万股）,持仓市值（万元）] quarter.table.thead
                #['11', '03606', '福耀玻璃', '', '', '变动详情股吧行情档案', '0.58%', '495.88', '17,777.30']
                l = list(operator.itemgetter(1,2,6,7,8)(l))
            else:
                #[序号,股票代码,股票名称,相关资讯,占净值比例,持股数（万股）,持仓市值（万元）]
                #['1', '601012', '隆基股份', '股吧行情档案', '5.79%', '1,669.92', '125,260.70']
                l = list(operator.itemgetter(1,2,4,5,6)(l))
            #['03606', '福耀玻璃', '0.58%', '495.88', '17,777.30']

            #handle with %
            #Careful of unavailable data!
            #['003001', '中岩大地', '---', '0.31', '---']
            #['000568', '泸州老窖', '--', '股吧行情', '9.90%']
            #['000921', '海信家电', '---', '484.00', '5,251.36']
            if not '-' in l[2]:
                l[2]=float(l[2][:-1])/100 #0.0057999
                l[2]=f'{l[2]:.4}'#'0.0058'
            else:
                print(l)
            l[3]=l[3].replace(',','') # stripping thousands separator
            l[4]=l[4].replace(',','')
            #['03606', '福耀玻璃', '0.0058', '495.88', '17777.30']
            ret.append([name,quarter_name]+l)
            #['睿远成长价值混合A', '2020年1季度股票投资明细', '688002', '睿创微纳', '0.002', '62.02', '2378.98']
    return ret
def process_stock_code(stock_code):
    if len(stock_code) == 5:
        return f'116.{stock_code}'
    elif len(stock_code) == 6:
        if stock_code.startswith('6'):
            return f'1.{stock_code}'
        elif stock_code.startswith(('0', '3')):
            return f'0.{stock_code}'

    return stock_code
import requests
import pandas as pd
import json

def get_industry_data(page_number=1, page_size=200, fields="f12,f14,f2,f3,f5,f14,f62,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205"):
    """
    获取东方财富网的行业数据

    参数:
    - page_number: 页码，默认为1
    - page_size: 每页的数量，默认为200
    - fields: 查询的字段，默认为"f12,f14,f2,f3,f5,f14,f62,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205"

    返回:
    - pandas.DataFrame: 包含行业数据的数据帧
    """
    # API的基本URL
    base_url = "https://push2.eastmoney.com/api/qt/clist/get"

    # 查询参数
    params = {
        "fid": "f62",
        "po": "1",
        "pz": page_size,
        "pn": page_number,
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fs": "m:90+t:2",
        "fields": fields
    }

    # 发送请求
    response = requests.get(base_url, params=params)

    # 解析返回的数据
    data = json.loads(response.text[response.text.index("{"):response.text.rindex("}")+1])

    # 将数据转换为数据帧
    df = pd.DataFrame(data['data']['diff'])
    # 重命名列名
    df = df.rename(columns={
        'f2': '最新价',
        'f3': '行业涨跌幅',
        'f4': '涨跌额',
        'f5': '成交量(万股)',
        'f6': '成交额(亿元)',
        'f7': '振幅',
        'f8': '换手率',
        'f9': '市盈(动)',
        'f10': '量比',
        'f11': '委比',
        'f12': '行业代码',
        'f14': '行业名称',
        "f204":"主力净流入最大股",
        "f205":"股票代码",
        "f62":"主力净流入额",
        "f66":"超大单净流入额",
        "f69":"超大单净流入占比",
        "f72":"大单净流入额",
        "f75":"大单净流入占比",
        "f78":"中单净流入额",
        "f81":"中单净流入占比",
        "f84":"小单净流入额",
        "f87":"小单净流入占比",


    })
#    df['主力净流入额'] = df['主力净流入额'].apply(lambda x: f'{float(x) / 100000000:.2f}')
    return df


#FIELDS_LIST = ['f{}'.format(i) for i in range(1, 301)]  # 全局常量，用于存储字段列表
FIELDS_LIST = ['f2,f9,f12,f100,f133']
def get_realtime_data(secids, fields_list=FIELDS_LIST):
    """
    获取东方财富网的实时行情数据

    参数:
    - secids: 股票代码列表
    - fields_list: 查询的字段列表，默认为全局常量 FIELDS_LIST

    返回:
    - pandas.DataFrame: 包含实时行情数据的数据帧
    """
    # API的基本URL
    base_url = "https://push2.eastmoney.com/api/qt/ulist.np/get"

    # 查询参数
    params = {
        "fltt": "2",
        "invt": "2",
        "fields": ','.join(fields_list),
        "secids": secids,
    }

    # 发送请求
    response = requests.get(base_url, params=params)

    # 解析返回的数据
    try:
        # 从响应文本中去除回调函数名称和括号
        json_text = response.text[response.text.index("{"):response.text.rindex("}")+1]
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

    # 将数据转换为数据帧
    df = pd.DataFrame(data['data']['diff'])
#    df['f5'] = df['f5'].apply(lambda x: round(x / 10000, 2))
#    df['f6'] = df['f6'].apply(lambda x: round(x / 100000000, 2))
    # 重命名列名
    df = df.rename(columns={
        'f2': '最新价',
        'f3': '涨跌幅',
        'f4': '涨跌额',
        'f5': '成交量(万股)',
        'f6': '成交额(亿元)',
        'f7': '振幅',
        'f8': '换手率',
        'f9': '市盈(动)',
        'f10': '量比',
        'f11': '委比',
        'f12': '股票代码',
        'f13': 'secids',
        'f14': '股票名称',
        'f15': '最高价',
        'f16': '最低价',
        'f17': '开盘价',
        'f18': '昨日收盘价',
        'f100': '所属行业',
        'f62': '主力净额',
        "f184": "主力净占比",
        "f66": "超大单净额",
        "f69": "超大单净占比",
        "f72": "大单净额",
        "f75": "大单净占比",
        "f78": "中单净额",
        "f81": "中单净占比",
        "f84": "小单净额",
        "f87": "小单净占比",
        "f38": "总股本",
        "f39": "流动股本",
        "f40": "最新一期营业收入",
        "f41": "最新一期营业收入yoy",
        "f42": "最新一期营业利润",
        "f44": "最新一期利润总额",
        "f45": "最新一期归母净利润",
        "f133":"股息率",

    })
    return df



def get_fund_data(sl, fields):
    """
    获取基金持仓数据，并与实时行情数据和行业数据进行合并

    参数:
    - sl: 基金代码列表
    - fields: 需要返回的字段列表

    返回:
    - pandas.DataFrame: 合并后的数据帧
    """
    # 基金持仓爬虫
    data = [['基金名称','季度','股票代码','股票名称','占净值比例','持股数（万股）','持仓市值（万元）']]
    pool = Pool(5)  # 限制线程数量
    results = pool.map(get_fund_holdings, sl)
    for result in results:
        data.extend(result)
    df = pd.DataFrame(data[1:], columns=data[0])
    df['季度'] = df['季度'].apply(lambda x: re.sub(r'(\d{2})年(\d)季度股票投资明细', r'\1Q\2', x))
    df['占净值比例'] = df['占净值比例'].apply(lambda x: (f'{float(x) * 100:.2f}%' if '%' not in x else '0.00%') if x not in ['-','--', '---'] else '0.00%')
    df['持仓市值(亿元)'] = df['持仓市值（万元）'].apply(lambda x: (f'{float(x) / 10000:.2f}' if '%' not in x else '0.00') if x not in ['-','--', '---'] else '0.00')
    df['secids'] = df['股票代码'].apply(process_stock_code)  # 创造secids前缀，沪1，深0，港116
    secids = df.secids.unique()
    # 通过secids前缀找到实时数据
    realtime_data = get_realtime_data(','.join(secids))  # realtime_data还有很多数据待发掘
    # 拼接data
    merged_data = pd.merge(df, realtime_data[['最新价','股票代码','股息率', "市盈(动)", "所属行业"]], on='股票代码', how='left')
    # 找到行业涨跌幅数据并进行拼接
#    industry_data = get_industry_data(fields='f3,f14')
#    merged_data = pd.merge(merged_data, industry_data, left_on='所属行业', right_on='行业名称', how='left')
    return merged_data[fields]


def get_fund_report_list(code:str = "377240", page_index:int = 1) -> dict:
  """ Get part of fund report list.

  An example response:

  {
    "Data": [{
        "FUNDCODE": "000001",
        "TITLE": "华夏成长2005年第四季度报告",
        "ShortTitle": "华夏成长混合",
        "NEWCATEGORY": "3,6",
        "PUBLISHDATE": "2006-01-23T00:00:00",
        "PUBLISHDATEDesc": "2006-01-23",
        "ATTACHTYPE": "5",
        "ID": "AN201202270004036072"
    }, {
        "FUNDCODE": "000001",
        "TITLE": "华夏成长2003年第二季度投资组合",
        "ShortTitle": "华夏成长混合",
        "NEWCATEGORY": "3,6",
        "PUBLISHDATE": "2003-07-18T00:00:00",
        "PUBLISHDATEDesc": "2003-07-18",
        "ATTACHTYPE": "5",
        "ID": "AN201202250003646520"
    }, {
        "FUNDCODE": "000001",
        "TITLE": "华夏成长：投资组合公告",
        "ShortTitle": "华夏成长混合",
        "NEWCATEGORY": "3,6",
        "PUBLISHDATE": "2002-10-25T00:00:00",
        "PUBLISHDATEDesc": "2002-10-25",
        "ATTACHTYPE": "5",
        "ID": "AN201202250003556838"
    }],
    "ErrCode": 0,
    "ErrMsg": null,
    "TotalCount": 103,
    "Expansion": 3,
    "PageSize": 20,
    "PageIndex": 6
  }

  """
  # base_url=f"http://fundf10.eastmoney.com/jjgg_{code}_3.html"\
  url = f"https://api.fund.eastmoney.com/f10/JJGG?&fundcode={code}&pageIndex={page_index}&pageSize=20&type=3"
  # Page size has a upper limit of 100. We use a fixed value of 20 here.
  # type=3 means annual & quarterly report, plz see base_url.
  r = requests.get(url,headers={"Referer":"http://fundf10.eastmoney.com/"})
  assert r.status_code==200
  result: dict = r.json()
  # print(len(result["Data"]))
  # print(yaml.dump(result,allow_unicode=True,))
  assert result["ErrCode"]==0, result["ErrMsg"]
  return result

def get_fund_report(code:str = "000001"):
  r = get_fund_report_list(code)
  data = []
  while True:
    data.extend(r["Data"])
    # print(data)
    if r["PageIndex"]*r["PageSize"] >= r["TotalCount"]:
      break
    r = get_fund_report_list(code,r["PageIndex"]+1)
  if len(data) != r["TotalCount"]:
    print("Report list not complete.")
  return data


def fetch_pdf(id,dir_name,file_name):
  if not os.path.exists(dir_name):
    try:
      os.mkdir(dir_name)
    except FileExistsError:
      pass
  file_path = dir_name+'/'+file_name.replace('/','')+'.pdf'
  if not os.path.exists(file_path):
    print(f"New report downloading: {file_name}")
    url = f"https://pdf.dfcfw.com/pdf/H2_{id}_1.pdf" # IDK what does H2 and 1 mean
    # print(url)
    bin = requests.get(url)
    with open(file_path, 'wb') as f:
      f.write(bin.content)
import requests
import json
import pandas as pd

def read_ann(art_code: str, show_all: int = 1) -> pd.DataFrame:
    """
    https://np-cnotice-fund.eastmoney.com/api/content/ann?client_source=web_fund&show_all=1&art_code=AN202308281595906195
    获取基金公告数据

    参数:
    art_code: 文章代码
    show_all: 默认为1

    返回:
    df: pandas DataFrame，包含公告数据
    """
    # API的基础URL
    base_url = "https://np-cnotice-fund.eastmoney.com/api/content/ann"

    # 查询参数
    params = {
        "client_source": "web_fund",
        "show_all": show_all,
        "art_code": art_code
    }

    # 请求头部
    headers = {
        "Referer": "https://fund.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }

    # 发送请求
    response = requests.get(base_url, params=params, headers=headers)

    # 解析返回的数据
    try:
        data = json.loads(response.text)
    except json.JSONDecodeError as e:
        print(f"解析JSON时出错: {e}")

    # 获取公告数据
    ann_data = data['data']

    # 输出报告内容
    return ann_data['notice_content']




import pandas as pd
import numpy as np
import base64
import io
import matplotlib.pyplot as plt
def get_net_value(fund_code='007119', page_index=1, page_size=3000, start_date='', end_date=''):
    base_url = "http://api.fund.eastmoney.com/f10/lsjz"
    params = {
        "fundCode": fund_code,
        "pageIndex": page_index,
        "pageSize": page_size,
        "startDate": start_date,
        "endDate": end_date,
    }
    headers = {
        "Referer": "http://fundf10.eastmoney.com/",
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(base_url, params=params, headers=headers)
    data = response.json()
    df = pd.DataFrame(data['Data']['LSJZList'])
    column_mapping = {
        'FSRQ': '净值日期', 'DWJZ': '单位净值', 'LJJZ': '累计净值', 'JZZZL': '日增长率'
    }
    df = df.rename(columns=column_mapping)
    df['净值日期'] = pd.to_datetime(df['净值日期'])
    df['单位净值'] = df['单位净值'].astype(float).round(4)
    df['累计净值'] = df['累计净值'].astype(float).round(4)
    df['日增长率'] = df['日增长率'].apply(lambda x: f"{x}%")
    return df[['净值日期', '单位净值', '累计净值', '日增长率']].sort_values(by='净值日期').reset_index(drop=True)


def get_main_holders(code):
    global quarter
    try:
        # 从 API 获取数据
        data = api.datacenter_api.get_report_data(
            report_name='RPT_MAINDATA_MAIN_POSITIONDETAILS',
            filter_condition=f'(SECURITY_CODE={code})(REPORT_DATE=\'{quarter_to_date(quarter)}\')(ORG_TYPE="基金")',
            sortcolumns='HOLD_MARKET_CAP',
            sortTypes='-1',
            columns='SECURITY_NAME_ABBR,HOLD_MARKET_CAP,FREE_SHARES_RATIO,ORG_NAME_ABBR',
            pagesize=500
        )
        # 数据处理
        holdings = (
            data.groupby(['ORG_NAME_ABBR'])
                .agg({'HOLD_MARKET_CAP': 'sum', 'FREE_SHARES_RATIO': 'sum'})
                .sort_values('HOLD_MARKET_CAP', ascending=False)
                .head(5)
                .reset_index()
        )

        # 利用 Pandas 构建字符串列
        holdings['info'] = holdings['ORG_NAME_ABBR'] + "：" + (holdings['HOLD_MARKET_CAP'] / 1e8).map("{:.2f}亿元".format) + "（" + holdings['FREE_SHARES_RATIO'].map("{:.2f}%".format) + "）"

        # 创建基金公司列
        holdings['company'] = '基金公司' + (holdings.index + 1).astype(str)

        # 重塑 DataFrame，使其变为两列，并转换为字典
        holdings_dict = holdings[['company', 'info']].set_index('company')['info'].to_dict()

        # 添加股票代码和名称
        holdings_dict['代码'] = code

        return holdings_dict

    except Exception as e:
        # 在出现异常时返回一个包含股票代码的字典，其他字段为空
        return {'代码': code, '基金公司1': '', '基金公司2': '', '基金公司3': '', '基金公司4': '', '基金公司5': ''}

# 定义一个执行多线程的函数
def fetch_data_concurrently(codes):
    with ThreadPoolExecutor(max_workers=5) as executor:  # 这里的 max_workers 可根据您的系统和需求进行调整
        results = executor.map(get_main_holders, codes)
    return list(results)


def get_jjhsl_data(fundcode="007119", pageindex=1, pagesize=50):
    # API的基本URL
    base_url = "http://api.fund.eastmoney.com/f10/JJHSL/"

    # 查询参数
    params = {
        "fundcode": fundcode,     # 基金代码
        "pageindex": pageindex,   # 页码
        "pagesize": pagesize,     # 每页的记录数
    }

    # 请求头信息
    headers = {
        "Referer": "http://fundf10.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }

    # 发送请求
    response = requests.get(base_url, params=params, headers=headers)
    # 解析返回的数据
    try:
        data = json.loads(response.text)
        '''
        {'Data': [{'REPORTDATE': '2023-06-30', 'STOCKTURNOVER': 50.55}, {'REPORTDATE': '2022-12-31', 'STOCKTURNOVER': 48.48}, {'REPORTDATE': '2022-06-30', 'STOCKTURNOVER': 42.99}, {'REPORTDATE': '2021-12-31', 'STOCKTURNOVER': 122.07}, {'REPORTDATE': '2021-06-30', 'STOCKTURNOVER': 156.23}, {'REPORTDATE': '2020-12-31', 'STOCKTURNOVER': 169.02}, {'REPORTDATE': '2020-06-30', 'STOCKTURNOVER': 173.14}, {'REPORTDATE': '2019-12-31', 'STOCKTURNOVER': 113.94}, {'REPORTDATE': '2019-06-30', 'STOCKTURNOVER': 77.71}], 'ErrCode': 0, 'ErrMsg': None, 'TotalCount': 9, 'Expansion': None, 'PageSize': 20, 'PageIndex': 1}
        '''
        return pd.DataFrame(data['Data']).sort_values(by = 'REPORTDATE',ascending = True)
    except json.JSONDecodeError as e:
        print(f"解析JSON时出错: {e}")
        return pd.DataFrame()