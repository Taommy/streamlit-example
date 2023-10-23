import requests
import json
import pandas as pd

class EastmoneydatacenterApi:

    def __init__(self):
        self.base_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

        self.report_dict = {
        'RPTA_WEB_RZRQ_GGMX': '融资融券明细',
        'RPT_BILLBOARD_PERFORMANCEHIS': '龙虎榜单',
        'RPT_DATA_BLOCKTRADE': '大宗交易',
        'RPTA_APP_ACCUMDETAILS': '股权质押',
        'RPTA_WEB_BGCZMX': '并购重组',
        'RPTA_WEB_GETHGLIST_NEW': '股票回购',
        'RPTA_WEB_ZDHT_LIST': '重大合同',
        'RPT_RELATED_TRADE': '关联交易',
        'RPTA_WEB_ZQTZMX': '证券投资',
        'RPTA_WEB_GQTZMX': '期权投资',
        'RPTA_WEB_WTLCMX': '委托理财',
        'RPT_DMSK_HOLDERS': '十大流通股东',
        'RPT_F10_EH_FREEHOLDERS': '十大流通股东',
        'RPT_GENERALMEETING_DETAIL': '股东大会',
        'RPT_LIFT_STAGE': '限售解禁',
        'RPT_SHARE_HOLDER_INCREASE': '股东增减持',
        'RPT_EXECUTIVE_HOLD_DETAILS': '高管持股变动',
        'RPT_ORG_SURVEY': '机构调研',
        'RPT_ORG_SURVEYNEW': '机构调研',
        'RPT_MAINDATA_MAIN_POSITIONDETAILS': '持仓明细',
        'RPT_HS_MAINOP_PRODUCT': '主盈业务构成',

        #经济数据
        'RPT_ECONOMY_CPI':'居民消费价格指数(CPI)',
        'RPT_ECONOMY_PPI': '工业品出厂价格指数(PPI)',
        'RPT_ECONOMY_GDP':'国内生产总值(GDP)',
        'RPT_ECONOMY_PMI':'采购经理人指数(PMI)',
        'RPT_ECONOMY_ASSET_INVEST':'城镇固定资产投资',
        'RPT_ECONOMY_BOOM_INDEX':'企业景气及企业家信心指数',
        'RPT_ECONOMY_TOTAL_RETAIL':'社会消费品零售总额',
        # detail.js
        'RPT_VALUEANALYSIS_DET': '估值分析',
        'RPT_VALUEINDUSTRY_DET': '行业估值',
        'RPT_VALUEINDUSTRY_STA': '行业概况'
}

    def get_report_data(self, report_name, filter_condition=None, columns='ALL',pagesize=30,sortcolumns='',sortTypes=1,quotecolumns=''):


        # 构造请求参数
        params = {
            "sortColumns": sortcolumns,
            "sortTypes": sortTypes,
            "pageSize": pagesize,
            "pageNumber": 1,
            "reportName": report_name,
            "columns": columns,
            "quoteColumns": quotecolumns,
            "filter": filter_condition
        }

        # 发送请求
        response = requests.get(self.base_url, params=params)

        # 解析响应
        json_str = response.text[response.text.index("{"):response.text.rindex("}")+1]
        data = json.loads(json_str)

        if 'result' in data and 'data' in data['result']:
            df = pd.DataFrame(data['result']['data'])
            return df

        else:
            print("没有找到数据")
            return None

class EastmoneyFundApi:

     def __init__(self):
        self.api_host = "http://api.fund.eastmoney.com"
        self.session = requests.Session()
        self.headers = {
            'Referer': 'https://fundf10.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        self.report_dict = {
              'lsjz': '历史净值',
              'lssy': '历史收益',
              'FundGPTop10': '基金持仓',
              'JJHSL': '基金换手率',
              'lssy': '历史收益',  # Historical Earnings
              'JJGG': '基金公告',  # Fund Announcements, fund_type: 0-6 全部公告,发行运作,分红送配,定期报告,人事调整,基金销售,其他公告
              'FHGG': '分红公告',  # Dividend Announcements 目前空值
              'HYPZ': '行业配置',  # Industry Allocation，需要year,可以year=''
              'JJPJ':'基金评级',

              # ... [其他报告名称和描述]
          }

     def get_data(self, report_name, code, pindex=1, pageSize=20, start_date='', end_date='',fund_type='',year=''):
        request_url = f"{self.api_host}/f10/{report_name}?fundCode={code}&pageIndex={pindex}&pageSize={pageSize}&startDate={start_date}&endDate={end_date}"
        params = {
                "type":fund_type,
                'year':year,
            }
        response = self.session.get(request_url, headers=self.headers,params=params)
        if response.status_code == 200:
            jsondata = response.json()
            if jsondata and jsondata.get('ErrCode') == 0:
                data = jsondata['Data']
                return data
            else:
                print("No data available")
                print(response.url)
                print(jsondata)
                return None

        else:
            response.raise_for_status()

     def load_gpxq(self, code, gpdm):
        """
        Load stock details
        """
        params = {
            'type': 'jjjl',
            'code': code,
            'gpdm': gpdm,
        }
        request_url = f"https://fundf10.eastmoney.com/F10DataApi.aspx?"
        response = self.session.get(request_url, headers=self.headers, params=params)
        return response.text
# 主API类，组合所有的子API
class EastmoneyApi:
    def __init__(self):
        self.datacenter_api = EastmoneydatacenterApi()
        self.fund_api = EastmoneyFundApi()
#        self.quote_api = EastmoneyQuoteApi()