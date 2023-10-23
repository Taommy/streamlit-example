import requests
import re
import json
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from eastmoneyapi import EastmoneyApi
from extractor import *
from processor import *
import numpy as np
api = EastmoneyApi()
session = requests.Session()
def quarter_to_date(quarter_str):
    # 解析输入字符串，提取年份和季度
    year, quarter = quarter_str.split('Q')

    # 将季度编号与月份/日期字符串映射
    end_dates = {'1': '03-31', '2': '06-30', '3': '09-30', '4': '12-31'}

    # 构建和返回日期字符串
    if quarter in end_dates:
        return f"{year}-{end_dates[quarter]}"
    else:
        raise ValueError("Invalid quarter format. Expected format is 'YYYYQX' where X is between 1 and 4.")

def process_fund_manager(data):
    if data and 'data' in data and 'now' in data['data']:  # Check if data is not None and contains the required keys
        managers_info = data['data']['now']
        all_managers_data = []

        # Iterate through each manager's data
        for manager_id, manager_info in managers_info.items():
            # Check if 'photo' field exists, if not, use an empty string
            photo_url = manager_info.get('photo') or ''
            manager_data = {
                '姓名': manager_info.get('name'),
                '起始时间': manager_info.get('start'),
                '结束时间': manager_info.get('end'),
                '简介': manager_info.get('intro'),
                '年龄': manager_info.get('age'),
                '学历': manager_info.get('xl'),
            }

            # Extract detailed information about the funds managed by the fund manager
            other_funds = manager_info.get('other', {})
            other_funds_data = []
            for fund_id, fund_info in other_funds.items():
                sy_info = fund_info.get('syInfo', {})  # Get the syInfo dictionary
                other_fund_data = {
                    '基金代码': fund_info.get('code'),
                    '基金名称': fund_info.get('name'),
                    '基金类型': fund_info.get('type'),
                    '开始时间': fund_info.get('start'),
                    '结束时间': fund_info.get('end'),
                    '总收益': sy_info.get('sy', 0),  # Get the value from the syInfo dictionary
                    '平均年化收益': sy_info.get('avgsy', 0),  # Get the value from the syInfo dictionary
                    '最大回撤': sy_info.get('zdhc', 0),  # Get the value from the syInfo dictionary
                    # Add other fields as needed
                }
                other_funds_data.append(other_fund_data)

            manager_data['其他基金'] = other_funds_data  # Add to manager data
            all_managers_data.append(manager_data)

        return all_managers_data
    else:
        print("没有接收到数据")
        return []


def extract_section(text, start_title, end_title):
    start_index = text.find(start_title)
    end_index = text.find(end_title, start_index)

    if start_index != -1 and end_index != -1:
        section = text[start_index:end_index].strip()
    elif start_index != -1:
        section = text[start_index:].strip()
    else:
        return None

    # Find the last occurrence of any of the ending punctuation marks
    last_punct_index = max(section.rfind(punct) for punct in ["。", "？", "！"])
    if last_punct_index != -1:
        return section[:last_punct_index+1].strip()

    return section


def max_drawdown(df):
    dates = df['净值日期'].values
    values = df['累计净值'].values
    running_max = np.maximum.accumulate(values)
    drawdowns = values / running_max - 1.0
    end_date_idx = np.argmin(drawdowns)
    start_date_idx = np.argmax(running_max[:end_date_idx + 1])
    return dates[start_date_idx], dates[end_date_idx], drawdowns[end_date_idx]


def load_and_preprocess_data(filepath):
    # Load and preprocess the data
    df = pd.read_csv(filepath)
    df['净值日期'] = pd.to_datetime(df['净值日期'])
    df['日增长率'] = df['日增长率'].str.replace('%', '').str.strip()
    df['日增长率'] = df['日增长率'].replace('', np.nan)
    df['日增长率'] = df['日增长率'].astype(float) / 100
    return df

def calculate_returns(df):
    # Calculate total and annualized returns
    total_return = df['累计净值'].iloc[-1] / df['累计净值'].iloc[0] - 1
    years = (df['净值日期'].iloc[-1] - df['净值日期'].iloc[0]).days / 365.25
    annualized_return = (1 + total_return) ** (1 / years) - 1
    return total_return, annualized_return

def rolling_returns_volatility(df, window_short=30, window_long=365):
    # Calculate rolling returns and volatility
    df['每日收益'] = df['累计净值'].pct_change()
    df[f'{window_short}天滚动收益'] = df['每日收益'].rolling(window=window_short).sum()
    df[f'{window_long}天滚动收益'] = df['每日收益'].rolling(window=window_long).sum()
    volatility = df['每日收益'].std()
    return df[f'{window_short}天滚动收益'].iloc[-1], df[f'{window_long}天滚动收益'].iloc[-1], volatility

def calculate_max_drawdown(df):
    """
    计算最大回撤及其开始和结束日期

    参数:
    df (DataFrame): 包含日期和累计净值的数据框

    返回:
    tuple: 最大回撤开始日期, 最大回撤结束日期, 最大回撤
    """
    dates = df['净值日期'].values
    values = df['累计净值'].values
    running_max = np.maximum.accumulate(values)  # 计算到目前为止的最大值
    drawdowns = values / running_max - 1.0  # 计算每个点的回撤
    end_date_idx = np.argmin(drawdowns)  # 找到最大回撤的点
    start_date_idx = np.argmax(values[:end_date_idx + 1])  # 找到最大回撤开始的点
    return dates[start_date_idx], dates[end_date_idx], drawdowns[end_date_idx]


def analyze_returns(df):
    # Analyzing the distribution of returns and the ratio of positive to negative return days
    positive_days = df[df['每日收益'] > 0].shape[0]
    negative_days = df[df['每日收益'] < 0].shape[0]
    positive_negative_ratio = positive_days / negative_days if negative_days > 0 else np.nan
    return_distribution = df['每日收益'].describe()
    return positive_days, negative_days, positive_negative_ratio, return_distribution


# Convert hex to rgb
def hex_to_rgb(value):
    """Convert hex color to RGB."""
    value = value.lstrip('#')
    return tuple(int(value[i:i+2], 16) / 255 for i in (0, 2, 4))
def get_cell_color(value):
    # Normalize value to the range [0, 1] with 0% mapping to 0 and 10% mapping to 1
    normalized = value / 0.10
    normalized = max(0, min(1, normalized))  # clamp between 0 and 1

    # Interpolate color
    color = interpolate_color(end_color, start_color, normalized)

    # Convert color tuple back to hex
    return f"#{int(color[0]*255):02X}{int(color[1]*255):02X}{int(color[2]*255):02X}"

def get_previous_quarter(quarter, quarter_num=1):
    year, q = quarter.split('Q')
    year = int(year)
    q = int(q)

    total_quarters = (year * 4 + q) - quarter_num
    year = total_quarters // 4
    q = total_quarters % 4
    if q == 0:
        q = 4
        year -= 1

    return f"{year}Q{q}"

def highlight_entities(text, entities):
    pattern = "|".join(re.escape(entity) for entity in entities)
    return re.sub(pattern, lambda m: f'<span style="color: #FF0000;font-weight: bold;">{m.group(0)}</span>', text)