import streamlit as st
from eastmoneyapi import EastmoneyApi
from extractor import *
from processor import *
import pandas as pd
FONT_CSS = """
<style>
    /* 定义全局英文字体为 Times New Roman */
    body {
        font-family: 'Times New Roman', sans-serif;
    }

    /* 定义标题的字体 */
    h1 {
        font-family: 'Times New Roman', sans-serif;
    }
    /* 定义Streamlit标题的字体 */
    .stMarkdown h1 {
        font-family: 'Times New Roman', sans-serif;
    }

    /* 定义Streamlit输入框的字体 */
    .stTextInput input {
        font-family: 'Times New Roman', sans-serif;
    }
    /* 定义输入框的字体 */
    input {
        font-family: 'Times New Roman', sans-serif;
    }

    /* 定义表格中的中文字体为楷体 */
    table {
        font-family: 'KaiTi', 'Times New Roman', sans-serif;
    }
</style>
"""
st.markdown(FONT_CSS, unsafe_allow_html=True)
mobile_css = """
<style>
    @media only screen and (max-width: 768px) {
        table {
            display: block;
            overflow-x: auto;
            white-space: nowrap;
        }
        th, td {
            font-size: 10px; /* 适当调整字体大小 */
            padding: 5px !important; /* 减少填充 */
        }
        body, .stTextInput input {
            font-size: 12px; /* 调整非表格文本的字体大小 */
        }
        .stTextInput input {
            height: 3em; /* 调整输入框大小 */
        }
    }
</style>
"""
st.markdown(mobile_css, unsafe_allow_html=True)
# 添加一个标题
st.title('基金持股')

# 用户输入
user_input = st.text_input('请输入基金代码:比如007119')

if  user_input:
    code = user_input
    quarter = '2023Q2'
    sl = [code]
    fields = ['基金名称', '季度', '股票代码', '股票名称','占净值比例', '持仓市值(亿元)','最新价','持股数（万股）','股息率', "市盈(动)","所属行业"]
    api = EastmoneyApi()



    # 获取股票代码列表
    #stock_codes =  ['002475','600309']
    df = get_fund_data(sl = sl,fields = fields)
    # 提取基金名称和基金代码
    fund_name = df.iloc[0]['基金名称']
    fund_code = code  # 您提供的基金代码
    fund_quarter = quarter
    # 在Streamlit应用中显示数据框
    st.subheader('基金名称: ' + fund_name)
    st.text('基金代码: ' + fund_code)
    st.text('季度: ' + fund_quarter)
    # Get the previous quarter using the function
    previous_quarter = get_previous_quarter(quarter)
    # Merge the dataframe with itself to get current and previous quarter's holdings side by side
    merged_df = df[df['季度'] == quarter].merge(df[df['季度'] == previous_quarter], on='股票代码', how='left', suffixes=('', '_prev'))

    # Convert the relevant columns to numeric
    merged_df['持股数（万股）'] = pd.to_numeric(merged_df['持股数（万股）'], errors='coerce')
    merged_df['持股数（万股）_prev'] = pd.to_numeric(merged_df['持股数（万股）_prev'], errors='coerce')

    # Calculate the change rate
    merged_df['同比%'] = merged_df.apply(lambda row: "新增" if pd.isnull(row['持股数（万股）_prev']) else
                                        round((row['持股数（万股）'] - row['持股数（万股）_prev']) / row['持股数（万股）_prev'] * 100, 2), axis=1)

    import io
    import base64

    df_html = (merged_df[['股票代码', '股票名称', '所属行业', '占净值比例', '持仓市值(亿元)','持股数（万股）', '同比%', '股息率', '市盈(动)']]
                    .head(10)
                    .rename(columns={'持仓市值(亿元)': '持仓(亿元)','所属行业':'行业','持股数（万股）':'持股(万股)','股票代码':'代码','股票名称':'名称'})
                    .reset_index(drop=True)
                    .astype(str))
    df_html.columns.name = '序号'
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor
    st.markdown("## 持股数据")  # 添加标题
    st.table(df_html.set_index('代码'))  # 使用Streamlit的dataframe显示功能，并移除索引  # 使用 Streamlit 的 dataframe 显示功能



    # 获取股票代码列表
    stock_codes = df_html['代码'].tolist()

    # 并行获取数据
    holdings_data = fetch_data_concurrently(stock_codes)

    # 使用从 get_main_holders 返回的字典列表创建一个新的 DataFrame
    holdings_df = df_html[['代码','名称']].merge(pd.DataFrame(holdings_data),on='代码').applymap(lambda x: x.replace('基金', '') if isinstance(x, str) else x)

    # 将 DataFrame 转换为 HTML
    html_df = holdings_df.to_html(index=False)



# 如果用户输入了股票代码
if user_input:
    try:
        # 获取数据
        holdings_data = get_main_holders(user_input)

        # 如果返回的数据不是空的
        if holdings_data:
            st.markdown("""
                        <style>
                            table {
                                width: 100%;
                                border-collapse: collapse;
                                font-size: 12px;  /* 这里调整字号大小 */
                                font-family: 'KaiTi';  /* 使用楷体字体 */
                            }
                            th, td {
                                text-align: left;
                                padding: 8px;
                                border: 1px solid #dddddd;
                            }
                            th {
                                background-color: #f2f2f2;
                            }
                            body {
                                font-family: 'KaiTi';  /* 设置页面其他部分的字体为楷体 */
                            }
                        </style>
                    """, unsafe_allow_html=True)

            
            st.markdown("## 持股主要基金")  # 添加标题
            st.table(holdings_df.set_index('代码'))  # 使用 Streamlit 的 dataframe 显示功能
        else:
            st.write("没有找到相关数据。")
    except Exception as e:
        # 如果出现错误，则在Streamlit应用中显示错误信息
        st.error(f"出现错误: {str(e)}")

