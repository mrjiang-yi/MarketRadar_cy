#yuan si de dai ma
import pandas as pd
import numpy as np
import requests
import json
import os

def calculate_ma(df, windows=[5, 10, 20, 60, 120, 250]):
    """
    计算移动平均线
    """
    if df is None or df.empty or 'close' not in df.columns:
        return []

    df = df.sort_values('date').copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    if 'name' in df.columns:
        groups = df.groupby('name')
    else:
        groups = [('Unknown', df)]

    final_results = []

    for name, group_df in groups:
        if len(group_df) < 1:
            continue
            
        latest_record = group_df.iloc[-1].to_dict()
        
        date_val = latest_record.get('date')
        if isinstance(date_val, pd.Timestamp):
            date_str = date_val.strftime('%Y-%m-%d')
        else:
            date_str = str(date_val)

        # 计算涨跌幅 (相对于前一天)
        change_pct = 0.0
        if len(group_df) >= 2:
            prev_close = group_df['close'].iloc[-2]
            curr_close = group_df['close'].iloc[-1]
            if prev_close > 0:
                change_pct = round((curr_close - prev_close) / prev_close * 100, 2)

        ma_data = {
            "名称": name,
            "日期": date_str,
            "收盘价": round(latest_record.get('close'), 2),
            "涨跌幅": f"{change_pct}%"
        }

        for w in windows:
            col_name = f"{w}日均线"
            ma_series = group_df['close'].rolling(window=w).mean()
            latest_ma = ma_series.iloc[-1]
            if pd.notna(latest_ma):
                ma_data[col_name] = round(latest_ma, 2)
            else:
                ma_data[col_name] = None

        final_results.append(ma_data)
        
    return final_results

# def send_to_feishu(webhook_url, report_data):
#     """
#     发送消息到飞书
#     """
#     if not webhook_url:
#         print("⚠️ 未配置 FEISHU_WEBHOOK_URL，跳过推送")
#         return False

#     try:
#         # 提取日期
#         report_date = report_data.get('meta', {}).get('generated_at', 'Unknown')[:10]
        
#         # 提取自定义标的 (用于在卡片中快速预览)
#         custom_funds = report_data.get("market_klines", {}).get("自定义精选", [])
        
#         # 整理自定义标的预览文本 (取前 20 个，防止消息过长)
#         preview_lines = []
#         for f in custom_funds[:20]: 
#             # 这里的 f 是 K线数据的 latest record，我们需要找到它的名字和最新收盘价
#             # market_klines 结构是 {"自定义精选": [{date, name, close...}, {date, name, close...}]}
#             # 但传入的 json 已经是 list of records，可能包含历史数据
#             pass

#         # 上面的 custom_funds 是所有历史数据的扁平列表，我们需要提取每个标的的最新一条
#         # 更方便的是从 ma_data (技术分析) 中提取，因为 calculate_ma 已经只返回最新一条了
#         ma_list = report_data.get("技术分析", {}).get("指数+个股日均线", [])
        
#         # 筛选出属于 "自定义精选" 的标的名称
#         # 这里为了简单，我们直接遍历 ma_list，把几个关键的列出来
        
#         fund_preview = ""
#         # 定义需要高亮关注的关键词
#         keywords = ["ETF", "LOF", "保险", "稀土", "诺安", "人工智能", "有色"]
        
#         count = 0
#         for item in ma_list:
#             name = item.get("名称", "")
#             if any(k in name for k in keywords):
#                 price = item.get("收盘价", 0)
#                 pct = item.get("涨跌幅", "0%")
                
#                 # 简单的 emoji 指示
#                 icon = "🔴" if "-" not in str(pct) and pct != "0%" and pct != "0.0%" else "🟢"
                
#                 fund_preview += f"{icon} {name}: {price} ({pct})\n"
#                 count += 1
#                 if count >= 15: break # 限制显示数量

#         if not fund_preview:
#             fund_preview = "暂无相关标的数据"

#         # 构造飞书卡片消息
#         payload = {
#             "msg_type": "post",
#             "content": {
#                 "post": {
#                     "zh_cn": {
#                         "title": f"📊 MarketRadar 日报 ({report_date})",
#                         "content": [
#                             [{"tag": "text", "text": "✅ 数据抓取任务已完成 (Selenium/AkShare/YFinance)"}],
#                             [{"tag": "text", "text": "\n【重点关注标的】:"}],
#                             [{"tag": "text", "text": fund_preview}],
#                             [{"tag": "text", "text": "\n详细 JSON 报告已生成并发送至邮箱。"}],
#                             [{"tag": "a", "text": "查看 GitHub Actions", "href": "https://github.com/"}]
#                         ]
#                     }
#                 }
#             }
#         }
        
#         headers = {"Content-Type": "application/json"}
#         res = requests.post(webhook_url, data=json.dumps(payload), headers=headers, timeout=10)
        
#         if res.status_code == 200:
#             print("🚀 飞书推送成功！")
#             return True
#         else:
#             print(f"❌ 飞书推送失败: {res.text}")
#             return False

#     except Exception as e:
#         print(f"❌ 飞书推送异常: {e}")
#         return False
def send_to_feishu(webhook_url, report_data):
    try:
        # 注意：这里对应你 main.py 里的 "market_klines" 键名
        all_klines = report_data.get("market_klines", {})
        
        # 定义你想要在飞书预览中显示的板块优先级
        display_categories = ["全球核心指数", "恒生科技", "港股创新药", "大宗商品", "美股七巨头"]
        
        content_lines = []
        for cat in display_categories:
            items = all_klines.get(cat, [])
            if items:
                # 每个板块选最新的一条数据展示
                for stock in items:
                    # 假设每个 stock 字典里有 name, close, change_pct
                    # 注意：如果 items 里的数据还没计算涨跌幅，需要在这里逻辑处理
                    name = stock.get('name', '未知')
                    price = stock.get('close', 0)
                    # 尝试获取涨跌幅，如果没有则不显示
                    chg = stock.get('change_pct', 0) 
                    
                    icon = "🔺" if chg >= 0 else "🔻"
                    content_lines.append([{"tag": "text", "text": f"• {name}: {price} ({icon}{abs(chg)}%)"}])

        # 构造飞书消息体
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"📈 MarketRadar 市场动态 ({report_data.get('meta', {}).get('generated_at', '今日')})",
                        "content": [
                            [{"tag": "text", "text": "核心标的快照："}]
                        ] + content_lines[:15] + [ # 最多显示15行防止刷屏
                            [{"tag": "text", "text": "---------------------------"}] ,
                            [{"tag": "text", "text": "详细均线及信号分析请查看附件 JSON 或邮件。"}]
                        ]
                    }
                }
            }
        }
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        print(f"飞书推送报错: {e}")























