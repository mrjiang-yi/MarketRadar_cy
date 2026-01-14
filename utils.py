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
    """
    发送增强型富文本消息到飞书机器人
    1. 展示容量提升至 12 个
    2. 加入涨跌趋势图标与均线状态
    """
    if not webhook_url:
        print("⚠️ 提示: 未配置 FEISHU_WEBHOOK_URL，跳过推送")
        return False

    try:
        # 按照你原始代码的路径提取数据
        # 如果你的 main.py 结构没变，这里依然使用 "market_kline" 和 "自定义标的"
        custom_funds = report_data.get("market_kline", {}).get("自定义标的", [])
        
        # 1. 构造增强型预览文字
        content_lines = []
        # --- 关键修改：容量从 [:10] 增加到 [:12] ---
        for f in custom_funds[:12]:
            name = f.get('name', '未知')
            price = f.get('close', 0.0)
            
            # 尝试获取涨跌幅 (如果你的 main.py 已计算该字段)
            chg = f.get('change_pct', 0.0)
            trend_icon = "🔺" if chg >= 0 else "🔻"
            
            # 尝试获取均线状态 (如果 price > ma20)
            ma20 = f.get('ma20')
            ma_status = ""
            if ma20:
                ma_status = " [20日线上]" if price > ma20 else " [20日线下]"

            # 组装单行内容
            line = f"• {name}: {price} ({trend_icon}{abs(chg):.2f}%){ma_status}"
            content_lines.append([{"tag": "text", "text": line}])

        # 2. 构造飞书消息体 (富文本格式)
        report_date = report_data.get('report_date', 'Today')
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"📈 MarketRadar 行情快报 ({report_date})",
                        "content": [
                            [{"tag": "text", "text": "✅ 核心标的数据已更新（前12项）："}]
                        ] + content_lines + [
                            [{"tag": "text", "text": "---------------------------"}] ,
                            [{"tag": "text", "text": "📂 更多技术指标与全量 JSON 请查看邮件附件。"}]
                        ]
                    }
                }
            }
        }
        
        headers = {"Content-Type": "application/json"}
        response = requests.post(webhook_url, data=json.dumps(payload), headers=headers, timeout=10)
        
        if response.status_code == 200:
            print("🚀 飞书 12 项指标消息发送成功！")
            return True
        else:
            print(f"❌ 飞书推送失败: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False
























