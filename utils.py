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
    发送消息到飞书 - 修复版 (数值驱动，红涨绿跌，不漏数据)
    """
    if not webhook_url:
        print("⚠️ 未配置 FEISHU_WEBHOOK_URL，跳过推送")
        return False

    try:
        # 1. 提取生成时间
        meta = report_data.get('meta', {})
        report_date = meta.get('generated_at', '未知日期')
        
        # 2. 构造标的展示内容 (直接从 market_klines 提取，确保不漏)
        # 这样无论你的分类叫什么，只要在 market_klines 里都会显示
        content_lines = []
        all_klines = report_data.get("market_klines", {})

        for cat_name, items in all_klines.items():
            if not items: continue
            
            # 添加分类标题 (如：券商板块、自定义精选)
            content_lines.append([{"tag": "text", "text": f"\n💠 【{cat_name}】"}])
            
            # 这里的 items 是该分类下的所有标的数据列表
            # 我们需要获取每个标的最新的那一条记录 (通常是 list 的最后一个)
            # 或者如果 main.py 已经处理成单条，则直接遍历
            for item in items:
                name = item.get('name', '未知标的')
                price = item.get('close', 0)
                
                # --- 🎯 精准涨跌幅获取 ---
                # 尝试从不同的可能字段名中获取涨跌幅
                chg = item.get('chg_pct') or item.get('change_pct') or 0
                
                try:
                    chg_val = float(str(chg).replace('%', '')) # 强制转为浮点数
                except:
                    chg_val = 0.0

                # --- 🎯 箭头与颜色判断逻辑 ---
                if chg_val > 0:
                    icon = "🔺"  # 涨：红
                    trend = f"+{chg_val:.3f}%"
                elif chg_val < 0:
                    icon = "🔻"  # 跌：绿
                    trend = f"{chg_val:.3f}%"
                else:
                    icon = "🔹"  # 平/数据未更新：蓝
                    trend = "0.00%"

                content_lines.append([{"tag": "text", "text": f"  • {name}: {price} ({icon} {trend})"}])

        # 3. 构造飞书卡片 payload
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"📊 MarketRadar 市场快报 ({report_date[:10]})",
                        "content": [
                            [{"tag": "text", "text": "✅ 数据源: AkShare / YFinance (已完成去重与计算)"}],
                            [{"tag": "text", "text": f"🕒 生成时间: {report_date}"}]
                        ] + content_lines[:45] # 飞书卡片单次不宜超过 50 行
                    }
                }
            }
        }
        
        headers = {"Content-Type": "application/json"}
        res = requests.post(webhook_url, json=payload, timeout=10)
        
        if res.status_code == 200:
            print("🚀 飞书精准推送成功！")
            return True
        else:
            print(f"❌ 飞书推送失败: {res.text}")
            return False

    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")
        return False






















