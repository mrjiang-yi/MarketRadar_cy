import json
import os
import time
import math
import pandas as pd
from datetime import datetime

# 引入接口
import fetch_data
import MarketRadar
# import utils # main.py 不再直接依赖 utils 计算，因为 MarketRadar 已经算好了

# 输出文件名称
OUTPUT_FILENAME = "MarketRadar_Report.json"

def print_banner():
    print(r"""
  __  __            _        _   ____          _            
 |  \/  | __ _ _ __| | _____| |_|  _ \ __ _ __| | __ _ _ __ 
 | |\/| |/ _` | '__| |/ / _ \ __| |_) / _` / _` |/ _` | '__|
 | |  | | (_| | |  |   <  __/ |_|  _ < (_| (_| | (_| | |   
 |_|  |_|\__,_|_|  |_|\_\___|\__|_| \_\__,_\__,_|\__,_|_|   
                                                            
    """)

def clean_and_round(data):
    """
    数据清洗核心逻辑：
    1. 递归遍历字典和列表
    2. 浮点数强制保留2位小数
    3. 处理特殊数值 (NaN/Inf -> None)
    """
    if isinstance(data, dict):
        return {k: clean_and_round(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_and_round(x) for x in data]
    elif isinstance(data, float):
        # 检查 NaN 或 Inf，转换为 None (JSON null)
        if math.isnan(data) or math.isinf(data):
            return None
        return round(data, 2)
    else:
        # 其他类型 (int, str, None) 原样返回
        return data

def merge_data(macro_data, kline_data_dict, ma_data_list):
    """
    合并宏观数据、K线数据和均线数据
    """
    merged = {
        "meta": kline_data_dict.get("meta", {}),
        "技术分析": {
            "周K线移动平均线": ma_data_list # 直接使用 MarketRadar 传回的均线数据
        },
        # 宏观数据部分
        "market_fx": macro_data.get("market_fx", {}),
        "china": macro_data.get("china", {}),
        "usa": macro_data.get("usa", {}),
        "japan": macro_data.get("japan", {}),
        # K线数据部分 (MarketRadar原本放在 "data" 键下)
        "market_klines": kline_data_dict.get("data", {})
    }
    
    # 更新 meta 信息
    merged["meta"]["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged["meta"]["description"] = "MarketRadar Consolidated Report (Macro + Klines + MA)"
    
    return merged

def save_compact_json(data, filename):
    """
    自定义 JSON 保存函数
    功能：强制将列表内的字典对象保持在同一行，实现紧凑格式。
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('{\n')
            
            # 顶层键 (如 meta, market_fx, china, market_klines, 技术分析 等)
            keys = list(data.keys())
            for i, key in enumerate(keys):
                val = data[key]
                
                # 写入 Key
                f.write(f'    "{key}": ')
                
                if isinstance(val, dict):
                    f.write('{\n')
                    sub_keys = list(val.keys())
                    for j, sub_key in enumerate(sub_keys):
                        sub_val = val[sub_key]
                        f.write(f'        "{sub_key}": ')
                        
                        if isinstance(sub_val, list):
                            # === 核心逻辑：如果是列表，强制内部元素单行显示 ===
                            f.write('[\n')
                            for k, item in enumerate(sub_val):
                                # 使用 json.dumps 将单个字典转为单行字符串
                                item_str = json.dumps(item, ensure_ascii=False)
                                comma = "," if k < len(sub_val) - 1 else ""
                                f.write(f'            {item_str}{comma}\n')
                            f.write('        ]')
                        else:
                            # 如果不是列表（例如 meta 中的字符串值），正常 dump
                            f.write(json.dumps(sub_val, ensure_ascii=False))
                        
                        # 处理子项之间的逗号
                        if j < len(sub_keys) - 1:
                            f.write(',\n')
                        else:
                            f.write('\n')
                    f.write('    }')
                else:
                    # 如果顶层值不是字典，直接 dump
                    f.write(json.dumps(val, ensure_ascii=False))
                
                # 处理顶层项之间的逗号
                if i < len(keys) - 1:
                    f.write(',\n')
                else:
                    f.write('\n')
            
            f.write('}')
            
        print(f"\n✅ 成功! 所有数据已合并写入 {filename} (紧凑格式)")
        return True
    except Exception as e:
        print(f"\n❌ 写入合并 JSON 失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    start_time = time.time()
    print_banner()
    print("🚀 MarketRadar 启动主程序...")
    
    # 1. 获取宏观经济数据 (fetch_data)
    print("\n[Step 1/4] 开始获取宏观经济数据...")
    try:
        macro_data = fetch_data.get_data_main()
    except Exception as e:
        print(f"❌ 获取宏观数据失败: {e}")
        macro_data = {}

    # 2. 获取市场K线数据 & 均线数据 (MarketRadar)
    print("\n[Step 2/4] 开始获取全球市场K线数据 & 计算技术指标...")
    try:
        # MarketRadar.get_all_kline_data 现在返回包含 "data" 和 "ma_data" 的字典
        kline_result = MarketRadar.get_all_kline_data()
        
        # 提取 K线数据 (字典: {指数:[], 恒生科技:[]...})
        kline_data_dict = {"meta": kline_result.get("meta"), "data": kline_result.get("data")}
        
        # 提取 均线数据 (列表: [{}, {}...])
        ma_data_list = kline_result.get("ma_data", [])
        
        print(f"✅ 获取到 {len(ma_data_list)} 条均线数据")
        
    except Exception as e:
        print(f"❌ 获取K线数据失败: {e}")
        kline_data_dict = {"meta": {}, "data": {}}
        ma_data_list = []

    # 3. (原步骤3已合并至 Step 2，此处略过)
    print("\n[Step 3/4] (已在 Step 2 中并发完成)")

    # 4. 整合数据
    print("\n[Step 4/4] 整合数据并生成报告...")
    final_data = merge_data(macro_data, kline_data_dict, ma_data_list)
    
    # === 新增：全局数据清洗 (保留两位小数) ===
    print("🧹 [Step 4.5] 执行全局数据清洗 (保留两位小数, 去除NaN)...")
    final_data = clean_and_round(final_data)

    # 5. 保存并发送
    if save_compact_json(final_data, OUTPUT_FILENAME):
        # 发送邮件 (调用 MarketRadar 的邮件功能)
        email_subject = f"MarketRadar全量日报_{datetime.now().strftime('%Y-%m-%d')}"
        email_body = f"""
        MarketRadar 自动化报告
        
        生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        包含模块:
        - 宏观经济数据 (中国/美国/日本/FX)
        - 全球市场K线 (指数/美股/港股/新兴市场)
        - 技术分析 (移动平均线)
        
        附件: {OUTPUT_FILENAME}
        
        System: GitHub Actions / Local
        """
        MarketRadar.send_email(email_subject, email_body, [OUTPUT_FILENAME])

    elapsed = time.time() - start_time
    print(f"\n✨ 所有任务完成，耗时: {elapsed:.2f} 秒")

if __name__ == "__main__":
    main()