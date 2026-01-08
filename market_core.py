import os
import pandas as pd
import akshare as ak
import yfinance as yf
import requests
import random
import time
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

import utils

# === 邮件相关库 ===
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# -----------------------------------------------------------------------------
# 配置区域（需要传递给 MarketRadar.py 使用的也可以放在这里，或者保留在原处，
# 这里主要放 Fetcher 逻辑和通用函数）
# -----------------------------------------------------------------------------

ENV_KEYS = {
    "FMP": os.environ.get("FMP_API_Key"),
}

class MarketFetcher:
    def __init__(self, fetch_start_date, end_date):
        self.session = requests.Session()
        self.fetch_start_date = fetch_start_date
        self.end_date = end_date
    
    def normalize_df(self, df, name):
        """统一清洗K线数据格式并自动补全指标"""
        if df.empty: return df
        
        # 1. 统一列名 (Lower case)
        df.columns = [c.lower() for c in df.columns]
        
        # 2. 处理日期列名
        if 'date' not in df.columns and '日期' in df.columns:
            df.rename(columns={'日期': 'date'}, inplace=True)
        
        # 3. 处理 AkShare 中文列名映射
        rename_map = {
            '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', 
            '成交量': 'volume', '交易量': 'volume', '持仓量': 'open_interest',
            '成交额': 'amount', '量比': 'volume_ratio',
            '开盘价': 'open', '收盘价': 'close', '最高价': 'high', '最低价': 'low', 
            'date': 'date' 
        }
        df.rename(columns=rename_map, inplace=True)
        
        # 4. 确保日期格式并排序 (计算指标必须按时间顺序)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date', ascending=True)
        
        # 5. 确保包含基础列 (不存在则先置为空)
        for col in ['open', 'close', 'high', 'low', 'volume']:
            if col not in df.columns:
                df[col] = 0.0
        
        if 'name' not in df.columns:
            df['name'] = name

        # 6. 数值转换 (处理可能的字符串, 逗号等)
        cols_to_numeric = ['open', 'close', 'high', 'low', 'volume', 'amount', 'volume_ratio']
        for col in cols_to_numeric:
            if col in df.columns:
                if df[col].dtype == object:
                     df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')
                else:
                     df[col] = pd.to_numeric(df[col], errors='coerce')

        # 7. 补全/计算 成交额 (Amount)
        if 'amount' not in df.columns or df['amount'].isna().all():
            df['amount'] = df['close'] * df['volume']
        else:
            df['amount'] = df['amount'].fillna(df['close'] * df['volume'])
            
        # 8. 补全/计算 量比 (Volume Ratio)
        need_calc_vr = False
        if 'volume_ratio' not in df.columns:
            need_calc_vr = True
        elif df['volume_ratio'].isna().all():
            need_calc_vr = True
        
        if need_calc_vr:
            ma5_vol = df['volume'].rolling(window=5, min_periods=1).mean().shift(1)
            df['volume_ratio'] = df['volume'] / ma5_vol
            df['volume_ratio'] = df['volume_ratio'].replace([float('inf'), -float('inf')], 0.0).fillna(0.0)

        # 9. 最终列筛选与填充
        final_cols = ['date', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount', 'volume_ratio']
        
        for col in final_cols:
            if col not in df.columns:
                df[col] = 0.0
        
        df.fillna(0, inplace=True)

        # 格式化特定列：如果值为 0 则输出 "-"
        cols_to_check = ['volume', 'amount', 'volume_ratio']
        for col in cols_to_check:
            def replace_zero(x):
                try:
                    if float(x) == 0:
                        return "-"
                except:
                    pass
                return x
            df[col] = df[col].apply(replace_zero)

        return df[final_cols]

    def fetch_akshare(self, symbol, asset_type):
        if not symbol: return pd.DataFrame()
        max_retries = 5
        
        for i in range(max_retries):
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [AkShare] 请求: {symbol} ({asset_type}){retry_msg} ...", end="", flush=True)

            try:
                df = pd.DataFrame()
                start_date_clean = self.fetch_start_date.replace("-", "")
                end_date_clean = self.end_date.replace("-", "")

                if asset_type == "index_us":
                    df = ak.index_us_stock_sina(symbol=symbol)
                elif asset_type == "index_hk":
                    df = ak.stock_hk_index_daily_sina(symbol=symbol)
                elif asset_type == "gold_cn":
                    df = ak.spot_hist_sge(symbol=symbol)
                elif asset_type == "future_foreign":
                    df = ak.futures_foreign_hist(symbol=symbol)
                elif asset_type == "stock_hk":
                    df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
                elif asset_type == "stock_vn":
                    try:
                        df = ak.stock_vn_hist(symbol=symbol)
                    except:
                        df = pd.DataFrame()
                elif asset_type == "stock_us":
                    df = ak.stock_us_daily(symbol=symbol, adjust="qfq")
                elif asset_type == "future_zh_sina":
                    df = ak.futures_main_sina(symbol=symbol)
                elif asset_type == "etf_zh":
                    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                elif asset_type == "stock_zh_a":
                    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                
                if not df.empty:
                    print(" ✅")
                    return df
                else:
                    print(" ❌ (空数据)")
                    return pd.DataFrame()

            except Exception as e:
                print(f" ❌ (Err: {str(e)[:15]})")
                if i < max_retries - 1:
                    time.sleep(2)
                continue
        
        print(" ❌ (AkShare多次重试失败, 放弃)")
        return pd.DataFrame()

    def fetch_yfinance(self, symbol):
        if not symbol: return pd.DataFrame()
        max_retries = 5
        
        for i in range(max_retries):
            retry_msg = f" [重试{i}]" if i > 0 else ""
            print(f"   ⚡ [YFinance] 请求: {symbol}{retry_msg} ...", end="", flush=True)
            
            try:
                df = yf.download(symbol, start=self.fetch_start_date, end=self.end_date, progress=False, auto_adjust=False)
                if not df.empty:
                    df = df.reset_index()
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    print(" ✅")
                    return df
                else:
                    print(" ❌ (空数据)")
                    return pd.DataFrame()
            except Exception as e:
                print(f" ❌ (Err: {str(e)[:15]})")
                if i < max_retries - 1:
                    time.sleep(2)
                continue

        print(" ❌ (YFinance多次重试失败, 放弃)")
        return pd.DataFrame()

    def fetch_fmp(self, name):
        key = ENV_KEYS.get("FMP")
        if not key: return pd.DataFrame()
        
        symbol_map = {
            "纳斯达克": "^IXIC", "标普500": "^GSPC", 
            "黄金(COMEX)": "GCUSD", "VNM(ETF)": "VNM",
            "越南胡志明指数": "^VNINDEX"
        }
        symbol = symbol_map.get(name)
        if not symbol: return pd.DataFrame()

        print(f"   ⚡ [FMP] 请求: {symbol} ...", end="", flush=True)
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={self.fetch_start_date}&to={self.end_date}&apikey={key}"
            res = requests.get(url, timeout=10) 
            data = res.json()
            if "historical" in data:
                df = pd.DataFrame(data["historical"])
                print(" ✅")
                return df
        except:
            print(" ❌")
        return pd.DataFrame()

    def get_kline_data(self, name, config):
        print(f"正在获取 K线 [{name}] ...")
        time.sleep(random.uniform(1.0, 3.0))
        
        df = pd.DataFrame()
        if config.get("ak"):
            df = self.fetch_akshare(config.get("ak"), config.get("type"))
            df = self.normalize_df(df, name)
        
        if df.empty:
            df = self.fetch_yfinance(config.get("yf"))
            df = self.normalize_df(df, name)

        if df.empty:
            df = self.fetch_fmp(name)
            df = self.normalize_df(df, name)
            
        return df

def fetch_group_data(fetcher, targets, group_name, report_start_date, end_date):
    """
    通用函数：返回 (K线数据列表, 均线数据列表, 状态日志列表)
    """
    print(f"\n🚀 开始处理任务组: {group_name} (并发模式)")
    
    kline_list = []
    ma_list = []
    status_logs = []
    
    def fetch_task(name, config):
        try:
            df = fetcher.get_kline_data(name, config)
            if df.empty:
                return None, None, {'name': name, 'status': False, 'error': "Data source returned empty after retries"}
            
            df = df.sort_values(by='date', ascending=True)

            ma_info_list = utils.calculate_ma(df) 
            ma_info = ma_info_list[0] if ma_info_list else None

            df_slice = df[(df['date'] >= pd.to_datetime(report_start_date)) & (df['date'] <= pd.to_datetime(end_date))].copy()
            
            if df_slice.empty:
                return None, ma_info, {'name': name, 'status': True, 'error': None}
            
            df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d')
            kline_records = df_slice.to_dict(orient='records')
            
            return kline_records, ma_info, {'name': name, 'status': True, 'error': None}

        except Exception as e:
            print(f"❌ 任务 {name} 异常: {e}")
            return None, None, {'name': name, 'status': False, 'error': str(e)}

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_name = {executor.submit(fetch_task, name, config): name for name, config in targets.items()}
        
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result(timeout=15)
                klines, ma, status = result
                
                status_logs.append(status)
                
                if klines:
                    kline_list.extend(klines)
                else:
                    print(f"⚠️ 警告: 无法获取 {name} 的K线数据")
                
                if ma:
                    ma_list.append(ma)
                    
            except TimeoutError:
                print(f" 💀 严重超时: 获取 {name} 超过15秒无响应，强制跳过！")
                status_logs.append({'name': name, 'status': False, 'error': "Thread timed out (15s)"})
            except Exception as e:
                print(f"❌ 处理 {name} 结果时出错: {e}")
                status_logs.append({'name': name, 'status': False, 'error': f"Processing error: {str(e)}"})

    if kline_list:
        temp_df = pd.DataFrame(kline_list)
        temp_df.sort_values(by=['date', 'name'], ascending=[False, True], inplace=True)
        final_kline_data = temp_df.to_dict(orient='records')
    else:
        final_kline_data = []

    return final_kline_data, ma_list, status_logs

def send_email(subject, body, attachment_files, sender_email, sender_password, receiver_email, smtp_server, smtp_port, enable_email):
    """
    发送带有多个附件的邮件 (QQ邮箱使用 SMTP_SSL:465)
    """
    if not enable_email:
        print("\n🔕 邮件功能已关闭，跳过发送。")
        return
    
    if not sender_email or not sender_password:
        print("\n❌ 错误：未检测到 SENDER_EMAIL 或 SENDER_PASSWORD 环境变量，无法发送邮件！")
        return

    print("\n📧 正在准备发送邮件...")
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    attachment_count = 0
    for file_path in attachment_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                
                filename = os.path.basename(file_path)
                part.add_header('Content-Disposition', 'attachment', filename=filename)
                
                msg.attach(part)
                print(f"   📎 已添加附件: {filename}")
                attachment_count += 1
            except Exception as e:
                print(f"   ❌ 添加附件 {file_path} 失败: {e}")
        else:
            print(f"   ⚠️ 附件文件不存在: {file_path}")

    if attachment_count == 0:
        print("⚠️ 警告: 没有有效附件被添加，仍尝试发送邮件...")

    try:
        print(f"🚀 连接 SMTP 服务器 {smtp_server}:{smtp_port} (SSL) 并发送...")
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("✅ 邮件发送成功！")
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")