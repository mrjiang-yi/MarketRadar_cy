import os
import pandas as pd
import akshare as ak
import yfinance as yf
import requests
import random
import time
import socket
import numpy as np 
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

import utils

# === 尝试导入 MyTT ===
try:
    import MyTT 
except ImportError:
    try:
        import indicators as MyTT
    except ImportError:
        MyTT = None
        print("⚠️ Warning: MyTT.py not found. Technical indicators will be skipped.")

# === 邮件相关库 ===
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

ENV_KEYS = {
    "FMP": os.environ.get("FMP_API_Key"),
}

# ========================================================
# 技术指标计算辅助函数
# ========================================================
def calculate_tech_indicators(df):
    """
    使用 MyTT 计算 MACD, KDJ, RSI
    """
    if MyTT is None or df.empty:
        return {}
    
    try:
        CLOSE = df['close'].values
        HIGH = df['high'].values
        LOW = df['low'].values
        OPEN = df['open'].values
        
        # 处理场外基金只有 close 的情况 (Open/High/Low = Close)
        # 避免 KDJ 计算异常
        
        # 1. MACD (12, 26, 9)
        dif, dea, macd_bar = MyTT.MACD(CLOSE)
        
        # 2. KDJ (9, 3, 3)
        k, d, j = MyTT.KDJ(CLOSE, HIGH, LOW)
        
        # 3. RSI (6)
        rsi6 = MyTT.RSI(CLOSE, 6)
        
        signals = []
        
        # MACD 金叉
        if len(dif) > 1:
            if dif[-2] < dea[-2] and dif[-1] > dea[-1]:
                signals.append("MACD金叉")
            elif dif[-2] > dea[-2] and dif[-1] < dea[-1]:
                signals.append("MACD死叉")
                
        # KDJ 金叉
        if len(k) > 1:
            if k[-2] < d[-2] and k[-1] > d[-1]:
                signals.append("KDJ金叉")
        
        # RSI 超买超卖
        if len(rsi6) > 0:
            if rsi6[-1] > 80:
                signals.append("RSI超买")
            elif rsi6[-1] < 20:
                signals.append("RSI超卖")

        if not signals:
            signals.append("无特殊技术形态")

        return {
            "MACD": round(float(macd_bar[-1]), 4) if len(macd_bar) > 0 else 0,
            "DIF": round(float(dif[-1]), 4) if len(dif) > 0 else 0,
            "DEA": round(float(dea[-1]), 4) if len(dea) > 0 else 0,
            "K": round(float(k[-1]), 2) if len(k) > 0 else 0,
            "D": round(float(d[-1]), 2) if len(d) > 0 else 0,
            "J": round(float(j[-1]), 2) if len(j) > 0 else 0,
            "RSI6": round(float(rsi6[-1]), 2) if len(rsi6) > 0 else 0,
            "Signals": signals
        }

    except Exception as e:
        # print(f"Error calculating indicators: {e}")
        return {}

class MarketFetcher:
    def __init__(self, fetch_start_date, end_date):
        self.session = requests.Session()
        self.fetch_start_date = fetch_start_date
        self.end_date = end_date
    
    def normalize_df(self, df, name):
        """统一清洗K线数据格式"""
        if df.empty: return df
        
        df.columns = [c.lower() for c in df.columns]
        
        if 'date' not in df.columns and '日期' in df.columns:
            df.rename(columns={'日期': 'date'}, inplace=True)
        
        # AkShare 列名映射
        rename_map = {
            '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', 
            '成交量': 'volume', '交易量': 'volume', '持仓量': 'open_interest',
            '成交额': 'amount', '量比': 'volume_ratio',
            '开盘价': 'open', '收盘价': 'close', '最高价': 'high', '最低价': 'low', 
            '净值日期': 'date', '单位净值': 'close', # 适配场外基金
            'date': 'date' 
        }
        df.rename(columns=rename_map, inplace=True)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by='date', ascending=True)
        
        # 场外基金填充 OHLC
        if 'close' in df.columns and 'open' not in df.columns:
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
        
        for col in ['open', 'close', 'high', 'low', 'volume']:
            if col not in df.columns:
                df[col] = 0.0
        
        if 'name' not in df.columns:
            df['name'] = name

        cols_to_numeric = ['open', 'close', 'high', 'low', 'volume', 'amount', 'volume_ratio']
        for col in cols_to_numeric:
            if col in df.columns:
                if df[col].dtype == object:
                     df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')
                else:
                     df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'amount' not in df.columns:
            df['amount'] = df['close'] * df['volume']
            
        final_cols = ['date', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount']
        
        # 补全缺失列
        for col in final_cols:
            if col not in df.columns:
                df[col] = 0.0
        
        df.fillna(0, inplace=True)

        return df[final_cols]

    def fetch_akshare(self, symbol, asset_type):
        if not symbol: return pd.DataFrame()
        max_retries = 3 # 降低重试次数加快速度
        
        for i in range(max_retries):
            retry_msg = f"." if i > 0 else ""
            if i == 0:
                print(f"   ⚡ [AkShare] {symbol} ({asset_type}) ...", end="", flush=True)

            try:
                df = pd.DataFrame()
                start_date_clean = self.fetch_start_date.replace("-", "")
                end_date_clean = self.end_date.replace("-", "")

                if asset_type == "index_us":
                    df = ak.index_us_stock_sina(symbol=symbol)
                elif asset_type == "index_hk":
                    df = ak.stock_hk_index_daily_sina(symbol=symbol)
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
                    # 场内ETF/LOF
                    df = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                elif asset_type == "stock_zh_a":
                    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date_clean, end_date=end_date_clean, adjust="qfq")
                elif asset_type == "fund_open":
                    # 【新增】场外基金/LOF净值
                    # 注意：fund_open_fund_info_em 返回的是全量历史，不需要start/end
                    df = ak.fund_open_fund_info_em(fund=symbol, indicator="单位净值走势")
                
                if not df.empty:
                    print(" ✅")
                    return df
                else:
                    if i == max_retries - 1: print(" ❌ (空)")
                    time.sleep(1)

            except Exception as e:
                if i == max_retries - 1: print(f" ❌ {str(e)[:20]}")
                time.sleep(1)
                continue
        
        return pd.DataFrame()

    def fetch_yfinance(self, symbol):
        if not symbol: return pd.DataFrame()
        # 略微简化打印
        print(f"   ⚡ [YFinance] {symbol} ...", end="", flush=True)
        try:
            df = yf.download(symbol, start=self.fetch_start_date, end=self.end_date, progress=False, auto_adjust=False)
            if not df.empty:
                df = df.reset_index()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)
                print(" ✅")
                return df
            else:
                print(" ❌")
                return pd.DataFrame()
        except Exception:
            print(" ❌")
            return pd.DataFrame()

    def get_kline_data(self, name, config):
        # 优先 AkShare
        df = pd.DataFrame()
        if config.get("ak"):
            df = self.fetch_akshare(config.get("ak"), config.get("type"))
            df = self.normalize_df(df, name)
        
        # 失败则 YFinance
        if df.empty and config.get("yf"):
            df = self.fetch_yfinance(config.get("yf"))
            df = self.normalize_df(df, name)
            
        return df

def fetch_group_data(fetcher, targets, group_name, report_start_date, end_date):
    print(f"\n🚀 处理: {group_name}")
    
    kline_list = []
    ma_list = []
    status_logs = []
    
    def fetch_task(name, config):
        try:
            df = fetcher.get_kline_data(name, config)
            if df.empty:
                return None, None, {'name': name, 'status': False, 'error': "No data"}
            
            df = df.sort_values(by='date', ascending=True)
            
            # 计算均线
            ma_info_list = utils.calculate_ma(df) 
            ma_info = ma_info_list[0] if ma_info_list else None
            
            # 计算指标
            tech_indicators = calculate_tech_indicators(df)
            if ma_info:
                ma_info.update(tech_indicators)

            # 切片用于前端/JSON展示
            df_slice = df[(df['date'] >= pd.to_datetime(report_start_date)) & (df['date'] <= pd.to_datetime(end_date))].copy()
            if not df_slice.empty:
                df_slice['date'] = df_slice['date'].dt.strftime('%Y-%m-%d')
                kline_records = df_slice.to_dict(orient='records')
            else:
                kline_records = []
            
            return kline_records, ma_info, {'name': name, 'status': True, 'error': None}

        except Exception as e:
            return None, None, {'name': name, 'status': False, 'error': str(e)}

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_name = {executor.submit(fetch_task, name, config): name for name, config in targets.items()}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result(timeout=45) 
                klines, ma, status = result
                status_logs.append(status)
                if klines: kline_list.extend(klines)
                if ma: ma_list.append(ma)
            except Exception as e:
                status_logs.append({'name': name, 'status': False, 'error': str(e)})

    # 按最新日期倒序整理
    if kline_list:
        temp_df = pd.DataFrame(kline_list)
        temp_df.sort_values(by=['date', 'name'], ascending=[False, True], inplace=True)
        final_kline_data = temp_df.to_dict(orient='records')
    else:
        final_kline_data = []

    return final_kline_data, ma_list, status_logs

def send_email(subject, body, attachment_files, sender_email, sender_password, receiver_email, smtp_server, smtp_port, enable_email):
    if not enable_email or not sender_email or not sender_password:
        return

    print("\n📧 发送邮件...")
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    for file_path in attachment_files:
        if os.path.exists(file_path):
            with open(file_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            filename = os.path.basename(file_path)
            part.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(part)

    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("✅ 邮件发送成功")
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")
