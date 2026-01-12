# selenium_scrapers.py
# -----------------------------------------------------------------------------
# DeepSeek Finance Project - Specific Scraper Implementations
# -----------------------------------------------------------------------------

import time
import pandas as pd
import re
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium_utils

def fetch_cnn_fear_greed(name, url, chrome_options):
    """
    专门抓取 CNN Fear & Greed Index
    结构变动频繁，使用非顺序的独立正则匹配。
    """
    max_retries = 5
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - CNN)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })

            # 设置大窗口以确保桌面布局
            driver.set_window_size(1920, 1080)
            driver.set_page_load_timeout(45)
            driver.get(url)

            try:
                # 滚动到底部，确保 Timeline 历史数据加载
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3) 
            except:
                pass
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Timeline")
                )
            except:
                pass # 超时也继续尝试解析
            
            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            # 1. 当前值
            current_val = None
            match_header = re.search(r"Fear & Greed Index\s+(\d+)", normalized_text, re.IGNORECASE)
            if match_header:
                current_val = int(match_header.group(1))
            else:
                match_timeline = re.search(r"Timeline\s+(\d+)", normalized_text, re.IGNORECASE)
                if match_timeline:
                    current_val = int(match_timeline.group(1))

            # 2. 历史值
            prev_close = 0
            week_ago = 0
            month_ago = 0
            
            m_prev = re.search(r"Previous close\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_prev: prev_close = int(m_prev.group(1))
            
            m_week = re.search(r"1 week ago\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_week: week_ago = int(m_week.group(1))
            
            m_month = re.search(r"1 month ago\s+(\d+)", normalized_text, re.IGNORECASE)
            if m_month: month_ago = int(m_month.group(1))
            
            if current_val is not None:
                record = {
                    "日期": pd.Timestamp.now().strftime('%Y-%m-%d'),
                    "最新值": current_val,
                    "前值": prev_close,
                    "一周前": week_ago,
                    "一月前": month_ago,
                    "description": "CNN Fear & Greed Index"
                }
                print(f"✅ [{name}] 抓取成功! 当前值: {current_val}")
                return name, [record], None
            else:
                raise ValueError("无法解析当前恐惧贪婪指数数值")

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(3)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                    
    return name, [], last_error

def fetch_cboe_data(name, url, chrome_options):
    """
    抓取 CBOE Options Market Statistics
    """
    max_retries = 3
    last_error = None
    
    target_keys = [
        "TOTAL PUT/CALL RATIO",
        "INDEX PUT/CALL RATIO",
        "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO",
        "EQUITY PUT/CALL RATIO",
        "CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO",
        "SPX + SPXW PUT/CALL RATIO",
        "OEX PUT/CALL RATIO",
        "MRUT PUT/CALL RATIO",
        "MXEA PUT/CALL RATIO",
        "MXEF PUT/CALL RATIO",
        "MXACW PUT/CALL RATIO",
        "MXWLD PUT/CALL RATIO",
        "MXUSA PUT/CALL RATIO",
        "CBTX PUT/CALL RATIO",
        "MBTX PUT/CALL RATIO",
        "SPEQX PUT/CALL RATIO",
        "SPEQW PUT/CALL RATIO",
        "MGTN PUT/CALL RATIO",
        "MGTNW PUT/CALL RATIO"
    ]

    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - CBOE)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(45)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "TOTAL PUT/CALL RATIO")
                )
            except:
                pass

            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            records = []
            current_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            
            # 解析日期 (示例: 2026年1月9日)
            date_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", normalized_text)
            if date_match:
                try:
                    y, m, d = date_match.groups()
                    current_date = f"{y}-{int(m):02d}-{int(d):02d}"
                except:
                    pass
            
            data_dict = {"日期": current_date}
            
            found_count = 0
            for key in target_keys:
                pattern = re.escape(key) + r"\s+([\d\.]+)"
                match = re.search(pattern, normalized_text)
                if match:
                    data_dict[key] = float(match.group(1))
                    found_count += 1
                else:
                    data_dict[key] = None
            
            if found_count > 0:
                records.append(data_dict)
                print(f"✅ [{name}] 抓取成功! 获得 {found_count} 个指标, 日期: {current_date}")
                return name, records, None
            else:
                raise ValueError("未匹配到任何 Put/Call Ratio 数据")

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(2)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return name, [], last_error

def fetch_fed_rate_monitor(name, url, chrome_options):
    """
    抓取 Investing.com Fed Rate Monitor Tool
    """
    max_retries = 3
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - FedRate)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(45)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(
                    EC.text_to_be_present_in_element((By.TAG_NAME, "body"), "Fed Interest Rate Decision")
                )
            except:
                pass

            body_text = driver.find_element(By.TAG_NAME, "body").text
            normalized_text = re.sub(r'\s+', ' ', body_text).strip()
            
            # 解析日期
            meeting_date = "Unknown"
            date_match = re.search(r"Meeting Time:\s*([A-Za-z]{3}\s\d{1,2},\s\d{4})", normalized_text)
            if not date_match:
                date_match = re.search(r"Fed Interest Rate Decision\s*([A-Za-z]{3}\s\d{1,2},\s\d{4})", normalized_text)
            if date_match:
                meeting_date = date_match.group(1).strip()
            
            # 解析概率表
            table_pattern = r"(\d+\.\d+\s*-\s*\d+\.\d+)\s+([\d\.]+%)\s+([\d\.]+%)\s+([\d\.]+%)(?:\s|$)"
            matches = re.findall(table_pattern, normalized_text)
            
            if not matches:
                raise ValueError("未匹配到利率概率表数据")

            records = []
            fetch_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            
            for m in matches:
                records.append({
                    "抓取日期": fetch_date,
                    "会议日期": meeting_date,
                    "目标利率区间": m[0],
                    "当前概率": m[1],
                    "前一日概率": m[2],
                    "前一周概率": m[3]
                })
            
            print(f"✅ [{name}] 抓取成功! 会议: {meeting_date}, 获得 {len(records)} 个区间数据")
            return name, records, None

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(2)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return name, [], last_error

def fetch_investing_source(name, url, chrome_options):
    """
    通用 Investing.com 表格抓取
    """
    max_retries = 5
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium - Investing专线)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })

            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                pass
            
            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            if not dfs:
                raise ValueError("页面解析为空，未找到表格数据")

            target_df = None
            for df in dfs:
                cols = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                if all(k in cols for k in ['日期', '收盘', '交易量']):
                    df.columns = cols 
                    target_df = df
                    break
            
            if target_df is None:
                for df in dfs:
                    cols = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                    if '日期' in cols and '收盘' in cols:
                        df.columns = cols
                        target_df = df
                        break

            if target_df is None:
                    raise ValueError(f"未找到符合 Investing 格式的表格")

            df = target_df.copy()
            df['_std_date'] = df['日期'].apply(selenium_utils.clean_investing_date)
            df = df.dropna(subset=['_std_date'])
            df['_std_date'] = pd.to_datetime(df['_std_date'])
            
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=180)
            df = df[df['_std_date'] >= cutoff_date]
            df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
            
            rename_map = {
                '日期': '日期', '收盘': 'close', '开盘': 'open',
                '高': 'high', '低': 'low', '交易量': 'volume', '涨跌幅': 'change_pct'
            }
            available_map = {k: v for k, v in rename_map.items() if k in df.columns}
            df = df.rename(columns=available_map)
            
            if 'volume' in df.columns:
                df['volume'] = df['volume'].apply(selenium_utils.parse_volume)
            for col in ['close', 'open', 'high', 'low']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'change_pct' in df.columns:
                df['change_pct'] = df['change_pct'].apply(selenium_utils.parse_percentage)

            keep_cols = ['_std_date'] + list(available_map.values())
            keep_cols = list(dict.fromkeys(keep_cols))
            final_cols = [c for c in keep_cols if c in df.columns]
            
            df = df[final_cols]
            df.rename(columns={'_std_date': '日期'}, inplace=True)
            
            records = df.to_dict('records')
            print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
            return name, records, None 

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {str(e)[:100]}")
            if attempt < max_retries:
                time.sleep(2)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return name, [], last_error

def fetch_generic_source(name, url, chrome_options, days_to_keep=180):
    """
    通用数据源抓取 (Eastmoney 等)
    """
    max_retries = 5
    last_error = None

    for attempt in range(1, max_retries + 1):
        print(f"🌍 [{name}] 第 {attempt}/{max_retries} 次尝试 (Selenium)...")
        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"""
            })
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.get(url)
            
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except Exception:
                pass
            
            html = driver.page_source
            dfs = pd.read_html(StringIO(html))
            
            if not dfs:
                raise ValueError("页面解析为空，未找到表格数据")

            target_df = None
            for df in dfs:
                df.columns = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
                possible_date_cols = ['月份', '时间', '日期', '发布日期', '公布日期']
                if any(x in str(col) for x in df.columns for col in possible_date_cols):
                    if target_df is None or len(df) > len(target_df):
                        target_df = df
            
            if target_df is None:
                target_df = max(dfs, key=lambda x: len(x))

            df = target_df
            
            if isinstance(df.columns, pd.MultiIndex):
                new_cols = []
                for col in df.columns:
                    valid_parts = [str(c) for c in col if "Unnamed" not in str(c) and str(c).strip() != ""]
                    seen = set()
                    unique_parts = [x for x in valid_parts if not (x in seen or seen.add(x))]
                    new_cols.append("".join(unique_parts))
                df.columns = new_cols
            
            df.columns = [str(c).replace(" ", "").replace("\n", "").strip() for c in df.columns]
            possible_date_cols = ['月份', '时间', '日期', '发布日期', '公布日期']
            date_col = next((col for col in df.columns if any(x in str(col) for x in possible_date_cols)), None)
            
            if date_col:
                df['_std_date'] = df[date_col].apply(selenium_utils.clean_date)
                df = df.dropna(subset=['_std_date'])
                df['_std_date'] = pd.to_datetime(df['_std_date'])
                
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_to_keep)
                df = df[df['_std_date'] >= cutoff_date]
                
                df['_std_date'] = df['_std_date'].dt.strftime('%Y-%m-%d')
                df = df.replace({'-': None, 'nan': None})
                
                # 防御性逻辑
                if name == "中国_南向资金":
                    df = df.where(pd.notnull(df), None)
                    keep_cols = ['_std_date']
                    for c in df.columns:
                        if "净买额" in c and "当日" in c:
                            keep_cols.append(c)
                        elif "成交笔数" in c:
                            keep_cols.append(c)
                    df = df[keep_cols]
                    df.rename(columns={'_std_date': '日期'}, inplace=True)
                else:
                    df = df.where(pd.notnull(df), None)
                    if '日期' not in df.columns and '_std_date' in df.columns:
                        df['日期'] = df['_std_date']

                records = df.to_dict('records')
                print(f"✅ [{name}] 抓取成功! 获得 {len(records)} 条记录")
                return name, records, None
            else:
                raise ValueError(f"未找到日期列: {df.columns.tolist()}")

        except Exception as e:
            last_error = str(e)
            print(f"❌ [{name}] 失败: {last_error[:200]}") 
            if attempt < max_retries:
                time.sleep(2)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    return name, [], last_error