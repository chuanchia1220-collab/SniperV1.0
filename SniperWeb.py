import os
# 關閉 Streamlit 的檔案監視功能，避免觸發 inotify limit 報錯
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"
import streamlit as st
import pandas as pd
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
import time, os, twstock, json, threading, sqlite3, concurrent.futures, requests, queue
from itertools import cycle
import warnings
import logging
import numpy as np
import math
from bs4 import BeautifulSoup
import traceback
import platform
import pandas_ta as ta
from collections import deque
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# [系統除錯機制] 全域日誌
# ==========================================
sys_debug_logs = deque(maxlen=50)

def log_debug(msg):
    ts = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%H:%M:%S')
    full_msg = f"[{ts}] {msg}"
    print(full_msg)
    sys_debug_logs.appendleft(full_msg)

# ==========================================
# [雲端專屬防護] 最大化系統連線上限
# ==========================================
if platform.system() != "Windows":
    try:
        import resource
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft_limit < hard_limit:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))
            log_debug(f"✅ System FD limit raised to {hard_limit}")
    except Exception as e:
        log_debug(f"⚠️ System FD limit adjust failed: {e}")

try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class MockColor:
        def __getattr__(self, name): return ""
    Fore = Style = MockColor()
    colorama = None

warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 0. Global Strategy Config
# ==========================================
MIN_AMPLITUDE_THRESHOLD = 4.0 

MAD_DOG_PARAMS = {
    'TARGET_WAVE': 2,            
    'MAX_TRADES_DAY': 2,
    'ENTRY_CEILING_PCT': 4.0,   
    'STOP_LOSS_PCT': 2.0,
    'MAX_DAY_PCT': 3.5          
}

NORMAL_PARAMS = {
    'TARGET_WAVE': 0,            
    'MAX_TRADES_DAY': 2,
    'ENTRY_CEILING_PCT': 2.0,   
    'STOP_LOSS_PCT': 0.0,
    'MAX_DAY_PCT': 2.5          
}

ENTRY_THRESHOLD_PCT = 0.5   
TRIGGER_PCT = 1.5
TRAILING_CALLBACK = 1.0
PERIOD = "60d"
INTERVAL = "5m"

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper AI Commander", page_icon="🤖", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v616.db"

DEFAULT_WATCHLIST = "3037 1513 4566"
DEFAULT_INVENTORY = """4566"""

@dataclass
class SniperEvent:
    code: str
    name: str
    scope: str
    event_kind: str
    event_label: str
    price: float
    pct: float
    vwap: float
    ratio: float
    ratio_yest: float
    net_10m: int
    net_1h: int
    net_day: int
    tp_price: float
    sl_price: float
    win_rate: float
    twii_slope: float = 0.0  
    rsi: float = 0.0
    band_ratio: float = 0.0
    b_percent: float = 0.0
    control_ratio: float = 0.0
    is_snapshot: bool = False
    timestamp: float = field(default_factory=time.time)
    data_status: str = "DATA_OK"
    is_test: bool = False

# ==========================================
# 3. Database
# ==========================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.write_queue = queue.Queue()
        self.gs_sheet = None
        self._init_db()
        threading.Thread(target=self._writer_loop, daemon=True, name="DBWriterThread").start()
        threading.Thread(target=self._init_google_sheets, daemon=True).start()

    def _init_google_sheets(self):
        # 如果已經連線成功，就不要再跑一次
        if hasattr(self, 'gs_client') and self.gs_client:
            return
        try:
            from google.oauth2.service_account import Credentials # 確保導入
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            
            # --- 優先嘗試從 Streamlit Secrets 讀取 (雲端環境) ---
            if "gcp_service_account" in st.secrets:
                creds_info = dict(st.secrets["gcp_service_account"])
                # --- 自動修復 PEM 格式中的換行問題 ---
                if "private_key" in creds_info:
                    creds_info["private_key"] = creds_info["private_key"].replace("\\n", "\n")
                
                creds = Credentials.from_service_account_info(creds_info, scopes=scope)
                self.gs_client = gspread.authorize(creds)
                log_debug("☁️ Google Sheets 連線成功 (格式已自動校準)")
            
            # --- 次要嘗試從本地檔案讀取 (本地測試環境) ---
            elif os.path.exists("service_account.json"):
                from oauth2client.service_account import ServiceAccountCredentials # 確保本地相容導入
                creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
                self.gs_client = gspread.authorize(creds)
                log_debug("☁️ Google Sheets 連線成功 (透過本地 JSON)")
            
            else:
                log_debug("⚠️ 找不到憑證資訊，雲端同步關閉")
                return

            # 開啟試算表
            self.gs_sheet = self.gs_client.open_by_url("https://docs.google.com/spreadsheets/d/1cmB7osByPJeSA7Zz71K21xM5L2X1xMaHAYeEcYkVZfU/edit?usp=sharing").sheet1
            
        except Exception as e:
            log_debug(f"⚠️ Google Sheets 初始化失敗: {e}")

    def _get_conn(self):
        for _ in range(3):
            try: return sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
            except sqlite3.OperationalError: time.sleep(1)
        return sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)

    def _init_db(self):
        try:
            conn = self._get_conn(); c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, signal TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL', situation TEXT, ratio_yest REAL, active_light INTEGER DEFAULT 0, rsi REAL DEFAULT 0, band_ratio REAL DEFAULT 0, b_percent REAL DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
            c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
            c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, vol_5ma REAL, vol_yest REAL, price_5ma REAL, win_rate REAL DEFAULT 0, avg_ret REAL DEFAULT 0, avg_amp REAL DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS telegram_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, log_time TEXT, code TEXT, name TEXT, signal TEXT, price REAL, vwap REAL, net_10m INTEGER, net_1h INTEGER, net_day INTEGER, twii_slope REAL, rsi REAL, band_ratio REAL, b_percent REAL, ratio REAL, control_ratio REAL)''')

            try: c.execute("ALTER TABLE static_info ADD COLUMN avg_amp REAL DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE telegram_logs ADD COLUMN ratio REAL DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE telegram_logs ADD COLUMN control_ratio REAL DEFAULT 0")
            except: pass
            
            conn.commit(); conn.close()
        except Exception as e: log_debug(f"⚠️ [DB Init Error] {e}")

    def log_telegram(self, event: SniperEvent):
        time_str = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        sql = 'INSERT INTO telegram_logs (log_time, code, name, signal, price, vwap, net_10m, net_1h, net_day, twii_slope, rsi, band_ratio, b_percent, ratio, control_ratio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        args = (time_str, event.code, event.name, event.event_label, event.price, event.vwap,
                event.net_10m, event.net_1h, event.net_day, event.twii_slope,
                event.rsi, event.band_ratio, event.b_percent, event.ratio, event.control_ratio)
        self.write_queue.put(('execute', sql, args))

        if self.gs_sheet:
            threading.Thread(target=lambda: self._safe_gs_write(args), daemon=True).start()

    def _safe_gs_write(self, row):
        try: self.gs_sheet.append_row(list(row))
        except: pass

    def get_telegram_logs(self):
        try:
            conn = self._get_conn()
            df = pd.read_sql('SELECT log_time as 時間, code as 代碼, name as 名稱, signal as 訊號, price as 價格, vwap as 均價, net_10m as 大戶10M, net_1h as 大戶1H, net_day as 大戶日, ratio as 量比, control_ratio as 控盤比, twii_slope as 大盤斜率, rsi as RSI, band_ratio as 帶寬比, b_percent as 布林極限 FROM telegram_logs ORDER BY id ASC', conn)
            conn.close(); return df
        except: return pd.DataFrame()

    def get_watchlist_view(self):
        try:
            conn = self._get_conn()
            query = '''SELECT w.code, r.name, r.vol, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_10m, r.net_1h, r.net_day, r.situation, r.active_light, r.rsi, r.band_ratio, r.b_percent, s.price_5ma, s.win_rate, s.avg_ret, s.avg_amp, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
            df = pd.read_sql(query, conn); conn.close(); return df
        except: return pd.DataFrame()

    def get_inventory_view(self):
        try:
            conn = self._get_conn()
            query = '''SELECT i.code, r.name, r.vol, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_1h, r.net_day, r.situation, s.price_5ma, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
            df = pd.read_sql(query, conn); conn.close(); return df
        except: return pd.DataFrame()

    def _writer_loop(self):
        conn = self._get_conn(); cursor = conn.cursor()
        while True:
            try:
                task = self.write_queue.get(timeout=5.0)
                task_type, sql, args = task
                if task_type == 'executemany': cursor.executemany(sql, args)
                else: cursor.execute(sql, args)
                conn.commit()
            except queue.Empty: continue
            except Exception as e: log_debug(f"⚠️ DB Write Error: {e}")

    def get_volume_map(self):
        try:
            conn = self._get_conn(); c = conn.cursor()
            c.execute('SELECT code, vol_5ma, vol_yest, price_5ma, win_rate, avg_amp FROM static_info')
            rows = c.fetchall(); conn.close()
            return {r[0]: {'vol_5ma': r[1], 'vol_yest': r[2], 'price_5ma': r[3], 'win_rate': r[4], 'avg_amp': r[5]} for r in rows}
        except: return {}

    def get_all_codes(self):
        try:
            conn = self._get_conn(); c = conn.cursor()
            c.execute('SELECT code FROM inventory UNION SELECT code FROM watchlist UNION SELECT code FROM pinned')
            rows = c.fetchall(); conn.close(); return [r[0] for r in rows]
        except: return []

    def get_inventory_codes(self):
        try:
            conn = self._get_conn(); c = conn.cursor()
            c.execute('SELECT code FROM inventory')
            rows = c.fetchall(); conn.close(); return [r[0] for r in rows]
        except: return []

    def get_pinned_codes(self):
        try:
            conn = self._get_conn(); c = conn.cursor()
            c.execute('SELECT code FROM pinned')
            rows = c.fetchall(); conn.close(); return [r[0] for r in rows]
        except: return []

    def update_watchlist(self, codes_text):
        self.write_queue.put(('execute', 'DELETE FROM watchlist', ()))
        targets = [t.strip() for t in codes_text.split() if t.strip()]
        for t in targets: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO watchlist (code) VALUES (?)', (t,)))
        return targets

    def update_inventory_list(self, inventory_text):
        self.write_queue.put(('execute', 'DELETE FROM inventory', ()))
        raw_items = inventory_text.replace('\n', ' ').replace(',', ' ').split()
        for code in raw_items:
            if code.strip(): self.write_queue.put(('execute', 'INSERT OR REPLACE INTO inventory (code, cost, qty) VALUES (?, 0.0, 1.0)', (code.strip(),)))

    def upsert_realtime_batch(self, data_list):
        if not data_list: return
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, est_vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m, situation, ratio_yest, active_light, rsi, band_ratio, b_percent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, vol_5ma, vol_yest, price_5ma, win_rate, avg_ret, avg_amp) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))


db = Database(DB_PATH)

# ==========================================
# 2. Market Session (修正假日判斷)
# ==========================================
class MarketSession:
    MARKET_OPEN, MARKET_CLOSE = dt_time(9, 0), dt_time(13, 35)
    @staticmethod
    def is_market_open(now=None):
        if not now: now = datetime.now(timezone.utc) + timedelta(hours=8)
        # 嚴格判斷：必須是週一到週五 (weekday 0~4)
        return now.weekday() < 5 and MarketSession.MARKET_OPEN <= now.time() <= MarketSession.MARKET_CLOSE

# ==========================================
# 4. Utilities
# ==========================================
def adjust_to_tick(price, method='floor'):
    if price < 10: tick = 0.01
    elif price < 50: tick = 0.05
    elif price < 100: tick = 0.1
    elif price < 500: tick = 0.5
    elif price < 1000: tick = 1.0
    else: tick = 5.0
    
    if method == 'round': return round(price / tick) * tick
    else: return math.floor(price / tick) * tick

def _calculate_intraday_vwap(df):
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert('Asia/Taipei')
    else: df.index = df.index.tz_convert('Asia/Taipei')

    df['Date'] = df.index.date
    df['TP_Vol'] = df['Close'] * df['Volume']
    df['Cum_Vol'] = df.groupby('Date')['Volume'].cumsum()
    df['Cum_Val'] = df.groupby('Date')['TP_Vol'].cumsum()
    df['VWAP'] = df['Cum_Val'] / df['Cum_Vol']
    return df

def _run_core_backtest(df, params):
    results = []
    wins, losses = 0, 0
    unique_dates = sorted(list(set(df.index.date)))

    ENTRY_THRESHOLD = 1 + (ENTRY_THRESHOLD_PCT / 100)
    ENTRY_CEILING = 1 + (params['ENTRY_CEILING_PCT'] / 100)
    TRIGGER = 1 + (TRIGGER_PCT / 100)
    CALLBACK = TRAILING_CALLBACK / 100
    STOP_LOSS = 1 - (params['STOP_LOSS_PCT'] / 100)

    for trade_date in unique_dates:
        mask = df['Date'] == trade_date
        day_data = df.loc[mask]
        if day_data.empty: continue

        in_pos = False
        entry_p = 0; entry_v = 0
        daily_executed_trades = 0
        wave_count = 0; last_wave_ts = 0; is_holding_wave = False
        
        try: yesterday_close = day_data['Close'].iloc[0] 
        except: continue
        max_p_after_entry = 0; trailing_active = False

        for ts, row in day_data.iterrows():
            p = float(row['Close']); v = float(row['VWAP']); t = ts.time(); curr_ts = ts.timestamp()
            if daily_executed_trades >= params['MAX_TRADES_DAY']: break

            entry_line = v * ENTRY_THRESHOLD
            if p >= entry_line:
                if not is_holding_wave:
                    if (curr_ts - last_wave_ts) > 300:
                        wave_count += 1
                        last_wave_ts = curr_ts
                is_holding_wave = True
            else:
                is_holding_wave = False

            if not in_pos:
                if t.hour == 9 and t.minute < 10: continue
                if t.hour >= 13: continue
                
                if (v * ENTRY_THRESHOLD) < p < (v * ENTRY_CEILING):
                    day_pct = (p - yesterday_close) / yesterday_close * 100
                    if day_pct <= params['MAX_DAY_PCT']:
                        should_trade = False
                        if params['TARGET_WAVE'] == 0: should_trade = True
                        elif wave_count == params['TARGET_WAVE']: should_trade = True
                        
                        if should_trade:
                            in_pos = True
                            entry_p = p; entry_v = v
                            daily_executed_trades += 1
                            max_p_after_entry = p
                            trailing_active = False

            elif in_pos:
                exit_price = 0
                if p <= entry_v * STOP_LOSS: exit_price = p
                
                if p > max_p_after_entry: max_p_after_entry = p
                if p >= entry_p * TRIGGER: trailing_active = True
                
                if trailing_active and exit_price == 0:
                    dynamic_exit = max_p_after_entry * (1 - CALLBACK)
                    if p <= dynamic_exit: exit_price = dynamic_exit

                if t.hour == 13 and t.minute >= 25 and exit_price == 0: exit_price = p
                
                if exit_price > 0:
                    ret = (exit_price - entry_p) / entry_p * 100
                    results.append(ret / 100)
                    if ret > 0: wins += 1
                    else: losses += 1
                    in_pos = False

    return results, wins, losses

def _run_quick_backtest(target_code):
    try:
        if target_code.isdigit(): target_code += ".TW"
        
        df_daily = yf.download(target_code, period=PERIOD, interval="1d", progress=False, auto_adjust=True)
        if isinstance(df_daily.columns, pd.MultiIndex): df_daily.columns = df_daily.columns.get_level_values(0)
        df_daily['Prev_Close'] = df_daily['Close'].shift(1)
        df_daily['Amplitude'] = (df_daily['High'] - df_daily['Low']) / df_daily['Prev_Close'] * 100
        avg_amp_5d = df_daily['Amplitude'].dropna().tail(5).mean()
        if np.isnan(avg_amp_5d): avg_amp_5d = 0
        
        df = yf.download(target_code, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=True)
        if df.empty:
            target_code = target_code.replace(".TW", ".TWO")
            df = yf.download(target_code, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=True)
        if df.empty or len(df) < 50: return 0, 0, avg_amp_5d

        if avg_amp_5d >= MIN_AMPLITUDE_THRESHOLD: PARAMS = MAD_DOG_PARAMS
        else: PARAMS = NORMAL_PARAMS

        df = _calculate_intraday_vwap(df).dropna()
        results, wins, losses = _run_core_backtest(df, PARAMS)

        total = wins + losses
        win_rate = (wins/total*100) if total > 0 else 0
        avg_ret = np.mean(results) * 100 if results else 0
        
        return win_rate, avg_ret, avg_amp_5d
    except: return 0, 0, 0

def fetch_static_stats(client, code):
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        
        hist_10d = yf.Ticker(f"{code}{suffix}").history(period="10d", auto_adjust=True)
        vol_5ma, vol_yest, price_5ma = 0, 0, 0
        if not hist_10d.empty and len(hist_10d) >= 5:
            vol_yest = int(hist_10d['Volume'].iloc[-2]) // 1000
            last_5_days = hist_10d.iloc[-6:-1]
            if last_5_days.empty: last_5_days = hist_10d.tail(5)
            vol_5ma = int(last_5_days['Volume'].mean()) // 1000
            price_5ma = float(last_5_days['Close'].mean())

        win_rate, avg_ret, avg_amp = _run_quick_backtest(code)
        return vol_5ma, vol_yest, price_5ma, win_rate, avg_ret, avg_amp
    except: 
        return 0, 0, 0, 0, 0, 0

def get_stock_name(symbol):
    try: return twstock.codes[symbol].name if symbol in twstock.codes else symbol
    except: return symbol

def get_dynamic_thresholds(price):
    if price >= 1000: return {"tgt_pct": 1.5, "tgt_ratio": 1.2, "ambush": 1.5, "overheat": 4.0}
    elif price >= 500: return {"tgt_pct": 2.0, "tgt_ratio": 1.5, "ambush": 2.5, "overheat": 5.0}
    elif price >= 150: return {"tgt_pct": 2.5, "tgt_ratio": 1.8, "ambush": 4.0, "overheat": 6.5}
    elif price >= 70: return {"tgt_pct": 3.0, "tgt_ratio": 2.2, "ambush": 6.0, "overheat": 8.0}
    else: return {"tgt_pct": 5.0, "tgt_ratio": 3.0, "ambush": 10.0, "overheat": 9.0}

def _calc_est_vol(current_vol):
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    if now.weekday() >= 5: return current_vol
    
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < market_open: return 0
    elapsed_minutes = (now - market_open).seconds / 60
    if elapsed_minutes <= 0: return 0
    if elapsed_minutes >= 270: return current_vol
    weight = 2.0 if elapsed_minutes < 15 else 1.0
    return int(current_vol * (270 / elapsed_minutes) / weight)

def check_signal(pct, is_bullish, net_day, net_1h, ratio, thresholds, is_breakdown, price, vwap, has_attacked, now_time, vol_lots, twii_slope=0.0):
    if is_breakdown: return "🚨撤退"
    if pct >= 9.30: return "👑漲停"
    if now_time.time() < dt_time(9, 5): return "⏳暖機"

    # --- 新增防禦機制 ---
    if twii_slope < -10.0: return "⚖️觀望(盤勢險峻)"
    if pct < 0 and twii_slope < 5.0: return "👀跌深反彈"
    # -------------------

    in_golden_zone = False
    if is_bullish and price <= (vwap * 1.006):
        in_golden_zone = True

    if ratio >= thresholds['tgt_ratio']:
        if in_golden_zone and net_1h > 0: return "🔥攻擊"
        elif net_1h < 0: return "💀出貨"
        
    if not is_bullish: return "📉線下"
    if is_bullish and not in_golden_zone: return "⚠️追高"

    bias = ((price - vwap) / vwap) * 100 if vwap > 0 else 0
    if bias > thresholds['overheat']: return "⚠️過熱"

    if dt_time(13, 0) <= now_time.time() <= dt_time(13, 25):
        if (3.0 <= pct <= 9.0) and (net_1h > 0) and (net_day / (vol_lots+1) >= 0.05): return "🔥尾盤"
        
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if pct >= thresholds['tgt_pct']: return "⚠️價強"

    return "盤整"

# ==========================================
# 5. GPT Advisor
# ==========================================
class GPTAdvisor:
    def __init__(self, api_key):
        self.api_key = api_key
    
    def analyze_signal(self, event: SniperEvent):
        if not self.api_key: return "⚠️ 未設定 OpenAI Key"
        
        try:
            code = event.code
            suffix = ".TW"
            if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
            full_code = f"{code}{suffix}"
            
            df_intra = yf.download(full_code, period="1d", interval="5m", progress=False, auto_adjust=True)
            intra_str = "No Data"
            if not df_intra.empty:
                tail_data = df_intra.tail(12) 
                intra_str = tail_data[['Close', 'Volume']].to_string(header=False)
            
            df_daily = yf.download(full_code, period="5d", progress=False, auto_adjust=True)
            daily_str = "No Data"
            if not df_daily.empty:
                if isinstance(df_daily.columns, pd.MultiIndex): df_daily.columns = df_daily.columns.get_level_values(0)
                daily_str = df_daily[['Close', 'Volume']].to_string()

            inst_str = "No Data"
            try:
                stock = twstock.Stock(code)
                inst_data = stock.institutional_investors 
                if inst_data:
                    recent_inst = inst_data[-3:] 
                    inst_str = "\n".join([f"{d['date']}: Foreign:{d['foreign_diff']} Investment:{d['investment_trust_diff']} Dealer:{d['dealer_diff']}" for d in recent_inst])
            except: pass

            news_str = "No News"
            try:
                ticker = yf.Ticker(full_code)
                news = ticker.news
                if news:
                    news_titles = [n.get('title', '') for n in news[:3]]
                    news_str = "\n".join(news_titles)
            except: pass
            
            prompt = f"""
You are a top-tier day trading commander. Analyze the following data for stock {code} ({event.name}).
Current Signal: {event.event_label} at Price {event.price}.

[Advanced Technical Indicators]
- RSI (14-day): {event.rsi} (Reference: >60 Bullish, >80 Overbought)
- Bollinger Band Ratio: {event.band_ratio} (Reference: >1.0 indicates expanding bands/increasing volatility)
- Bollinger %b: {event.b_percent} (Reference: 1.0 means price is at the upper band, >1.0 is a breakout)

[Context Data]
1. Recent 5 Days (Daily):
{daily_str}

2. Intraday Trend (Last 1 Hour 5m Bars):
{intra_str}

3. Institutional Investors (Foreign/Investment Trust/Dealer) Net Buy/Sell (Last 3 Days):
{inst_str}

4. Recent News Headlines:
{news_str}

[Task]
Based on the signal, price action, chip flow, and the Advanced Technical Indicators provided, determine the immediate operation:
- ACTION: [BUY / SELL / WAIT]
- REASON: (Short explanation under 50 words focusing on chips, trend, and the new indicators)
- WARNING: (Any risk factor)

Reply in Traditional Chinese (繁體中文). Be concise and military style.
"""
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            payload = {
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 150,
                "temperature": 0.5
            }
            
            resp = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()['choices'][0]['message']['content']
                return result
            else:
                return f"GPT Error: {resp.status_code}"
        except Exception as e:
            return f"GPT Analysis Failed: {str(e)}"

# ==========================================
# 6. Notification
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    RATE_LIMIT_DELAY = 1.0
    EMOJI_MAP = {
        "🔥攻擊": "🚀", "💣伏擊": "💣", "👀量增": "👀",
        "💀出貨": "💀", "🚨撤退": "⚠️", "👑漲停": "👑",
        "⚠️價強": "💪", "❌誘多": "🎣", "🔥尾盤": "🔥",
        "⚠️過熱": "🚫", "⏳暖機": "⏳", "📉線下": "📉", "⚠️追高": "🚫",
    }

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        self.gpt_advisor = GPTAdvisor(OPENAI_API_KEY)
        threading.Thread(target=self._worker_loop, daemon=True, name="NotificationThread").start()

    def reset_daily_state(self):
        self._cooldowns.clear()

    def should_notify(self, event: SniperEvent) -> bool:
        if event.is_test: return True
        if not MarketSession.is_market_open(): return False

        # --- 【新增：09:25 靜音閘門】 ---
        # 取得當前台北時間 (HHMM 格式)
        now_hhmm = int((datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%H%M'))
        if now_hhmm < 925:
            # 雖然不推播，但因為這是在 should_notify 內判斷
            # 只要 fetch_stock 裡有呼叫 db.log_telegram，數據依然會進雲端！
            return False 
        # -----------------------------
        
        if event.scope == "watchlist" and event.event_label == "🔥攻擊":
            if event.win_rate < 50: return False

        if event.event_label == "⚠️追高": return False

        key = f"{event.code}_{event.scope}_{event.event_label}"
        
        if event.scope == "inventory":
            if event.event_label not in ["💀出貨", "🚨撤退", "🔥攻擊", "👑漲停"]: return False
            if "撤退" in event.event_label:
                 if time.time() - self._cooldowns.get(key, 0) < 300: return False
                 return True

        if event.scope == "watchlist":
            if event.event_label != "🔥攻擊": return False

        if time.time() - self._cooldowns.get(key, 0) < self.COOLDOWN_SECONDS: return False
        return True

    def enqueue(self, event: SniperEvent):
        if self.should_notify(event):
            if not event.is_test: self._cooldowns[f"{event.code}_{event.scope}_{event.event_label}"] = time.time()
            self._queue.put(event)

    def _worker_loop(self):
        while True:
            event = self._queue.get()
            try:
                self._send_telegram(event)
                if OPENAI_API_KEY and event.event_label in ["🔥攻擊", "🔥尾盤", "💀出貨"]:
                    gpt_analysis = self.gpt_advisor.analyze_signal(event)
                    self._send_telegram_gpt(event.code, gpt_analysis)
                time.sleep(self.RATE_LIMIT_DELAY)
            except: pass
            finally: self._queue.task_done()

    def _send_telegram(self, event: SniperEvent):
        if not TG_BOT_TOKEN or not TG_CHAT_ID: return
        emoji = self.EMOJI_MAP.get(event.event_label, "📌")
        up_dn = "UP" if event.pct >= 0 else "DN"
        
        msg = (f"<b>{emoji} {event.event_label}｜{event.code} {event.name} ({event.win_rate:.0f}%)</b>\n"
               f"現價：{event.price:.2f} ({event.pct:.2f}% {up_dn})　均價：{event.vwap:.2f}\n"
               f"<b>🎯 止盈：{event.tp_price:.1f} (2%)｜🛡️ 止損：{event.sl_price:.1f} (均-1.5%)</b>\n"
               f"📊 量比：{event.ratio_yest:.1f} / <b>{event.ratio:.1f}</b>\n"
               f"💰 大戶：{event.net_10m} / <b>{event.net_1h}</b> / {event.net_day}")
               
        buttons = [[{"text": "📈 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{event.code}.TW"}]]
        try: 
            resp = requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
            if resp.status_code == 200:
                db.log_telegram(event)
        except: pass

    def _send_telegram_gpt(self, code, analysis_text):
        if not TG_BOT_TOKEN or not TG_CHAT_ID: return
        msg = f"<b>🤖 AI 戰略顧問｜{code}</b>\n\n{analysis_text}"
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 7. Engine (Sniper Core - Cloud Hardened)
# ==========================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS] if API_KEYS else []
        self.client_cycle = cycle(self.clients) if self.clients else None
        self.targets = []
        self.inventory_codes = []
        self.base_vol_cache = {} 
        self.daily_net = {}
        self.vol_queues = {}
        self.prev_data = {}
        self.active_flags = {}
        self.daily_risk_flags = {}
        self.wave_tracker = {}
        self.adv_indicator_cache = {} 
        
        self.market_stats = {"Time": 0, "Price5MA": 0, "Slope5Min": 0.0, "PriceHistory": []} 
        self.twii_data = None 
        self.last_reset = datetime.now().date()
        self.last_snapshot_ts = 0
        
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        self._init_market_stats()

    def update_targets(self):
        self.targets = db.get_all_codes()
        self.inventory_codes = db.get_inventory_codes()
        new_data = db.get_volume_map()
        if new_data:
            self.base_vol_cache.update(new_data)

    def start(self):
        if self.running: return
        self.update_targets()
        self.running = True
        thread = threading.Thread(target=self._run_loop, daemon=True, name="SniperCoreThread")
        thread.start()

    def stop(self): self.running = False
    
    def _init_market_stats(self):
        try:
             tse = yf.Ticker("^TWII")
             hist = tse.history(period="10d", auto_adjust=True)
             if not hist.empty: self.market_stats["Price5MA"] = hist['Close'].iloc[-6:-1].mean() if len(hist) >= 6 else hist['Close'].mean()
             else: self.market_stats["Price5MA"] = 0
        except: pass

    def _update_market_thermometer(self):
        current_ts = time.time()
        if current_ts - self.market_stats.get("Time", 0) < 15: return
        
        current_price = 0; current_pct = 0
        now_str = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%H:%M:%S')
        source_status = f"{now_str}"

        try:
            url = "https://tw.stock.yahoo.com/quote/^TWII"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
            r = requests.get(url, headers=headers, timeout=3)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                price_elem = soup.find('span', class_='Fz(32px)')
                if price_elem:
                    current_price = float(price_elem.text.replace(',', ''))
                    pct_elem = None
                    for span in soup.find_all('span'):
                        if span.text and '%' in span.text and ('+' in span.text or '-' in span.text or '0.00%' in span.text):
                             if len(span.text) < 15: 
                                 pct_elem = span; break
                    if pct_elem: current_pct = float(pct_elem.text.replace('%', '').replace('+', ''))
        except: pass

        if not current_price:
            try:
                tse = yf.Ticker("^TWII"); fi = tse.fast_info
                current_price = fi.last_price; prev = fi.previous_close
                if current_price and prev:
                    current_pct = ((current_price - prev) / prev) * 100; source_status = f"{now_str} (Backup API)"
            except: pass

        if current_price:
            if "PriceHistory" not in self.market_stats:
                self.market_stats["PriceHistory"] = []
            
            self.market_stats["PriceHistory"].append((current_ts, current_price))
            self.market_stats["PriceHistory"] = [x for x in self.market_stats["PriceHistory"] if current_ts - x[0] <= 600]
            
            past_price = current_price
            for ts, p in self.market_stats["PriceHistory"]:
                if current_ts - ts <= 300:
                    past_price = p
                    break
            
            slope_5min = float(current_price - past_price)
            self.market_stats["Slope5Min"] = slope_5min
            price_5ma = self.market_stats.get("Price5MA", current_price)
            
            # 👇 補足 vol 欄位以供儀表板運算
            self.twii_data = {
                'code': '0000', 'name': f'加權指數 {source_status}', 
                'vol': 0,
                'price': current_price, 'pct': current_pct, 'vwap': current_price, 
                'price_5ma': price_5ma, 'ratio': 1.0, 'ratio_yest': 1.0,
                'net_10m': 0, 'net_1h': 0, 'net_day': 0,
                'situation': '市場指標', 'event_label': '大盤',
                'is_pinned': 1, 'win_rate': 0, 'avg_ret': 0, 'avg_amp': 0, 'active_light': 1,
                'twii_slope': slope_5min,
                'rsi': 0.0, 'band_ratio': 0.0, 'b_percent': 0.0
            }
            self.market_stats["Time"] = current_ts

    def _dispatch_event(self, ev: SniperEvent):
        notification_manager.enqueue(ev)

    def _calculate_advanced_indicators(self, code):
        try:
            # --- [機制 1] 隨機節流：避免多線程同時向 Yahoo 擊發 ---
            import random
            time.sleep(random.uniform(0.5, 3.0))
            
            # --- [機制 2] 延長快取：整備月指標每 30 分鐘更新一次即可 (節省 80% 流量) ---
            now_ts = time.time()
            if code in self.adv_indicator_cache:
                # 判斷快取時間 (600 秒 = 10 分鐘)
                if (now_ts - self.adv_indicator_cache[code].get('time', 0)) < 600:
                    c = self.adv_indicator_cache[code]
                    return c['rsi'], c['br'], c['bp']

            # --- [機制 3] 標記故障：針對經常喷錯的標的進行隔離 ---
            if not hasattr(self, 'indicator_fail_count'): self.indicator_fail_count = {}
            if self.indicator_fail_count.get(code, 0) >= 3:
                return 0.0, 0.0, 0.0

            suffix = ".TW"
            if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
            full_code = f"{code}{suffix}"
            
            # 抓取資料 (縮短 period 到 1mo 減少資料傳輸量，計算 RSI 14 仍夠用)
            ticker = yf.Ticker(full_code)
            df = ticker.history(period="3mo", interval="1d", auto_adjust=True)
            
            if df.empty or len(df) < 25: 
                log_debug(f"⚠️ [{code}] 指標失敗：歷史資料不足")
                return 0.0, 0.0, 0.0
                
            df['Close'] = pd.to_numeric(df['Close'], errors='coerce')

            # 技術指標計算
            rsi_series = ta.rsi(df['Close'], length=14)
            df['RSI_14'] = rsi_series if rsi_series is not None else 0.0

            df['BBM_20_2.0'] = df['Close'].rolling(window=20).mean()
            std = df['Close'].rolling(window=20).std(ddof=0)
            df['BBU_20_2.0'] = df['BBM_20_2.0'] + (2 * std)
            df['BBL_20_2.0'] = df['BBM_20_2.0'] - (2 * std)
            df['BBB_20_2.0'] = (df['BBU_20_2.0'] - df['BBL_20_2.0']) / df['BBM_20_2.0']
            df['Band_Ratio'] = (df['BBB_20_2.0'] / df['BBB_20_2.0'].rolling(window=5).max().shift(1)).round(3)
            df['%b'] = ((df['Close'] - df['BBL_20_2.0']) / (df['BBU_20_2.0'] - df['BBL_20_2.0'])).round(3)
            
            latest = df.iloc[-1]
            rsi_val = float(latest.get('RSI_14', 0.0))
            band_ratio_val = float(latest.get('Band_Ratio', 0.0))
            b_percent_val = float(latest.get('%b', 0.0))

            if math.isnan(rsi_val): rsi_val = 0.0
            if math.isnan(band_ratio_val): band_ratio_val = 0.0
            if math.isnan(b_percent_val): b_percent_val = 0.0

            # 更新快取
            self.adv_indicator_cache[code] = {'time': now_ts, 'rsi': rsi_val, 'br': band_ratio_val, 'bp': b_percent_val}
            # 成功時歸零失敗計數
            self.indicator_fail_count[code] = 0

            log_debug(f"✅ [{code}] 指標更新成功 (Cache 啟動)")
            return round(rsi_val, 1), round(band_ratio_val, 2), round(b_percent_val, 2)

        except Exception as e:
            err_msg = str(e)
            if "Too Many Requests" in err_msg:
                self.indicator_fail_count[code] = self.indicator_fail_count.get(code, 0) + 1
                log_debug(f"🛑 [{code}] Yahoo 限制流量中，失敗次數: {self.indicator_fail_count[code]}")
            else:
                log_debug(f"💥 [{code}] 指標嚴重例外：{err_msg}")
            return 0.0, 0.0, 0.0

    def _fetch_stock(self, code, now_time=None, force_snapshot=False):
        try:
            if now_time is None: now_time = datetime.now(timezone.utc) + timedelta(hours=8)
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            static_data = self.base_vol_cache.get(code, {})
            base_vol_5ma = static_data.get('vol_5ma', 0)
            base_vol_yest = static_data.get('vol_yest', 0)
            price_5ma = static_data.get('price_5ma', 0)
            win_rate = static_data.get('win_rate', 0)
            avg_amp = static_data.get('avg_amp', 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            order_book = q.get('order', {})
            best_asks = order_book.get('bestAsks', []) or []; best_bids = order_book.get('bestBids', []) or []
            best_ask = best_asks[0].get('price') if best_asks else None
            best_bid = best_bids[0].get('price') if best_bids else None
            pct = q.get('changePercent', 0); vol_lots = q.get('total', {}).get('tradeVolume', 0)
            if vol_lots == 0 and code in self.prev_data: vol_lots = self.prev_data[code]['vol']

            vol = vol_lots * 1000; est_lots = _calc_est_vol(vol_lots)
            ratio_5ma = est_lots / base_vol_5ma if base_vol_5ma > 0 else 0
            ratio_yest = est_lots / base_vol_yest if base_vol_yest > 0 else 0
            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = (total_val / vol) if vol > 0 else price

            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']; delta_v = vol_lots - prev_v
                if delta_v > 0:
                    if best_ask and price >= best_ask: delta_net = int(delta_v)
                    elif best_bid and price <= best_bid: delta_net = -int(delta_v)
                    else:
                        prev_p = self.prev_data[code]['price']
                        if price > prev_p: delta_net = int(delta_v)
                        elif price < prev_p: delta_net = -int(delta_v)

            self.prev_data[code] = {'vol': vol_lots, 'price': price}
            now_ts = time.time()
            if code not in self.vol_queues: self.vol_queues[code] = []
            if delta_net != 0: self.vol_queues[code].append((now_ts, delta_net))

            self.daily_net[code] = self.daily_net.get(code, 0) + delta_net
            self.vol_queues[code] = [x for x in self.vol_queues[code] if x[0] > now_ts - 3600]
            
            net_1h = sum(x[1] for x in self.vol_queues[code])
            net_10m = sum(x[1] for x in self.vol_queues[code] if x[0] > now_ts - 600)
            net_day = self.daily_net.get(code, 0)

            is_bullish = price >= (vwap * 1.005)
            is_breakdown = price < (vwap * 0.99)
            
            situation = "⚖️觀望"
            if net_1h > 0: situation = "🔥主動吸籌" if is_bullish else "🛡️被動吃盤"
            elif net_1h < 0: situation = "💀主動倒貨" if not is_bullish else "🎣拉高出貨"

            if code not in self.wave_tracker:
                self.wave_tracker[code] = {'count': 0, 'last_trigger_ts': 0, 'is_holding': False}

            entry_threshold = vwap * 1.005
            tracker = self.wave_tracker[code]
            current_ts = now_ts

            if price >= entry_threshold:
                if not tracker['is_holding']:
                    if (current_ts - tracker['last_trigger_ts']) > 300:
                        tracker['count'] += 1
                        tracker['last_trigger_ts'] = current_ts
                tracker['is_holding'] = True
            else:
                tracker['is_holding'] = False
            
            if avg_amp >= MIN_AMPLITUDE_THRESHOLD: PARAMS = MAD_DOG_PARAMS
            else: PARAMS = NORMAL_PARAMS
            
            active_light = 0 
            time_ok = now_time.time() >= dt_time(9, 5)
            
            if PARAMS['TARGET_WAVE'] == 0: wave_ok = True
            else: wave_ok = (tracker['count'] == PARAMS['TARGET_WAVE'])
            
            pct_ok = pct <= PARAMS['MAX_DAY_PCT']
            ceiling_price = vwap * (1 + PARAMS['ENTRY_CEILING_PCT']/100)
            ceiling_ok = price <= ceiling_price
            threshold_ok = price >= entry_threshold

            if time_ok and wave_ok and pct_ok and ceiling_ok and threshold_ok:
                active_light = 1

            current_twii_slope = self.market_stats.get("Slope5Min", 0.0)
            thresholds = get_dynamic_thresholds(price)
            raw_state = check_signal(pct, is_bullish, net_day, net_1h, ratio_5ma, thresholds, is_breakdown, price, vwap, code in self.active_flags, now_time, vol_lots, twii_slope=current_twii_slope)

            event_label = None
            scope = "inventory" if code in self.inventory_codes else "watchlist"
            trigger_price = vwap * 1.005
            tp_calc = adjust_to_tick(trigger_price * 1.02, method='round')
            sl_calc = adjust_to_tick(vwap * 0.985, method='floor')

            if "攻擊" in raw_state and code not in self.active_flags: event_label = "🔥攻擊"
            elif "漲停" in raw_state and scope == "inventory": event_label = "👑漲停"
            elif "撤退" in raw_state and scope == "inventory": event_label = "🚨撤退"
            elif "出貨" in raw_state and code not in self.daily_risk_flags and scope == "inventory": event_label = "💀出貨"
            elif "尾盤" in raw_state: event_label = "🔥尾盤"

            if code not in self.adv_indicator_cache or (now_ts - self.adv_indicator_cache[code].get('time', 0)) > 300:
                c_rsi, c_br, c_bp = self._calculate_advanced_indicators(code)
                self.adv_indicator_cache[code] = {'time': now_ts, 'rsi': c_rsi, 'br': c_br, 'bp': c_bp}

            rsi_val = self.adv_indicator_cache[code].get('rsi', 0.0)
            band_ratio_val = self.adv_indicator_cache[code].get('br', 0.0)
            b_percent_val = self.adv_indicator_cache[code].get('bp', 0.0)
            ctrl_ratio = (net_day / vol_lots * 100) if vol_lots > 0 else 0
            
            if event_label:
                if "攻擊" in event_label: self.active_flags[code] = True
                if "出貨" in event_label or "撤退" in event_label: self.daily_risk_flags[code] = True
                
                ev = SniperEvent(code=code, name=get_stock_name(code), scope=scope, event_kind="STRATEGY", event_label=event_label, price=price, pct=pct, vwap=vwap, ratio=ratio_5ma, ratio_yest=ratio_yest, net_10m=net_10m, net_1h=net_1h, net_day=net_day, tp_price=tp_calc, sl_price=sl_calc, win_rate=win_rate, twii_slope=current_twii_slope, rsi=rsi_val, band_ratio=band_ratio_val, b_percent=b_percent_val, control_ratio=ctrl_ratio, is_snapshot=False)
                self._dispatch_event(ev)
                db.log_telegram(ev)

            # [新增] 5分鐘定時快照紀錄
            # 建議修正為：
            # --- [修正後的 SNAPSHOT 強制紀錄邏輯] ---
            if force_snapshot:
                # 判斷標籤：如果有訊號就標註 SNAPSHOT_攻擊，沒訊號就單純 SNAPSHOT
                snap_label = "SNAPSHOT" if not event_label else f"SNAPSHOT_{event_label}"
                
                ev_snap = SniperEvent(
                    code=code, 
                    name=get_stock_name(code), 
                    scope=scope, 
                    event_kind="SNAPSHOT", 
                    event_label=snap_label, 
                    price=price, 
                    pct=pct, 
                    vwap=vwap, 
                    ratio=ratio_5ma, 
                    ratio_yest=ratio_yest, 
                    net_10m=net_10m, 
                    net_1h=net_1h, 
                    net_day=net_day, 
                    tp_price=0, 
                    sl_price=0, 
                    win_rate=win_rate, 
                    twii_slope=current_twii_slope, 
                    rsi=rsi_val, 
                    band_ratio=band_ratio_val, 
                    b_percent=b_percent_val, 
                    control_ratio=ctrl_ratio, 
                    is_snapshot=True
                )
                db.log_telegram(ev_snap)
            # ---------------------------------------

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol_lots, est_lots, ratio_5ma, net_1h, net_day, raw_state, now_ts, "DATA_OK", "B", "NORMAL", net_10m, situation, ratio_yest, active_light, rsi_val, band_ratio_val, b_percent_val)
        except Exception as e:
            return None

    def _run_loop(self):
        log_debug(">>> Sniper Engine Loop Started")
        fail_count = 0
        while self.running:
            try:
                now = datetime.now(timezone.utc) + timedelta(hours=8)
                current_ts = time.time()
                
                # --- 新增：5分鐘快照觸發器 ---
                # --- [修正後的動態採樣頻率] ---
                do_snapshot = False
                if MarketSession.is_market_open(now):
                    # 取得當前台北時間 (HHMM 格式)
                    now_hhmm = int(now.strftime('%H%M'))
                    
                    # 策略：09:00 - 09:15 採樣間隔為 60 秒；其餘時間 300 秒
                    interval = 60 if now_hhmm <= 915 else 300
                    
                    if (current_ts - self.last_snapshot_ts >= interval):
                        do_snapshot = True
                        self.last_snapshot_ts = current_ts
                        log_debug(f"📸 執行 {interval} 秒定時採樣 (目標 15 檔全覆蓋)...")

                if now.date() > self.last_reset:
                    self.active_flags = {}; self.daily_risk_flags = {}; self.daily_net = {}; self.prev_data = {}; self.vol_queues = {}; self.base_vol_cache = {}; self.wave_tracker = {}; self.adv_indicator_cache = {}
                    notification_manager.reset_daily_state()
                    db.write_queue.put(('execute', 'DELETE FROM telegram_logs', ()))
                    self.last_reset = now.date()
                    log_debug(f"[{now}] Daily Reset Complete")
                    self.update_targets()

                self._update_market_thermometer()
                
                targets = db.get_all_codes()
                self.inventory_codes = db.get_inventory_codes()
                pinned_codes = db.get_pinned_codes()
                
                if not targets: 
                    time.sleep(2)
                    continue

                inv_set = set(self.inventory_codes); pin_set = set(pinned_codes)
                def priority_key(code): return 0 if code in inv_set else 1 if code in pin_set else 2
                targets.sort(key=priority_key)

                batch = []
                # [修改] 傳入 force_snapshot
                futures = {self.executor.submit(self._fetch_stock, c, now, force_snapshot=do_snapshot): c for c in targets}
                
                for f in concurrent.futures.as_completed(futures):
                    try:
                        result = f.result(timeout=10)
                        if result: batch.append(result)
                    except: pass

                db.upsert_realtime_batch(batch)
                fail_count = 0 
                time.sleep(1.5 if MarketSession.is_market_open(now) else 5)

            except Exception as e:
                fail_count += 1
                log_debug(f"⚠️ [Engine Critical Error] {e}")
                time.sleep(min(30, 2 ** fail_count))
                try:
                    self.executor.shutdown(wait=False)
                    self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
                except: pass

# ==========================================
# [雲端核心] 全域引擎單例綁定
# ==========================================
@st.cache_resource
def get_sniper_engine():
    return SniperEngine()

engine = get_sniper_engine()

# ==========================================
# 8. UI (Table Layout)
# ==========================================
def render_streamlit_ui():
    with st.sidebar:
        st.title("🛡️ 戰情室 v6.16.15 Cloud Commander")
        st.caption(f"Update: {datetime.now(timezone(timedelta(hours=8))).strftime('%H:%M:%S')}")
        st.markdown("---")

        mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
        # 👇 實裝菁英過濾開關
        use_elite_filter = st.checkbox("👑 菁英過濾協議 (>70元 & 總量>3000張)")

        if mode == "👨‍✈️ 指揮官":
            with st.expander("📦 庫存管理", expanded=False):
                inv_input = st.text_area("庫存清單", DEFAULT_INVENTORY, height=100)
                if st.button("更新庫存"):
                    db.update_inventory_list(inv_input)
                    time.sleep(0.5); engine.update_targets(); st.rerun()

            with st.expander("🔭 監控設定", expanded=True):
                raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=150)
                if st.button("1. 初始化並更新清單", type="primary"):
                    if not API_KEYS: st.error("缺 API Key")
                    else:
                        db.update_watchlist(raw_input)
                        time.sleep(0.5)
                        engine.update_targets()
                        targets = engine.targets
                        status = st.status("正在建立戰略數據 (含60天回測)...", expanded=True)
                        static_list = []
                        progress_bar = status.progress(0)

                        for i, code in enumerate(targets):
                            status.write(f"正在分析 {code} 歷史戰報...")
                            current_key = API_KEYS[i % len(API_KEYS)]
                            client = RestClient(api_key=current_key)
                            vol_5ma, vol_yest, price_5ma, wr, ar, amp = fetch_static_stats(client, code)
                            static_list.append((code, vol_5ma, vol_yest, price_5ma, wr, ar, amp))
                            progress_bar.progress((i + 1) / len(targets))
                            time.sleep(0.1)

                        db.upsert_static(static_list)
                        engine.update_targets()
                        status.update(label="戰略數據建立完成！", state="complete")
                        st.rerun()

            with st.expander("📥 戰情報告 (推播 LOG 下載)", expanded=False):
                st.caption("自動記錄當日 Telegram 推播歷史，包含當下價格、籌碼、大盤斜率與技術指標(RSI等)。")
                logs_df = db.get_telegram_logs()
                if not logs_df.empty:
                    st.dataframe(logs_df.tail(10), hide_index=True) 
                    csv = logs_df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 下載今日推播 LOG (CSV)",
                        data=csv,
                        file_name=f"sniper_telegram_logs_{datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d')}.csv",
                        mime='text/csv'
                    )
                else:
                    st.info("尚無推播紀錄")

            with st.expander("🛠️ 系統除錯日誌 (Debug Logs)", expanded=False):
                st.caption("即時監控背景引擎的運作狀態與報錯。")
                if st.button("🔄 刷新日誌"): st.rerun()
                log_text = "\n".join(sys_debug_logs) if sys_debug_logs else "目前無除錯訊息..."
                st.text_area("終端機輸出", value=log_text, height=200, disabled=True)

            st.markdown("---")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🟢 啟動監控", disabled=engine.running):
                    engine.start(); st.toast("核心已啟動"); st.rerun()
            with col_b:
                if st.button("🔴 停止監控", disabled=not engine.running):
                    engine.stop(); st.toast("核心已停止"); st.rerun()
        
        st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")
        
        if not engine.running and "auto_started" not in st.session_state:
            st.warning("⚠️ 偵測到核心停止，嘗試自動重啟...")
            try:
                engine.start()
                st.session_state.auto_started = True
                st.rerun()
            except Exception as e:
                st.error(f"重啟失敗: {e}")

    try:
        from streamlit import fragment
    except ImportError:
        def fragment(run_every=None):
            def decorator(func):
                def wrapper(*args, **kwargs):
                    if run_every:
                        if "last_frag_run" not in st.session_state: st.session_state.last_frag_run = time.time()
                        if time.time() - st.session_state.last_frag_run >= run_every:
                            st.session_state.last_frag_run = time.time()
                            st.rerun()
                    return func(*args, **kwargs)
                return wrapper
            return decorator

    @fragment(run_every=1.5)
    @fragment(run_every=1.5)
    def render_live_dashboard():
        # 👇 庫存戰況 UI 已徹底抹除，直接進入精銳監控
        st.subheader("⚔️ 精銳監控 (Tactical Table)")
        df_watch = db.get_watchlist_view()
        
        if engine.twii_data:
            twii_row = pd.DataFrame([engine.twii_data])
            if not df_watch.empty:
                for col in df_watch.columns:
                    if col not in twii_row.columns: twii_row[col] = 0
                df_watch = pd.concat([twii_row, df_watch], ignore_index=True)
            else: df_watch = twii_row

        if df_watch.empty: st.info("尚未加入監控標的"); return

        numeric_cols = ['price', 'pct', 'vwap', 'vol', 'ratio', 'ratio_yest', 'net_10m', 'net_1h', 'net_day', 'price_5ma', 'win_rate', 'avg_ret', 'active_light', 'avg_amp', 'twii_slope', 'rsi', 'band_ratio', 'b_percent']
        for col in numeric_cols:
            if col in df_watch.columns: df_watch[col] = pd.to_numeric(df_watch[col], errors='coerce').fillna(0.0)

        # 👇 執行菁英過濾協議 (大盤 0000 絕對保留)
        if use_elite_filter: 
            df_watch = df_watch[(df_watch['code'] == '0000') | ((df_watch['price'] > 70) & (df_watch['vol'] >= 3000))]

        # 👇 下方維持原本的 HTML 表格渲染邏輯不變
        table_start = """
    <style>
    table.sniper-table { width: 100%; border-collapse: collapse; font-family: 'Courier New', monospace; }
    table.sniper-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; white-space: nowrap; }
    table.sniper-table td { padding: 6px; border-bottom: 1px solid #444; font-size: 15px; vertical-align: middle; white-space: nowrap; }
    table.sniper-table tr.pinned-row { background-color: #fff9c4 !important; color: black !important; }
    table.sniper-table tr.golden-row { background-color: #fff9c4 !important; color: black !important; font-weight: bold; }
    table.sniper-table tr:hover { background-color: #f0f2f6; color: black; }
    </style>
    <table class="sniper-table">
    <thead>
    <tr>
    <th>📌</th><th>代碼/名稱 (Link)</th><th>訊號</th><th>5MA</th><th>現價</th><th>漲跌%</th>
    <th>均價 (燈/Buy/TP/SL)</th><th>量比 (昨/5日)</th><th>局勢</th><th>大戶 (10m/1H/日)</th><th>指標(RSI/BW/%b)</th>
    </tr>
    </thead>
    <tbody>
    """
        html_rows = []
        for _, row in df_watch.iterrows():
            situation = str(row.get('situation') or '盤整')
            event_label = str(row.get('event_label') or '')
            is_pinned = row.get('is_pinned', 0)
            row_class = "pinned-row" if is_pinned else ""
            pin_icon = "📌" if is_pinned else ""
            is_twii = str(row['code']) == "0000"

            win_rate = row.get("win_rate", 0)
            avg_ret = row.get("avg_ret", 0)
            avg_amp = row.get("avg_amp", 0)

            main_color = "#ff4d4f" if row['pct'] > 0 else "#2ecc71" if row['pct'] < 0 else "#999999"
            price_html = f"<span style='color:{main_color}; font-weight:bold'>{row['price']:.2f}</span>"
            pct_html = f"<span style='color:{main_color}'>{row['pct']:.2f}%</span>"

            active_light = row.get('active_light', 0)
            vwap_color = "#ff4d4f" if row['price'] >= row['vwap'] else "#2ecc71"
            
            rsi_val = row.get('rsi', 0.0)
            br_val = row.get('band_ratio', 0.0)
            bp_val = row.get('b_percent', 0.0)

            if rsi_val > 0: indicators_html = f"<span style='font-size:0.85em; color:#555;'>{rsi_val:.1f} / {br_val:.2f} / {bp_val:.2f}</span>"
            else: indicators_html = "<span style='color:#777'>-</span>"

            if is_twii: 
                twii_slope = float(row.get('twii_slope', 0.0))
                slope_color = "#2ecc71" if twii_slope > 0 else "#ff4d4f" if twii_slope < 0 else "#999999"
                slope_light = "🟢" if twii_slope > 0 else "🔴" if twii_slope < 0 else "⚪"
                
                name_html = f'<span style="font-weight:bold;">{row["code"]} {row["name"]}</span>'
                vwap_html = f"<span style='color:{slope_color}; font-weight:bold'>5m斜率: {twii_slope:+.2f}</span>"
                ratio_html = "<span style='color:#777'>-</span>"
                situation_text = "趨勢向上" if twii_slope > 0 else "趨勢向下" if twii_slope < 0 else "橫盤"
                situation_html = f"<span style='color:{slope_color}; font-weight:bold'>{situation_text} {slope_light}</span>"
                bp_html = "<span style='color:#777'>- / - / -<br>(-%)</span>"

            else:
                # 垂直整合 代碼/名稱/回測數據
                name_html = f'<a href="https://tw.stock.yahoo.com/quote/{row["code"]}.TW" target="_blank" style="text-decoration:none; color:#3498db; font-weight:bold; font-size:16px;">{row["code"]} {row["name"]}</a><br><span style="font-size:0.85em; color:#888;">({win_rate:.0f}%, Exp{avg_ret:+.2f})</span>'
                if win_rate < 50: name_html += " <span style='color:#ff4d4f; font-size:0.8em;'>(高風險)</span>"
                if avg_amp >= MIN_AMPLITUDE_THRESHOLD: name_html += " <span style='color:#e67e22; font-weight:bold;'>(瘋)</span>"

                if active_light == 1: vwap_light = "🟢"
                else: vwap_light = "🔴"
                trigger_val = row['vwap'] * 1.005
                buy_price = adjust_to_tick(trigger_val, method='round')
                buy_html = f"<span style='color:#e67e22; font-weight:bold; margin-left:4px;'>Buy:{buy_price:.2f}</span>"
                vwap_html = f"<span style='color:{vwap_color}'>{row['vwap']:.2f} {vwap_light}</span> {buy_html}"
                
                trigger_price = row['vwap'] * 1.005
                tp_price = adjust_to_tick(trigger_price * 1.02, method='round')
                sl_price = adjust_to_tick(row['vwap'] * 0.985, method='floor')
                vwap_html += f"<br><span style='font-size:0.85em; color:#888'>(TP:{tp_price:.1f} / SL:{sl_price:.1f})</span>"

                thresholds = get_dynamic_thresholds(row['price'])
                r_yest = row.get('ratio_yest', 0); r_5ma = row['ratio']
                is_vol_strong = r_5ma >= thresholds['tgt_ratio']
                vol_light = "🟢" if is_vol_strong else "🔴"
                c_5ma_r = "#ff4d4f" if is_vol_strong else "#999999"
                ratio_html = f"{r_yest:.1f} / <span style='color:{c_5ma_r}; font-weight:bold'>{r_5ma:.1f} {vol_light}</span>"

                sit_color = "#ff4d4f" if "吸籌" in situation or "攻擊" in situation else "#2ecc71" if "倒貨" in situation else "#e67e22" if "吃盤" in situation else "#999999"
                clean_situation = situation.replace("🔥", "").replace("🛡️", "").replace("💀", "").replace("🎣", "").replace("⚖️", "")
                situation_html = f"<span style='color:{sit_color}; font-weight:bold'>{clean_situation}</span>"

                # 提取數據並計算大戶控盤比 (%)
                n10 = int(row.get('net_10m', 0))
                n1h = int(row.get('net_1h', 0))
                nd = int(row.get('net_day', 0))
                vol = float(row.get('vol', 0))

                chip_ratio = (nd / vol * 100) if vol > 0 else 0.0
                chip_ratio_str = f"{chip_ratio:+.1f}%"
                ratio_color = "#ff4d4f" if chip_ratio > 0 else "#2ecc71" if chip_ratio < 0 else "#999999"

                bp_light = "🟢" if n1h > 0 else "🔴"
                c10 = "#ff4d4f" if n10 > 0 else "#2ecc71" if n10 < 0 else "#999999"
                c1h = "#ff4d4f" if n1h > 0 else "#2ecc71" if n1h < 0 else "#999999"
                cd  = "#ff4d4f" if nd > 0 else "#2ecc71" if nd < 0 else "#999999"
                
                # 組合字串，換行顯示控盤比
                bp_html = f"<span style='color:{c10}'>{n10}</span> / <span style='color:{c1h}'>{n1h} {bp_light}</span> / <span style='color:{cd}'>{nd}</span><br><span style='color:{ratio_color}; font-size:0.9em; font-weight:bold;'>({chip_ratio_str})</span>"

                is_three_lights = (active_light == 1) and (vol_light == "🟢") and (bp_light == "🟢")
                if is_three_lights: row_class = "golden-row"
                elif is_pinned: row_class = "pinned-row"
                else: row_class = ""

            p_5ma = row.get('price_5ma', 0)
            c_5ma = "#ff4d4f" if row['price'] > p_5ma else "#2ecc71"
            ma_html = f"<span style='color:{c_5ma}'>{p_5ma:.2f}</span>"
            
            html_rows.append(f'<tr class="{row_class}"><td>{pin_icon}</td><td>{name_html}</td><td>{event_label}</td><td>{ma_html}</td><td>{price_html}</td><td>{pct_html}</td><td>{vwap_html}</td><td>{ratio_html}</td><td>{situation_html}</td><td>{bp_html}</td><td>{indicators_html}</td></tr>')

        st.markdown(table_start + "".join(html_rows) + "</tbody></table>", unsafe_allow_html=True)

    render_live_dashboard()

# ==========================================
# 9. Console Backtest Mode
# ==========================================
def run_console_backtest(target_code):
    print(f"\n{Fore.CYAN}📡 調閱 {target_code} 過去 60 天戰報...{Style.RESET_ALL}")
    try:
        df_daily = yf.download(target_code, period=PERIOD, interval="1d", progress=False, auto_adjust=True)
        if isinstance(df_daily.columns, pd.MultiIndex): df_daily.columns = df_daily.columns.get_level_values(0)
        df_daily['Prev_Close'] = df_daily['Close'].shift(1)
        df_daily['Amplitude'] = (df_daily['High'] - df_daily['Low']) / df_daily['Prev_Close'] * 100
        avg_amp_5d = df_daily['Amplitude'].dropna().tail(5).mean()
        if np.isnan(avg_amp_5d): avg_amp_5d = 0
        
        df = yf.download(target_code, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=True)
        if df.empty:
            target_code = target_code.replace(".TW", ".TWO")
            df = yf.download(target_code, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=True)
        
        if df.empty or len(df) < 50:
             print(f"{Fore.YELLOW}⚠️ 警告：資料筆數過少或找不到{Style.RESET_ALL}"); return
    except Exception as e:
        print(f"{Fore.RED}❌ 下載失敗: {e}{Style.RESET_ALL}"); return

    if avg_amp_5d >= MIN_AMPLITUDE_THRESHOLD:
        PARAMS = MAD_DOG_PARAMS
        mode_name = f"{Fore.RED}🔥 瘋狗模式 (High Vol){Style.RESET_ALL}"
    else:
        PARAMS = NORMAL_PARAMS
        mode_name = f"{Fore.GREEN}🛡️ 常規模式 (Normal){Style.RESET_ALL}"

    wave_str = "全波段" if PARAMS['TARGET_WAVE'] == 0 else f"第 {PARAMS['TARGET_WAVE']} 波"
    print(f"📊 均震幅: {avg_amp_5d:.2f}% -> 採用 {mode_name}")
    print(f"⚙️ 設定: {wave_str} | 日限 {PARAMS['MAX_TRADES_DAY']} 次 | 漲幅限 {PARAMS['MAX_DAY_PCT']}%")
    print(f"   - 天花板: +{PARAMS['ENTRY_CEILING_PCT']}% | 停損: -{PARAMS['STOP_LOSS_PCT']}%")

    df = _calculate_intraday_vwap(df).dropna()
    trade_logs = []; results = []; unique_dates = sorted(list(set(df.index.date)))
    total_wins = 0; total_losses = 0

    ENTRY_THRESHOLD = 1 + (ENTRY_THRESHOLD_PCT / 100)
    ENTRY_CEILING = 1 + (PARAMS['ENTRY_CEILING_PCT'] / 100)
    TRIGGER = 1 + (TRIGGER_PCT / 100)
    CALLBACK = TRAILING_CALLBACK / 100
    STOP_LOSS = 1 - (PARAMS['STOP_LOSS_PCT'] / 100)

    for trade_date in unique_dates:
        mask = df['Date'] == trade_date
        day_data = df.loc[mask]
        if day_data.empty: continue

        in_pos = False; entry_p = 0; entry_v = 0; entry_time_str = ""; daily_executed_trades = 0
        wave_count = 0; last_wave_ts = 0; is_holding_wave = False
        
        try: yesterday_close = day_data['Close'].iloc[0]
        except: continue
        max_p_after_entry = 0; trailing_active = False

        for ts, row in day_data.iterrows():
            p = float(row['Close']); v = float(row['VWAP']); t = ts.time(); curr_ts = ts.timestamp()

            if daily_executed_trades >= PARAMS['MAX_TRADES_DAY']: break

            entry_line = v * ENTRY_THRESHOLD
            if p >= entry_line:
                if not is_holding_wave:
                    if (curr_ts - last_wave_ts) > 300:
                        wave_count += 1
                        last_wave_ts = curr_ts
                is_holding_wave = True
            else:
                is_holding_wave = False

            if not in_pos:
                if t.hour == 9 and t.minute < 10: continue
                if t.hour >= 13: continue
                
                if (v * ENTRY_THRESHOLD) < p < (v * ENTRY_CEILING):
                    day_pct = (p - yesterday_close) / yesterday_close * 100
                    if day_pct <= PARAMS['MAX_DAY_PCT']:
                        should_trade = False
                        if PARAMS['TARGET_WAVE'] == 0: should_trade = True
                        elif wave_count == PARAMS['TARGET_WAVE']: should_trade = True
                        
                        if should_trade:
                            in_pos = True
                            entry_p = p; entry_v = v; entry_time_str = t.strftime("%H:%M")
                            daily_executed_trades += 1
                            max_p_after_entry = p
                            trailing_active = False

            elif in_pos:
                exit_price = 0; exit_reason = ""
                if p <= entry_v * STOP_LOSS: exit_price = p; exit_reason = "停損"
                
                if p > max_p_after_entry: max_p_after_entry = p
                if p >= entry_p * TRIGGER: trailing_active = True
                
                if trailing_active and exit_price == 0:
                    dynamic_exit = max_p_after_entry * (1 - CALLBACK)
                    if p <= dynamic_exit: exit_price = dynamic_exit; exit_reason = "鎖利"

                if t.hour == 13 and t.minute >= 25 and exit_price == 0: exit_price = p; exit_reason = "尾盤"

                if exit_price > 0:
                    ret = (exit_price - entry_p) / entry_p * 100
                    results.append(ret / 100)
                    if ret > 0: total_wins += 1
                    else: total_losses += 1
                    color = Fore.RED if ret < 0 else Fore.GREEN
                    trade_logs.append({"date": trade_date, "time": entry_time_str, "buy": entry_p, "sell": exit_price, "ret": ret, "reason": exit_reason, "color": color})
                    in_pos = False

    total_trades = total_wins + total_losses
    if total_trades == 0:
        print(f"{Fore.YELLOW}⚠️ 無交易紀錄。{Style.RESET_ALL}")
        return

    print(f"\n📝 {Fore.YELLOW}交易明細 (日期 | 時間 | 進場 | 出場 | 損益){Style.RESET_ALL}")
    print("-" * 60)
    for log in trade_logs:
        print(f"{log['date']} | {log['time']} | {log['buy']:.1f} | {log['sell']:.1f} | {log['color']}{log['ret']:+5.2f}%{Style.RESET_ALL} ({log['reason']})")
    print("-" * 60)

    win_rate = (total_wins / total_trades * 100)
    avg_profit = np.mean(results) * 100

    print(f"📊 總結報告:")
    print(f"   交易次數: {total_trades} (勝 {total_wins} / 敗 {total_losses})")
    print(f"   勝率: {Fore.GREEN if win_rate >= 50 else Fore.RED}{win_rate:.2f}%{Style.RESET_ALL}")
    print(f"   平均報酬: {Fore.GREEN if avg_profit > 0 else Fore.RED}{avg_profit:+.2f}%{Style.RESET_ALL} (不含稅費)")

# ==========================================
# 10. Main Dispatcher
# ==========================================
if __name__ == "__main__":
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx():
            render_streamlit_ui()
        else:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"{Fore.YELLOW}🔥 Sniper 回測終端機 v6.16.15 (Cloud Commander){Style.RESET_ALL}")
            while True:
                try:
                    user_input = input(f"\n請輸入股票代碼 (輸入 q 離開): ").strip().upper()
                    if user_input in ['Q', 'EXIT', 'QUIT']: break
                    if not user_input: continue
                    if user_input.isdigit(): target = f"{user_input}.TW"
                    else: target = user_input
                    run_console_backtest(target)
                except KeyboardInterrupt: break
                except Exception as e: print(f"錯誤: {e}")
    except ModuleNotFoundError:
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"{Fore.YELLOW}🔥 Sniper 回測終端機 v6.16.15 (Cloud Commander){Style.RESET_ALL}")
        while True:
            try:
                user_input = input(f"\n請輸入股票代碼 (輸入 q 離開): ").strip().upper()
                if user_input in ['Q', 'EXIT', 'QUIT']: break
                if not user_input: continue
                if user_input.isdigit(): target = f"{user_input}.TW"
                else: target = user_input
                run_console_backtest(target)
            except KeyboardInterrupt: break
            except Exception as e: print(f"錯誤: {e}")
