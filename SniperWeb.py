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

# [FIX] Colorama 防呆機制
try:
    import colorama
    from colorama import Fore, Style
    colorama.init(autoreset=True)
except ImportError:
    class MockColor:
        def __getattr__(self, name): return ""
    Fore = Style = MockColor()
    colorama = None

# [LOG FIX] Silence yfinance and other non-critical warnings
warnings.filterwarnings("ignore")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 0. Global Strategy Config (Strictly synced)
# ==========================================
MIN_AMPLITUDE_THRESHOLD = 4.0  # 瘋狗股判定標準

# [🅰️ 瘋狗股參數]
MAD_DOG_PARAMS = {
    'TARGET_WAVE': 2,           # 只做第 2 波
    'MAX_TRADES_DAY': 2,
    'ENTRY_CEILING_PCT': 4.0,   
    'STOP_LOSS_PCT': 2.0,
    'MAX_DAY_PCT': 3.5          
}

# [🅱️ 常規股參數]
NORMAL_PARAMS = {
    'TARGET_WAVE': 0,           
    'MAX_TRADES_DAY': 2,
    'ENTRY_CEILING_PCT': 2.0,   
    'STOP_LOSS_PCT': 0.0,
    'MAX_DAY_PCT': 2.5          
}

# [共通設定]
ENTRY_THRESHOLD_PCT = 0.5       # 啟動: VWAP + 0.5%
TRIGGER_PCT = 1.5
TRAILING_CALLBACK = 1.0
PERIOD = "60d"
INTERVAL = "5m"

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v6.16.15 BuySignal", page_icon="⚔️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v616.db"

DEFAULT_WATCHLIST = "3006 3037 1513 3189 1795 3491 8046 6274"
DEFAULT_INVENTORY = """2481,84.4,3
3231,150.14,7
4566,54.94,2
8046,252.64,7"""

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
    timestamp: float = field(default_factory=time.time)
    data_status: str = "DATA_OK"
    is_test: bool = False

# ==========================================
# 2. Market Session
# ==========================================
class MarketSession:
    MARKET_OPEN, MARKET_CLOSE = dt_time(9, 0), dt_time(13, 35)
    @staticmethod
    def is_market_open(now=None):
        if not now: now = datetime.now(timezone.utc) + timedelta(hours=8)
        return MarketSession.MARKET_OPEN <= now.time() <= MarketSession.MARKET_CLOSE

# ==========================================
# 3. Database
# ==========================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.write_queue = queue.Queue()
        self._init_db()
        threading.Thread(target=self._writer_loop, daemon=True).start()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, signal TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL', situation TEXT, ratio_yest REAL, active_light INTEGER DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, vol_5ma REAL, vol_yest REAL, price_5ma REAL, win_rate REAL DEFAULT 0, avg_ret REAL DEFAULT 0, avg_amp REAL DEFAULT 0)''')
        
        try: c.execute("ALTER TABLE static_info ADD COLUMN avg_amp REAL DEFAULT 0")
        except: pass
        try: c.execute("ALTER TABLE realtime ADD COLUMN active_light INTEGER DEFAULT 0")
        except: pass
        
        conn.commit(); conn.close()

    def _writer_loop(self):
        conn = self._get_conn(); cursor = conn.cursor()
        while True:
            try:
                tasks = []
                try: tasks.append(self.write_queue.get(timeout=0.1))
                except queue.Empty: continue
                while not self.write_queue.empty() and len(tasks) < 50: tasks.append(self.write_queue.get())
                for task_type, sql, args in tasks:
                    try:
                        if task_type == 'executemany': cursor.executemany(sql, args)
                        else: cursor.execute(sql, args)
                    except: pass
                conn.commit()
                for _ in tasks: self.write_queue.task_done()
            except: time.sleep(1)

    def upsert_realtime_batch(self, data_list):
        if not data_list: return
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, est_vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m, situation, ratio_yest, active_light) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, vol_5ma, vol_yest, price_5ma, win_rate, avg_ret, avg_amp) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))

    def update_inventory_list(self, inventory_text):
        self.write_queue.put(('execute', 'DELETE FROM inventory', ()))
        for line in inventory_text.split('\n'):
            parts = line.split(',')
            if len(parts) >= 2:
                try: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO inventory (code, cost, qty) VALUES (?, ?, ?)', (parts[0].strip(), float(parts[1].strip()), float(parts[2].strip()) if len(parts) > 2 else 1.0)))
                except: pass

    def update_watchlist(self, codes_text):
        self.write_queue.put(('execute', 'DELETE FROM watchlist', ()))
        targets = [t.strip() for t in codes_text.split() if t.strip()]
        for t in targets: self.write_queue.put(('execute', 'INSERT OR REPLACE INTO watchlist (code) VALUES (?)', (t,)))
        return targets

    def get_watchlist_view(self):
        conn = self._get_conn()
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_10m, r.net_1h, r.net_day, r.situation, r.active_light, s.price_5ma, s.win_rate, s.avg_ret, s.avg_amp, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.ratio_yest, r.signal as event_label, r.net_1h, r.net_day, r.situation, s.price_5ma, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_all_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM inventory UNION SELECT code FROM watchlist UNION SELECT code FROM pinned')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_inventory_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM inventory')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_pinned_codes(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code FROM pinned')
        rows = c.fetchall(); conn.close(); return [r[0] for r in rows]

    def get_volume_map(self):
        conn = self._get_conn(); c = conn.cursor()
        c.execute('SELECT code, vol_5ma, vol_yest, price_5ma, win_rate, avg_amp FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: {'vol_5ma': r[1], 'vol_yest': r[2], 'price_5ma': r[3], 'win_rate': r[4], 'avg_amp': r[5]} for r in rows}

db = Database(DB_PATH)

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
    
    if method == 'round':
        return round(price / tick) * tick
    else:
        return math.floor(price / tick) * tick

def _calculate_intraday_vwap(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC').tz_convert('Asia/Taipei')
    else:
        df.index = df.index.tz_convert('Asia/Taipei')

    df['Date'] = df.index.date
    df['TP_Vol'] = df['Close'] * df['Volume']
    df['Cum_Vol'] = df.groupby('Date')['Volume'].cumsum()
    df['Cum_Val'] = df.groupby('Date')['TP_Vol'].cumsum()
    df['VWAP'] = df['Cum_Val'] / df['Cum_Vol']
    return df

# === 終端機/網頁版共用回測邏輯 ===
def _run_core_backtest(df, params):
    """
    核心回測運算 (含波次時間冷卻邏輯)
    """
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
        
        # 波次計數器 (Daily Reset)
        wave_count = 0
        last_wave_ts = 0
        is_holding_wave = False
        
        try: yesterday_close = day_data['Close'].iloc[0] 
        except: continue

        max_p_after_entry = 0; trailing_active = False

        for ts, row in day_data.iterrows():
            p = float(row['Close'])
            v = float(row['VWAP'])
            t = ts.time()
            curr_ts = ts.timestamp()

            if daily_executed_trades >= params['MAX_TRADES_DAY']: break

            # 1. 更新波次 (Time Cool-down Logic)
            entry_line = v * ENTRY_THRESHOLD
            if p >= entry_line:
                if not is_holding_wave:
                    # 剛突破，檢查冷卻
                    if (curr_ts - last_wave_ts) > 300: # 5 mins
                        wave_count += 1
                        last_wave_ts = curr_ts
                is_holding_wave = True
            else:
                is_holding_wave = False

            # 2. 進場邏輯
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

            # 3. 出場邏輯
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

# 網頁版用的快速回測
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
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < market_open: return 0
    elapsed_minutes = (now - market_open).seconds / 60
    if elapsed_minutes <= 0: return 0
    if elapsed_minutes >= 270: return current_vol
    weight = 2.0 if elapsed_minutes < 15 else 1.0
    return int(current_vol * (270 / elapsed_minutes) / weight)

def check_signal(pct, is_bullish, net_day, net_1h, ratio, thresholds, is_breakdown, price, vwap, has_attacked, now_time, vol_lots):
    if is_breakdown: return "🚨撤退"
    if pct >= 9.5: return "👑漲停"
    
    if now_time.time() < dt_time(9, 5): return "⏳暖機"

    in_golden_zone = False
    if is_bullish and price <= (vwap * 1.015):
        in_golden_zone = True

    if ratio >= thresholds['tgt_ratio']:
        if in_golden_zone and net_1h > 0:
            return "🔥攻擊"
        elif net_1h < 0:
            return "💀出貨"
        
    if not is_bullish:
        return "📉線下"
        
    if is_bullish and not in_golden_zone:
         return "⚠️追高"

    bias = ((price - vwap) / vwap) * 100 if vwap > 0 else 0
    if bias > thresholds['overheat']: return "⚠️過熱"

    if dt_time(13, 0) <= now_time.time() <= dt_time(13, 25):
        if (3.0 <= pct <= 9.0) and (net_1h > 0) and (net_day / (vol_lots+1) >= 0.05): return "🔥尾盤"
        
    if pct > 2.0 and net_1h < 0: return "❌誘多"
    if pct >= thresholds['tgt_pct']: return "⚠️價強"
    
    return "盤整"

# ==========================================
# 5. Notification
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    RATE_LIMIT_DELAY = 1.0
    EMOJI_MAP = {
        "🔥攻擊": "🚀", "💣伏擊": "💣", "👀量增": "👀",
        "💀出貨": "💀", "🚨撤退": "⚠️", "👑漲停": "👑",
        "⚠️價強": "💪", "❌誘多": "🎣", "🔥尾盤": "🔥",
        "⚠️過熱": "🚫", "⏳暖機": "⏳", "📉線下": "📉", "⚠️追高": "🚫"
    }

    def __init__(self):
        self._queue = queue.Queue()
        self._cooldowns = {}
        threading.Thread(target=self._worker_loop, daemon=True).start()

    def reset_daily_state(self):
        self._cooldowns.clear()

    def should_notify(self, event: SniperEvent) -> bool:
        if event.is_test: return True
        if not MarketSession.is_market_open(): return False
        
        if event.scope == "watchlist" and event.event_label == "🔥攻擊":
            if event.win_rate < 50:
                return False

        if event.event_label == "⚠️追高":
            return False

        key = f"{event.code}_{event.scope}_{event.event_label}"
        
        if event.scope == "inventory":
            if event.event_label not in ["💀出貨", "🚨撤退", "🔥攻擊", "👑漲停"]:
                return False
            if "撤退" in event.event_label:
                 if time.time() - self._cooldowns.get(key, 0) < 300: return False
                 return True

        if event.scope == "watchlist":
            if event.event_label != "🔥攻擊":
                return False

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
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 6. Engine (Sniper Core)
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
        
        self.market_stats = {"Time": 0, "Price5MA": 0} 
        self.twii_data = None 
        self.last_reset = datetime.now().date()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=12)
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
        threading.Thread(target=self._run_loop, daemon=True).start()

    def stop(self): self.running = False
    
    def _init_market_stats(self):
        try:
             tse = yf.Ticker("^TWII")
             hist = tse.history(period="10d", auto_adjust=True)
             if not hist.empty:
                 self.market_stats["Price5MA"] = hist['Close'].iloc[-6:-1].mean() if len(hist) >= 6 else hist['Close'].mean()
             else:
                 self.market_stats["Price5MA"] = 0
        except: pass

    def _update_market_thermometer(self):
        if time.time() - self.market_stats.get("Time", 0) < 3: return
        current_price = 0; current_pct = 0
        now_str = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%H:%M:%S')
        source_status = f"{now_str} (Crawler)"

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
             price_5ma = self.market_stats.get("Price5MA", current_price)
             self.twii_data = {
                'code': '0000', 'name': f'加權指數 {source_status}', 
                'price': current_price, 'pct': current_pct, 'vwap': current_price, 
                'price_5ma': price_5ma, 'ratio': 1.0, 'ratio_yest': 1.0,
                'net_10m': 0, 'net_1h': 0, 'net_day': 0,
                'situation': '市場指標', 'event_label': '大盤',
                'is_pinned': 1, 'win_rate': 0, 'avg_ret': 0, 'avg_amp': 0, 'active_light': 1
            }
             self.market_stats["Time"] = time.time()

    def _dispatch_event(self, ev: SniperEvent):
        notification_manager.enqueue(ev)

    def _fetch_stock(self, code, now_time=None):
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

            # === [核心升級] 時間冷卻波次計數器 ===
            if code not in self.wave_tracker:
                self.wave_tracker[code] = {
                    'count': 0,
                    'last_trigger_ts': 0,
                    'is_holding': False 
                }

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

            thresholds = get_dynamic_thresholds(price)
            raw_state = check_signal(pct, is_bullish, net_day, net_1h, ratio_5ma, thresholds, is_breakdown, price, vwap, code in self.active_flags, now_time, vol_lots)

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

            if event_label:
                if "攻擊" in event_label: self.active_flags[code] = True
                if "出貨" in event_label or "撤退" in event_label: self.daily_risk_flags[code] = True
                ev = SniperEvent(code=code, name=get_stock_name(code), scope=scope, event_kind="STRATEGY", event_label=event_label, price=price, pct=pct, vwap=vwap, ratio=ratio_5ma, ratio_yest=ratio_yest, net_10m=net_10m, net_1h=net_1h, net_day=net_day, tp_price=tp_calc, sl_price=sl_calc, win_rate=win_rate)
                self._dispatch_event(ev)

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol_lots, est_lots, ratio_5ma, net_1h, net_day, raw_state, now_ts, "DATA_OK", "B", "NORMAL", net_10m, situation, ratio_yest, active_light)
        except: return None

    def _run_loop(self):
        while self.running:
            try:
                now = datetime.now(timezone.utc) + timedelta(hours=8)
                if now.date() > self.last_reset:
                    self.active_flags = {}; self.daily_risk_flags = {}; self.daily_net = {}; self.prev_data = {}; self.vol_queues = {}; self.base_vol_cache = {}; self.wave_tracker = {}
                    notification_manager.reset_daily_state()
                    self.last_reset = now.date()

                self._update_market_thermometer()
                targets = db.get_all_codes()
                self.inventory_codes = db.get_inventory_codes()
                pinned_codes = db.get_pinned_codes()
                if not targets: time.sleep(2); continue

                inv_set = set(self.inventory_codes); pin_set = set(pinned_codes)
                def priority_key(code): return 0 if code in inv_set else 1 if code in pin_set else 2
                targets.sort(key=priority_key)

                batch = []
                futures = [self.executor.submit(self._fetch_stock, c, now) for c in targets]
                for f in concurrent.futures.as_completed(futures):
                    if f.result(): batch.append(f.result())
                db.upsert_realtime_batch(batch)
                time.sleep(1.5 if MarketSession.is_market_open(now) else 5)
            except Exception as e:
                print(f"Engine Loop Error: {e}")
                time.sleep(5)

if "sniper_engine_core" not in st.session_state: st.session_state.sniper_engine_core = SniperEngine()
engine = st.session_state.sniper_engine_core

# ==========================================
# 7. UI (Table Layout)
# ==========================================
def render_streamlit_ui():
    with st.sidebar:
        st.title("🛡️ 戰情室 v6.16.15 BuySignal")
        st.caption(f"Update: {datetime.now(timezone(timedelta(hours=8))).strftime('%H:%M:%S')}")
        st.markdown("---")

        mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
        use_filter = st.checkbox("只看局勢活躍 (>70元)")

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

                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("🟢 啟動監控", disabled=engine.running):
                        engine.start(); st.toast("核心已啟動"); st.rerun()
                with col_b:
                    if st.button("🔴 停止監控", disabled=not engine.running):
                        engine.stop(); st.toast("核心已停止"); st.rerun()
        st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")

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
    def render_live_dashboard():
        with st.expander("📦 庫存戰況 (Inventory)", expanded=False):
            df_inv = db.get_inventory_view()
            if not df_inv.empty:
                st.dataframe(df_inv[['code', 'name', 'situation', 'price', 'pct', 'profit_val', 'signal_level']], hide_index=True)
            else: st.info("尚無庫存")

        st.markdown("---")
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

        numeric_cols = ['price', 'pct', 'vwap', 'ratio', 'ratio_yest', 'net_10m', 'net_1h', 'net_day', 'price_5ma', 'win_rate', 'avg_ret', 'active_light', 'avg_amp']
        for col in numeric_cols:
            if col in df_watch.columns: df_watch[col] = pd.to_numeric(df_watch[col], errors='coerce').fillna(0.0)

        if use_filter: df_watch = df_watch[df_watch['price'] > 70]

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
    <th>📌</th><th>代碼</th><th>名稱 (Link)</th><th>訊號</th><th>5MA</th><th>現價</th><th>漲跌%</th>
    <th>均價 (燈/Buy/TP/SL)</th><th>量比 (昨/5日)</th><th>局勢</th><th>大戶 (10m/1H/日)</th>
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

            # [NEW FORMAT LOGIC]
            win_rate = row.get("win_rate", 0)
            avg_ret = row.get("avg_ret", 0)
            avg_amp = row.get("avg_amp", 0)
            
            name_part = row['name']
            if not is_twii:
                name_part = f"{row['name']} ({win_rate:.0f}%, Exp{avg_ret:+.2f})"
                if win_rate < 50: name_part += " <span style='color:#ff4d4f; font-size:0.8em;'>(高風險)</span>"
                if avg_amp >= MIN_AMPLITUDE_THRESHOLD: name_part += " <span style='color:#e67e22; font-weight:bold;'>(瘋)</span>"

            # 1. Price
            main_color = "#ff4d4f" if row['pct'] > 0 else "#2ecc71" if row['pct'] < 0 else "#999999"
            price_html = f"<span style='color:{main_color}; font-weight:bold'>{row['price']:.2f}</span>"
            pct_html = f"<span style='color:{main_color}'>{row['pct']:.2f}%</span>"

            # 2. VWAP Light (Strict Logic from DB)
            active_light = row.get('active_light', 0)
            vwap_color = "#ff4d4f" if row['price'] >= row['vwap'] else "#2ecc71"
            
            if is_twii: 
                vwap_light = ""
                buy_html = ""
            else:
                if active_light == 1: vwap_light = "🟢"
                else: vwap_light = "🔴"
                
                # Calculate Buy Price (Trigger) for display
                trigger_val = row['vwap'] * 1.005
                buy_price = adjust_to_tick(trigger_val, method='round')
                buy_html = f"<span style='color:#e67e22; font-weight:bold; margin-left:4px;'>Buy:{buy_price:.2f}</span>"
            
            vwap_html = f"<span style='color:{vwap_color}'>{row['vwap']:.2f} {vwap_light}</span> {buy_html}"
            
            if not is_twii:
                trigger_price = row['vwap'] * 1.005
                tp_price = adjust_to_tick(trigger_price * 1.02, method='round')
                sl_price = adjust_to_tick(row['vwap'] * 0.985, method='floor')
                vwap_html += f"<br><span style='font-size:0.85em; color:#888'>(TP:{tp_price:.1f} / SL:{sl_price:.1f})</span>"

            # 3. 5MA
            p_5ma = row.get('price_5ma', 0)
            c_5ma = "#ff4d4f" if row['price'] > p_5ma else "#2ecc71"
            ma_html = f"<span style='color:{c_5ma}'>{p_5ma:.2f}</span>"

            # 4. Volume
            thresholds = get_dynamic_thresholds(row['price'])
            r_yest = row.get('ratio_yest', 0); r_5ma = row['ratio']
            is_vol_strong = r_5ma >= thresholds['tgt_ratio']
            vol_light = "🟢" if is_vol_strong else "🔴"
            c_5ma_r = "#ff4d4f" if is_vol_strong else "#999999"
            ratio_html = f"{r_yest:.1f} / <span style='color:{c_5ma_r}; font-weight:bold'>{r_5ma:.1f} {vol_light}</span>"

            # 5. Situation
            sit_color = "#ff4d4f" if "吸籌" in situation or "攻擊" in situation else "#2ecc71" if "倒貨" in situation else "#e67e22" if "吃盤" in situation else "#999999"
            clean_situation = situation.replace("🔥", "").replace("🛡️", "").replace("💀", "").replace("🎣", "").replace("⚖️", "")
            situation_html = f"<span style='color:{sit_color}; font-weight:bold'>{clean_situation}</span>"
            
            # 6. Link
            if is_twii: name_html = f'<span style="font-weight:bold;">{name_part}</span>'
            else: name_html = f'<a href="https://tw.stock.yahoo.com/quote/{row["code"]}.TW" target="_blank" style="text-decoration:none; color:#3498db; font-weight:bold;">{name_part}</a>'

            # 7. Big Player
            if is_twii: bp_html = "<span style='color:#777'>- / - / -</span>"
            else:
                n10 = int(row['net_10m']); n1h = int(row['net_1h']); nd = int(row['net_day'])
                bp_light = "🟢" if n1h > 0 else "🔴"
                c10 = "#ff4d4f" if n10 > 0 else "#2ecc71" if n10 < 0 else "#999999"
                c1h = "#ff4d4f" if n1h > 0 else "#2ecc71" if n1h < 0 else "#999999"
                cd  = "#ff4d4f" if nd > 0 else "#2ecc71" if nd < 0 else "#999999"
                bp_html = f"<span style='color:{c10}'>{n10}</span> / <span style='color:{c1h}'>{n1h} {bp_light}</span> / <span style='color:{cd}'>{nd}</span>"

            is_three_lights = (active_light == 1) and (vol_light == "🟢") and (bp_light == "🟢")
            if is_three_lights: row_class = "golden-row"
            elif is_pinned: row_class = "pinned-row"
            else: row_class = ""

            html_rows.append(f'<tr class="{row_class}"><td>{pin_icon}</td><td>{row["code"]}</td><td>{name_html}</td><td>{event_label}</td><td>{ma_html}</td><td>{price_html}</td><td>{pct_html}</td><td>{vwap_html}</td><td>{ratio_html}</td><td>{situation_html}</td><td>{bp_html}</td></tr>')

        st.markdown(table_start + "".join(html_rows) + "</tbody></table>", unsafe_allow_html=True)

    render_live_dashboard()

# ==========================================
# 8. Console Backtest Mode (Restored)
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
                    if (curr_ts - last_wave_ts) > 300: # 5 mins
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
# 9. Main Dispatcher
# ==========================================
if __name__ == "__main__":
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx():
            render_streamlit_ui()
        else:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"{Fore.YELLOW}🔥 Sniper 回測終端機 v6.16.15 (BuySignal){Style.RESET_ALL}")
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
        print(f"{Fore.YELLOW}🔥 Sniper 回測終端機 v6.16.15 (BuySignal){Style.RESET_ALL}")
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
