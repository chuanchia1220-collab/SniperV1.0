import streamlit as st
import pandas as pd
import yfinance as yf
from fugle_marketdata import RestClient
from datetime import datetime, timedelta, timezone, time as dt_time
from dataclasses import dataclass, field
import time, os, twstock, json, threading, sqlite3, concurrent.futures, requests, queue
from itertools import cycle
import warnings
import openai
from streamlit.runtime.scriptrunner import add_script_run_ctx

# [LOG FIX] Silence non-critical warnings
warnings.filterwarnings("ignore")
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. Config & Domain Models
# ==========================================
st.set_page_config(page_title="Sniper v5.41 ColorFix", page_icon="🛡️", layout="wide")

try:
    raw_fugle_keys = st.secrets.get("Fugle_API_Key", "")
    TG_BOT_TOKEN = st.secrets.get("TG_BOT_TOKEN", "")
    TG_CHAT_ID = st.secrets.get("TG_CHAT_ID", "")
except:
    raw_fugle_keys = os.getenv("Fugle_API_Key", "")
    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

API_KEYS = [k.strip() for k in raw_fugle_keys.split(',') if k.strip()]
DB_PATH = "sniper_v61.db"
DEFAULT_WATCHLIST = "3035 3037 2368 2383 6274 8046 3189 3324 3017 3653 2421 3483 3081 3163 4979 4908 3363 4977 6442 2356 3231 2382 6669 2317 2330 2454 2303 6781 4931 3533"
DEFAULT_INVENTORY = """2330,800,1\n2317,105,5"""

AI_COMMANDER_PROMPT = """
# 🛡️ Sniper 股市戰情室 AI 指揮官

**角色設定**：
你是 **Sniper 股市戰情室的 AI 指揮官**。你的核心任務是**主動**依據傳入的市場快照數據 (Market Snapshot)，進行冷靜、客觀且具備「量化思維」的交易決策。

### ⚠️ 最高準則 (Prime Directive)
**請勿再次過濾基本面**。你的任務是**確認訊號**、**判斷多空**、**給出操作建議**。

---

### 一、 訊號定義 (Sniper Signals)
* **🔥 尾盤 (End Game)**：[13:00 後] 漲幅 3~9% + 現價>均價 + 大戶控盤 > 5%。(隔日沖首選)
* **🔥 攻擊 (Attack)**：漲幅 > 3% + 現價 > 均價 + 大戶狂掃。
* **💣 伏擊 (Ambush)**：股價貼均價 + 量比爆發 + 大戶 1H 翻紅。(最佳進場點)
* **👀 量增 (Accumulation)**：股價未動，量能先行。
* **💀 出貨 (Dump)**：跌破 VWAP + 爆量 + 大戶綠賣。**優先停損。**
* **❌ 誘多 (Bull Trap)**：漲幅 > 2% 但大戶籌碼 (1H 或 Day) 為**綠色負值**。

---

### 二、 情境任務 (請依時間執行)

#### **情境 A：盤中實戰 (09:00 - 13:00)**
**輸入數據**：你將收到一份 CSV 格式的即時戰報，包含庫存與監控名單。
**執行動作**：
1.  **庫存診斷 (最高優先)**：
    * 針對 **庫存股**，若出現「💀出貨」或「❌誘多」，直接給出**防守價** (例如 VWAP 下方)。
    * 若庫存股訊號正常，簡述「續抱」或「移動停利」。
2.  **戰術掃描**：
    * **09:00~13:00**：從名單中找出 **「💣 伏擊」** 或 **「🔥 攻擊」** 的標的，建議進場點。
    * **13:00~13:25**：特別標註符合 **「🔥 尾盤」** 條件的標的，建議隔日沖佈局。
3.  **無訊號處理**：若名單中皆為「盤整」或無明確訊號，請直接回報「目前戰場平靜，無高勝率獵物，建議觀望」。

---

### 三、 回答格式要求
1.  **語氣**：果斷、精簡條列式。
2.  **盤中輸出範例**：
    > **⏰ {現在時間} 戰情快報**
    > **📦 庫存**：
    > * 2330 台積電：🔥 攻擊中，續抱，停利設 1080。
    > **🎯 獵殺機會**：
    > * 3017 奇鋐：出現 💣 伏擊訊號，現價 680 貼近均價，大戶 1H 翻紅，建議試單。
"""

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
    net_10m: int
    net_1h: int
    net_day: int
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
# 3. Database (Block 1 & 2 Separation)
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
        c.execute('''CREATE TABLE IF NOT EXISTS realtime (code TEXT PRIMARY KEY, name TEXT, category TEXT, price REAL, pct REAL, vwap REAL, vol REAL, est_vol REAL, ratio REAL, net_1h REAL, net_10m REAL, net_day REAL, signal TEXT, update_time REAL, data_status TEXT DEFAULT 'DATA_OK', signal_level TEXT DEFAULT 'B', risk_status TEXT DEFAULT 'NORMAL')''')
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (code TEXT PRIMARY KEY, cost REAL, qty REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS watchlist (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pinned (code TEXT PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS static_info (code TEXT PRIMARY KEY, win REAL, ret REAL, yoy REAL, eps REAL, pe REAL, avg_vol REAL DEFAULT 0)''')
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
        sql = '''INSERT OR REPLACE INTO realtime (code, name, category, price, pct, vwap, vol, est_vol, ratio, net_1h, net_day, signal, update_time, data_status, signal_level, risk_status, net_10m) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        self.write_queue.put(('executemany', sql, data_list))

    def upsert_static(self, data_list):
        sql = 'INSERT OR REPLACE INTO static_info (code, win, ret, yoy, eps, pe, avg_vol) VALUES (?, ?, ?, ?, ?, ?, ?)'
        self.write_queue.put(('executemany', sql, data_list))

    def update_base_vol(self, code, vol):
        if vol is None or vol <= 0: return
        sql = 'UPDATE static_info SET avg_vol = ? WHERE code = ?'
        self.write_queue.put(('execute', sql, (vol, code)))

    def update_pinned(self, code, is_pinned):
        if is_pinned: self.write_queue.put(('execute', 'INSERT OR IGNORE INTO pinned (code) VALUES (?)', (code,)))
        else: self.write_queue.put(('execute', 'DELETE FROM pinned WHERE code = ?', (code,)))

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
        query = '''SELECT w.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal as event_label, r.net_10m, r.net_1h, r.net_day, s.yoy, s.eps, s.pe, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status, r.signal_level FROM watchlist w LEFT JOIN realtime r ON w.code = r.code LEFT JOIN static_info s ON w.code = s.code LEFT JOIN pinned p ON w.code = p.code'''
        df = pd.read_sql(query, conn)
        conn.close(); return df

    def get_inventory_view(self):
        conn = self._get_conn()
        query = '''SELECT i.code, r.name, r.pct, r.price, r.vwap, r.ratio, r.signal as event_label, r.net_1h, r.net_day, s.yoy, s.eps, s.pe, i.cost, i.qty, (r.price - i.cost) * i.qty * 1000 as profit_val, (r.price - i.cost) / i.cost * 100 as profit_pct, CASE WHEN p.code IS NOT NULL THEN 1 ELSE 0 END as is_pinned, r.data_status, r.risk_status FROM inventory i LEFT JOIN realtime r ON i.code = r.code LEFT JOIN static_info s ON i.code = s.code LEFT JOIN pinned p ON i.code = p.code'''
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
        c.execute('SELECT code, avg_vol FROM static_info')
        rows = c.fetchall(); conn.close()
        return {r[0]: r[1] for r in rows if r[1] and r[1] > 0}

db = Database(DB_PATH)

# ==========================================
# 4. Utilities
# ==========================================
def format_number(x, decimals=2, *, pos_color="#ff4d4f", neg_color="#2ecc71", zero_color="#e0e0e0", threshold=None, threshold_color="#ff4d4f", suffix=""):
    try:
        if pd.isna(x) or x == "" or x is None: return ""
        if isinstance(x, str) and "<span" in x: return x
        val = float(str(x).replace(",", "").replace("%", ""))
        if decimals == 0: text = f"{int(val)}{suffix}"
        else: text = f"{val:.{decimals}f}{suffix}"
        if threshold is not None and val >= threshold: return f"<span style='color:{threshold_color}'>{text}</span>"
        if val > 0: return f"<span style='color:{pos_color}'>{text}</span>"
        elif val < 0: return f"<span style='color:{neg_color}'>{text}</span>"
        else: return f"<span style='color:{zero_color}'>{text}</span>"
    except: return str(x)

def fetch_5ma_volume_hybrid(client, code):
    try:
        suffix = ".TW"
        if code in twstock.codes and twstock.codes[code].market == '上櫃': suffix = ".TWO"
        hist = yf.Ticker(f"{code}{suffix}").history(period="10d")
        if not hist.empty and len(hist) >= 5:
            avg_shares = hist['Volume'].tail(5).mean()
            return int(avg_shares) // 1000
    except: pass

    try:
        candles = client.stock.historical.candles(symbol=code, timeframe="D", limit=5)
        if candles and 'data' in candles and len(candles['data']) >= 5:
            total_shares = sum(int(c['volume']) for c in candles['data'])
            avg_shares = total_shares / 5
            return int(avg_shares) // 1000
    except: pass

    return None

def fetch_fundamental_data(code):
    suffix = ".TW"
    try:
        if code in twstock.codes:
            if twstock.codes[code].market == '上櫃': suffix = ".TWO"
    except: pass
    try:
        ticker = yf.Ticker(f"{code}{suffix}")
        info = ticker.info
        if info and 'symbol' in info:
            yoy = (info.get('revenueGrowth', 0) or 0) * 100
            eps = info.get('trailingEps', 0) or 0
            pe = info.get('trailingPE', 0) or 0
            return yoy, eps, pe
    except: pass
    return 0, 0, 0

def get_stock_name(symbol):
    try: return twstock.codes[symbol].name if symbol in twstock.codes else symbol
    except: return symbol

def get_dynamic_thresholds(price):
    if price >= 1200: return 2.0, 1.2, 1.8
    elif price >= 1000: return 2.0, 1.2, 2.0
    elif price >= 800:  return 2.0, 1.2, 2.5
    elif price >= 650:  return 2.0, 1.2, 3.0
    elif price >= 500:  return 2.0, 1.2, 4.0
    elif price >= 300:  return 2.0, 1.2, 5.0
    elif price >= 150:  return 2.5, 1.5, 6.0
    elif price >= 50:   return 2.5, 1.5, 8.0
    else:               return 3.5, 2.5, 10.0

def _calc_est_vol(current_vol):
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now < market_open: return 0
    elapsed_minutes = (now - market_open).seconds / 60
    if elapsed_minutes <= 0: return 0
    if elapsed_minutes >= 270: return current_vol
    return int(current_vol * (270 / elapsed_minutes))

def check_signal(pct, is_bullish, net_day, net_1h, ratio, tgt_pct, tgt_ratio, ambush_ratio, is_breakdown, price, vwap, has_attacked, now_time, vol_lots):
    if pct >= 9.5: return "漲停"

    # [NEW] End Game Strategy
    if dt_time(13, 0) <= now_time.time() <= dt_time(13, 25):
        if (3.0 <= pct <= 9.0) and (price > vwap) and (net_1h > 0):
             if vol_lots > 0 and (net_day / vol_lots >= 0.05):
                 return "尾盤"

    if ratio > 0:
        if (ratio >= ambush_ratio) and (abs(price - vwap) / vwap <= 0.01) and (pct <= 2.0) and (net_1h > 0) and (not has_attacked): return "伏擊"
        if is_bullish and net_day > 200 and pct >= tgt_pct and ratio >= tgt_ratio: return "攻擊"
        if ratio >= tgt_ratio and pct < tgt_pct and is_bullish and net_1h > 200: return "量增"
        if is_breakdown and ratio >= tgt_ratio and net_1h < 0: return "出貨"
    if pct > 2.0 and net_1h < 0: return "誘多"
    if is_bullish and pct >= tgt_pct: return "價強"
    return "盤整"

def get_market_snapshot():
    # Merge Watchlist and Inventory for AI
    try:
        df_watch = db.get_watchlist_view()
        df_inv = db.get_inventory_view()

        # Combined Dataframe
        df_combined = pd.concat([df_watch, df_inv], ignore_index=True).drop_duplicates(subset=['code'])

        if not df_combined.empty:
            # Sort by abs(pct)
            df_combined['abs_pct'] = df_combined['pct'].abs()
            df_combined = df_combined.sort_values(by='abs_pct', ascending=False)

            # Limit to 40
            if len(df_combined) > 40:
                df_combined = df_combined.head(40)

            # Select Columns: Code, Name, Price, Pct, Signal, Net_1H, Net_Day
            out_cols = ['code', 'name', 'price', 'pct', 'event_label', 'net_1h', 'net_day']
            # Handle missing cols
            for c in out_cols:
                if c not in df_combined.columns:
                    df_combined[c] = None

            csv_data = df_combined[out_cols].copy()
            csv_data.columns = ['Code', 'Name', 'Price', 'Pct', 'Signal', 'Net_1H', 'Net_Day']
            return csv_data.to_csv(index=False)
    except: pass

    return "No Data"

def call_ai_commander(snapshot_text):
    if not snapshot_text or snapshot_text == "No Data":
        return

    # Get API Key
    try:
        api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
    except:
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        error_msg = "❌ OpenAI API Key Not Found"
        try:
             if "ai_log" not in st.session_state: st.session_state.ai_log = []
             st.session_state.ai_log.insert(0, {"time": datetime.now().strftime('%H:%M:%S'), "content": error_msg})
        except: pass
        return

    try:
        current_time_str = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%H:%M')
        user_content = f"現在時間：{current_time_str}\n\n數據快照：\n{snapshot_text}"

        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": AI_COMMANDER_PROMPT},
                {"role": "user", "content": user_content}
            ]
        )
        content = response.choices[0].message.content

        # Save Log (Try best effort for session state)
        try:
            if "ai_log" not in st.session_state: st.session_state.ai_log = []
            st.session_state.ai_log.insert(0, {"time": datetime.now().strftime('%H:%M:%S'), "content": content})
        except: pass

        # Send Telegram
        if TG_BOT_TOKEN and TG_CHAT_ID:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                    data={"chat_id": TG_CHAT_ID, "text": f"🤖 **AI 指揮官**\n\n{content}", "parse_mode": "Markdown"},
                    timeout=5
                )
            except: pass

    except Exception as e:
        err_msg = f"AI Error: {str(e)}"
        try:
             if "ai_log" not in st.session_state: st.session_state.ai_log = []
             st.session_state.ai_log.insert(0, {"time": datetime.now().strftime('%H:%M:%S'), "content": err_msg})
        except: pass

# ==========================================
# 5. Notification
# ==========================================
class NotificationManager:
    COOLDOWN_SECONDS = 600
    RATE_LIMIT_DELAY = 1.0
    EMOJI_MAP = {
        "攻擊": "🚀", "伏擊": "💣", "量增": "👀",
        "出貨": "💀", "跌破": "⚠️", "漲停": "👑",
        "價強": "💪", "誘多": "🎣", "尾盤": "🔥"
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
        key = f"{event.code}_{event.scope}_{event.event_label}"
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
        market_label = "上市"
        try:
            if event.code in twstock.codes: market_label = twstock.codes[event.code].market
        except: pass
        msg = (f"<b>{emoji} {event.event_label}｜{event.code} {event.name} ({market_label})</b>\n"
               f"現價：{event.price:.2f} ({event.pct:.2f}% {up_dn})　均價：{event.vwap:.2f}\n"
               f"大戶10M：{event.net_10m}　大戶1H：{event.net_1h}　大戶(日)：{event.net_day}")
        buttons = [[{"text": "📈 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=TWSE%3A{event.code}&interval=1"},
                    {"text": "📊 Yahoo", "url": f"https://tw.stock.yahoo.com/quote/{event.code}.TW"}]]
        try: requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "reply_markup": json.dumps({"inline_keyboard": buttons})}, timeout=5)
        except: pass

notification_manager = NotificationManager()

# ==========================================
# 6. Engine (Stable Market Thermometer)
# ==========================================
class SniperEngine:
    def __init__(self):
        self.running = False
        self.clients = [RestClient(api_key=k) for k in API_KEYS] if API_KEYS else []
        self.client_cycle = cycle(self.clients) if self.clients else None
        self.targets = []
        self.inventory_codes = []
        # [BLOCK 1] Bunker Cache
        self.base_vol_cache = {}
        # [BLOCK 2] Live Data
        self.daily_net = {}
        self.vol_queues = {}
        self.prev_data = {}
        self.active_flags = {}
        self.daily_risk_flags = {}
        # [BLOCK 3] Market Stats - Init Time to 0 to force immediate update
        self.market_stats = {"TSE": 0, "OTC": 0, "Time": 0}

        self.last_reset = datetime.now().date()
        self.last_ai_trigger_time = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

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
        threading.Thread(target=self._worker_volume_backfill, daemon=True).start()

    def stop(self): self.running = False

    def _worker_volume_backfill(self):
        while self.running:
            if not self.targets:
                time.sleep(5); continue
            for code in self.targets:
                if not self.running: break
                if code in self.base_vol_cache and self.base_vol_cache[code] > 0:
                    continue
                try:
                    client = next(self.client_cycle) if self.client_cycle else None
                    if client:
                        vol = fetch_5ma_volume_hybrid(client, code)
                        if vol and vol > 0:
                            self.base_vol_cache[code] = vol
                            db.update_base_vol(code, vol)
                except: pass
                time.sleep(1.0)
            time.sleep(10)

    def _update_market_thermometer(self):
        """[BLOCK 3] Robust Fetch: Fugle -> Yahoo Backup"""
        if time.time() - self.market_stats["Time"] < 15: return

        tse_val = 0
        otc_val = 0

        # 1. Try Fugle First
        try:
            client = next(self.client_cycle) if self.client_cycle else None
            if client:
                q_tse = client.stock.intraday.quote(symbol="IX0001")
                q_otc = client.stock.intraday.quote(symbol="IX0043")
                tse_val = q_tse.get('total', {}).get('tradeValue', 0)
                otc_val = q_otc.get('total', {}).get('tradeValue', 0)
        except: pass

        # 2. Backup: Yahoo Finance (If Fugle fails or returns 0)
        if tse_val == 0 or otc_val == 0:
            try:
                # Yahoo: ^TWII (TSE), ^TWO (OTC)
                tickers = yf.Tickers("^TWII ^TWO")
                if tse_val == 0:
                    try:
                        # Fallback to history for last close if intraday fails
                        hist = tickers.tickers['^TWII'].history(period="1d")
                        if not hist.empty:
                            # Note: Yahoo volume for indices is often 0 or shares, not value.
                            # We use a rough estimate if needed, but here just keep 0 if unknown.
                            pass
                    except: pass
            except: pass

        # Update State (Even if 0, to reset timer)
        old_tse = self.market_stats.get("TSE", 0)
        old_otc = self.market_stats.get("OTC", 0)

        final_tse = tse_val if tse_val > 0 else old_tse
        final_otc = otc_val if otc_val > 0 else old_otc

        self.market_stats = {
            "TSE": final_tse,
            "OTC": final_otc,
            "Time": time.time()
        }

    def _dispatch_event(self, ev: SniperEvent):
        notification_manager.enqueue(ev)

    def _fetch_stock(self, code, now_time=None):
        try:
            if now_time is None: now_time = datetime.now(timezone.utc) + timedelta(hours=8)
            client = next(self.client_cycle) if self.client_cycle else None
            if not client: return None

            base_vol = self.base_vol_cache.get(code, 0)

            q = client.stock.intraday.quote(symbol=code)
            price = q.get('lastPrice', 0)
            if not price: return None

            pct = q.get('changePercent', 0)
            vol_lots = q.get('total', {}).get('tradeVolume', 0)

            if vol_lots == 0 and code in self.prev_data:
                vol_lots = self.prev_data[code]['vol']

            vol = vol_lots * 1000

            est_lots = _calc_est_vol(vol_lots)
            ratio = est_lots / base_vol if base_vol > 0 else 0

            total_val = q.get('total', {}).get('tradeValue', 0)
            vwap = (total_val / vol) if vol > 0 else price

            delta_net = 0
            if code in self.prev_data:
                prev_v = self.prev_data[code]['vol']
                prev_p = self.prev_data[code]['price']
                delta_v = vol_lots - prev_v
                threshold = max(1, int(400/price)) if price > 0 else 5
                if delta_v >= threshold:
                    if price >= prev_p: delta_net = int(delta_v)
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

            tgt_pct, tgt_ratio, ambush_ratio = get_dynamic_thresholds(price)

            raw_state = check_signal(pct, price >= vwap, net_day, net_1h, ratio, tgt_pct, tgt_ratio, ambush_ratio, price < vwap*0.99, price, vwap, code in self.active_flags, now_time, vol_lots)

            event_label = None
            scope = "inventory" if code in self.inventory_codes else "watchlist"

            if "攻擊" in raw_state and code not in self.active_flags: event_label = "攻擊"
            elif "漲停" in raw_state and scope == "inventory": event_label = "漲停"
            elif "出貨" in raw_state and code not in self.daily_risk_flags and scope == "inventory": event_label = "出貨"
            elif "伏擊" in raw_state and scope == "watchlist": event_label = "伏擊"
            elif "尾盤" in raw_state: event_label = "尾盤"

            if event_label:
                if event_label == "攻擊": self.active_flags[code] = True
                if event_label == "出貨": self.daily_risk_flags[code] = True
                ev = SniperEvent(
                    code=code, name=get_stock_name(code), scope=scope,
                    event_kind="STRATEGY", event_label=event_label,
                    price=price, pct=pct, vwap=vwap, ratio=ratio,
                    net_10m=net_10m, net_1h=net_1h, net_day=net_day
                )
                self._dispatch_event(ev)

            return (code, get_stock_name(code), "一般", price, pct, vwap, vol_lots, est_lots, ratio, net_1h, net_day, raw_state, now_ts, "DATA_OK", "B", "NORMAL", net_10m)
        except: return None

    def _run_loop(self):
        while self.running:
            now = datetime.now(timezone.utc) + timedelta(hours=8)
            if now.date() > self.last_reset:
                self.active_flags = {}
                self.daily_risk_flags = {}
                self.daily_net = {}
                self.prev_data = {}
                self.vol_queues = {}
                self.base_vol_cache = {}
                notification_manager.reset_daily_state()
                self.last_reset = now.date()

            # [BLOCK 3 Update]
            current_tse = self.market_stats.get("TSE", 0)
            if MarketSession.is_market_open(now) or current_tse == 0:
                self._update_market_thermometer()

            # [AI Scheduler]
            ai_schedule = ['09:06', '09:30', '10:00', '10:25', '10:50', '11:15', '11:40', '12:05', '12:30', '13:00', '13:25']
            now_hm = now.strftime('%H:%M')
            if now_hm in ai_schedule and self.last_ai_trigger_time != now_hm:
                self.last_ai_trigger_time = now_hm
                snapshot = get_market_snapshot()
                t = threading.Thread(target=call_ai_commander, args=(snapshot,))
                add_script_run_ctx(t)
                t.start()

            targets = db.get_all_codes()
            self.inventory_codes = db.get_inventory_codes()
            pinned_codes = db.get_pinned_codes()

            if not targets: time.sleep(2); continue

            # [PRIORITY FIX]
            inv_set = set(self.inventory_codes)
            pin_set = set(pinned_codes)

            def priority_key(code):
                if code in inv_set: return 0
                if code in pin_set: return 1
                return 2

            targets.sort(key=priority_key)

            batch = []
            futures = [self.executor.submit(self._fetch_stock, c, now) for c in targets]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): batch.append(f.result())
            db.upsert_realtime_batch(batch)
            time.sleep(3 if MarketSession.is_market_open(now) else 10)

if "sniper_engine_core" not in st.session_state:
    st.session_state.sniper_engine_core = SniperEngine()
engine = st.session_state.sniper_engine_core

# ==========================================
# 7. UI (Layout & Fragments)
# ==========================================
class LegacyDispatcher:
    def dispatch(self, event_dict):
        ev = SniperEvent(
            code=event_dict['code'], name=event_dict['name'], scope=event_dict['scope'],
            event_kind=event_dict.get('event_kind', 'TEST'), event_label=event_dict['event_label'],
            price=event_dict['price'], pct=event_dict['pct'], vwap=event_dict.get('vwap', 0),
            ratio=event_dict['ratio'], net_1h=event_dict['net_1h'], net_10m=event_dict['net_10m'],
            net_day=event_dict['net_day'], timestamp=event_dict['timestamp'], is_test=event_dict.get('is_test', False)
        )
        notification_manager.enqueue(ev)

dispatcher = LegacyDispatcher()

with st.sidebar:
    st.title("🛡️ 戰情室 v5.41")

    # --- [TOP] Market Thermometer ---
    st.subheader("🌡️ 大盤溫度計")

    tse_val = engine.market_stats.get("TSE", 0)
    otc_val = engine.market_stats.get("OTC", 0)

    est_tse = int(_calc_est_vol(tse_val) / 100000000)
    est_otc = int(_calc_est_vol(otc_val) / 100000000)

    st.metric("上市預估量 (億)", f"{est_tse}", delta=None, help=">4000億:熱, <2500億:冷")
    if est_tse >= 4000: st.caption("🔥 資金狂潮 (攻擊)")
    elif est_tse <= 2500 and est_tse > 0: st.caption("🧊 量能急凍 (盤整)")
    elif est_tse == 0: st.caption("⏳ 數據讀取中...")
    else: st.caption("☁️ 溫和換手 (中性)")

    st.divider()

    st.metric("上櫃預估量 (億)", f"{est_otc}", delta=None)
    if est_otc >= 1200: st.caption("🔥 中小噴發")
    elif est_otc <= 600 and est_otc > 0: st.caption("🧊 內資縮手")
    elif est_otc == 0: st.caption("⏳ 數據讀取中...")
    else: st.caption("☁️ 正常輪動")

    st.markdown("---")

    # --- AI Commander Log ---
    with st.expander("🤖 AI 指揮官日誌", expanded=True):
        if "ai_log" not in st.session_state:
            st.session_state.ai_log = []

        if st.button("⚡ 強制呼叫指揮官"):
            snapshot = get_market_snapshot()
            t = threading.Thread(target=call_ai_commander, args=(snapshot,))
            add_script_run_ctx(t)
            t.start()
            st.toast("指揮官連線中...")

        if st.session_state.ai_log:
            latest = st.session_state.ai_log[0]
            st.markdown(f"**⏰ {latest['time']}**")
            st.info(latest['content'])

            with st.expander("📜 歷史戰報"):
                for log in st.session_state.ai_log[1:]:
                    st.text(f"[{log['time']}]")
                    st.markdown(log['content'])
                    st.divider()
        else:
            st.caption("尚無戰報")

    st.markdown("---")

    # --- [BOTTOM] Controls ---
    mode = st.radio("身分模式", ["👀 戰情官", "👨‍✈️ 指揮官"])
    st.subheader("🔍 濾網設定")
    use_filter = st.checkbox("只看基本面良好")

    if mode == "👨‍✈️ 指揮官":
        with st.expander("📦 庫存管理 (Inventory)", expanded=False):
            inv_input = st.text_area("庫存清單 (代碼,成本,張數)", DEFAULT_INVENTORY, height=100)
            if st.button("更新庫存"):
                db.update_inventory_list(inv_input)
                time.sleep(0.5)
                engine.update_targets()
                st.toast("庫存已更新！")
                st.rerun()

        with st.expander("🔭 監控設定 (Watchlist)", expanded=True):
            raw_input = st.text_area("新選清單", DEFAULT_WATCHLIST, height=150)
            if st.button("1. 初始化並更新清單", type="primary"):
                if not API_KEYS: st.error("缺 API Key")
                else:
                    db.update_watchlist(raw_input)
                    time.sleep(0.5)
                    engine.update_targets()
                    targets = engine.targets
                    status = st.status("正在初始化數據 (含5日均量)...", expanded=True)
                    static_list = []
                    progress_bar = status.progress(0)

                    for i, code in enumerate(targets):
                        current_key = API_KEYS[i % len(API_KEYS)]
                        client = RestClient(api_key=current_key)

                        yoy, eps, pe = fetch_fundamental_data(code)
                        vol = fetch_5ma_volume_hybrid(client, code)
                        if vol and vol > 0:
                            db.update_base_vol(code, vol)
                        else:
                            vol = 0

                        static_list.append((code, 0, 0, yoy, eps, pe, vol))
                        progress_bar.progress((i + 1) / len(targets))
                        time.sleep(0.1)

                    db.upsert_static(static_list)
                    engine.update_targets()
                    status.update(label="初始化完成！", state="complete")
                    st.rerun()

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🟢 啟動監控", disabled=engine.running):
                    engine.start()
                    st.toast("核心已啟動")
                    st.rerun()
            with col_b:
                if st.button("🔴 停止監控", disabled=not engine.running):
                    engine.stop()
                    st.toast("核心已停止")
                    st.rerun()

    st.caption(f"Engine: {'🟢 RUNNING' if engine.running else '🔴 STOPPED'}")

    st.subheader("🧪 系統測試")
    if st.button("🔥 測試攻擊"):
        dispatcher.dispatch({
            "code": "2330", "name": "台積電 (測試)", "scope": "watchlist",
            "event_kind": "TEST", "event_label": "攻擊",
            "price": 888.0, "pct": 3.5, "vwap": 870.0, "ratio": 2.5, "net_10m": 150, "net_1h": 500, "net_day": 1200,
            "timestamp": time.time(), "is_test": True
        })
        st.toast("測試訊號已發送")

# --- Safe Fragment Fallback ---
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
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    st.caption(f"⚡ Live Refresh: {now.strftime('%H:%M:%S')} (Rate: 1.5s)")

    # --- Part 1: Inventory (Top - Editor Mode) ---
    st.subheader("📦 庫存損益")
    df_inv = db.get_inventory_view()
    if not df_inv.empty:
        df_inv = df_inv.rename(columns={'net_1h': '大戶1H', 'net_day': '大戶日', 'ratio': '量比', 'vwap': '均價', 'pct': '漲跌%', 'price': '現價', 'code': '代碼', 'name': '名稱', 'event_label': '訊號', 'yoy': '營收YoY', 'eps': 'EPS', 'pe': 'PE', 'cost': '成本', 'profit_val': '損益$', 'profit_pct': '報酬%', 'risk_status': '狀態'})
        cols = ['代碼', '名稱', '狀態', '漲跌%', '現價', '均價', '量比', '訊號', '大戶1H', '大戶日', '營收YoY', 'EPS', 'PE', '成本', '損益$', '報酬%']
        for c in cols:
            if c not in df_inv.columns: df_inv[c] = 0
        df_inv_show = df_inv[cols].copy()

        for col in ['營收YoY', 'EPS', 'PE']:
            df_inv_show[col] = df_inv_show[col].fillna(0).infer_objects(copy=False)

        st.data_editor(
            df_inv_show,
            column_config={
                "代碼": st.column_config.TextColumn("代碼", width="small", pinned=True),
                "名稱": st.column_config.TextColumn("名稱", pinned=True),
                "狀態": st.column_config.TextColumn("狀態", width="small"),
                "成本": st.column_config.NumberColumn("成本", format="%.2f"),
                "現價": st.column_config.NumberColumn("現價", format="%.2f"),
                "漲跌%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "均價": st.column_config.NumberColumn("均價", format="%.2f"),
                "量比": st.column_config.NumberColumn("量比", format="%.1f"),
                "損益$": st.column_config.NumberColumn("損益$", format="%d"),
                "報酬%": st.column_config.NumberColumn("報酬%", format="%.2f%%"),
                "營收YoY": st.column_config.NumberColumn("營收YoY", format="%.1f%%"),
                "EPS": st.column_config.NumberColumn("EPS", format="%.2f"),
                "PE": st.column_config.NumberColumn("PE", format="%.1f")
            },
            width='stretch',
            hide_index=True, disabled=True, key="inv_table_live"
        )
    else: st.info("尚無庫存資料")

    st.markdown("---")

    # --- Part 2: Watchlist (Manual HTML + Multiselect) ---
    st.subheader("🔭 監控雷達")
    df_watch = db.get_watchlist_view()

    if not df_watch.empty:
        # [CRITICAL FIX] Data Sanitization: Ensure numeric columns are floats, not None
        numeric_cols = ['price', 'pct', 'vwap', 'ratio', 'net_10m', 'net_1h', 'net_day', 'yoy', 'eps', 'pe']
        for col in numeric_cols:
            if col in df_watch.columns:
                df_watch[col] = pd.to_numeric(df_watch[col], errors='coerce').fillna(0.0)

        all_options = df_watch['code'].tolist()
        current_pinned = df_watch[df_watch['is_pinned'] == 1]['code'].tolist()

        new_pinned = st.multiselect(
            "📌 快速釘選 (選中即變色)",
            options=all_options,
            default=current_pinned,
            format_func=lambda x: f"{x} {get_stock_name(x)}",
            key="pinned_multiselect"
        )

        if set(new_pinned) != set(current_pinned):
            for code in all_options:
                is_p = 1 if code in new_pinned else 0
                was_p = 1 if code in current_pinned else 0
                if is_p != was_p:
                    db.update_pinned(code, is_p)
            st.rerun()

        df_watch['Pinned'] = df_watch['code'].isin(new_pinned)

        for col in ['net_10m', 'net_1h', 'net_day']:
            if col not in df_watch.columns: df_watch[col] = 0
            df_watch[col] = df_watch[col].fillna(0).infer_objects(copy=False)

        df_watch['yoy'] = df_watch['yoy'].fillna(0).infer_objects(copy=False)
        df_watch['eps'] = df_watch['eps'].fillna(0).infer_objects(copy=False)

        if use_filter:
            df_watch = df_watch[(df_watch['yoy'] > 0) & (df_watch['eps'] > 0) & (df_watch['pe'].notna()) & (df_watch['pe'] < 50)]

        def get_ratio_html(val):
            try:
                v = float(val)
                if v >= 10: return f"<span style='color:#ff4d4f; font-weight:bold'>{v:.1f}</span>"
                if v > 0: return f"{v:.1f}"
                return "<span style='color:#cccccc'>-</span>"
            except: return "-"

        # [FIX] Helper function for independent Big Player coloring
        def bp_span(val):
            try:
                v = int(val)
                if v > 0: return f"<span style='color:#ff4d4f'>{v}</span>"
                elif v < 0: return f"<span style='color:#2ecc71'>{v}</span>"
                else: return f"<span style='color:#999999'>0</span>"
            except: return "<span style='color:#999999'>-</span>"

        # --- Build HTML Table Manually (No Indentation) ---
        table_start = """
<style>
table.sniper-table { width: 100%; border-collapse: collapse; font-family: monospace; }
table.sniper-table th { text-align: left; background-color: #262730; color: white; padding: 8px; font-size: 14px; }
table.sniper-table td { padding: 8px; border-bottom: 1px solid #444; font-size: 14px; }
table.sniper-table tr.pinned-row { background-color: #f5f0c8 !important; color: black !important; }
table.sniper-table tr.pinned-row span { font-weight: bold; }
table.sniper-table tr:hover { background-color: #f0f2f6; color: black; }
</style>
<table class="sniper-table">
<thead>
<tr>
<th>📌</th><th>代碼</th><th>名稱</th><th>等級</th><th>現價</th><th>漲跌%</th>
<th>均價</th><th>量比</th><th>訊號</th><th>大戶(10m/1H/日)</th><th>營收YoY</th><th>EPS</th><th>PE</th>
</tr>
</thead>
<tbody>
"""
        html_rows = []
        for _, row in df_watch.iterrows():
            row_class = "pinned-row" if row['Pinned'] else ""
            pin_icon = "📌" if row['Pinned'] else ""

            # [LOGIC FIX] Price color purely based on PCT (Independent of Pinned)
            if row['pct'] > 0:
                main_color = "#ff4d4f"
            elif row['pct'] < 0:
                main_color = "#2ecc71"
            else:
                main_color = "#999999" # Neutral

            price_html = f"<span style='color:#000000'>{row['price']:.2f}</span>"
            pct_html = f"<span style='color:{main_color}'>{row['pct']:.2f}%</span>"

            # VWAP Logic: Red if Price > VWAP, Black if Price <= VWAP
            if row['price'] > row['vwap']: vwap_color = "#ff4d4f"
            else: vwap_color = "#000000"

            vwap_html = f"<span style='color:{vwap_color}'>{row['vwap']:.2f}</span>"

            ratio_html = get_ratio_html(row['ratio'])

            # [LOGIC FIX] Independent Big Player Coloring
            big_player = f"{bp_span(row['net_10m'])} / {bp_span(row['net_1h'])} / {bp_span(row['net_day'])}"

            html_rows.append(f'<tr class="{row_class}"><td>{pin_icon}</td><td>{row["code"]}</td><td>{row["name"]}</td><td>{row["signal_level"]}</td><td>{price_html}</td><td>{pct_html}</td><td>{vwap_html}</td><td>{ratio_html}</td><td>{row["event_label"]}</td><td>{big_player}</td><td>{row["yoy"]:.1f}%</td><td>{row["eps"]:.2f}</td><td>{row["pe"]:.1f}</td></tr>')

        final_html = table_start + "".join(html_rows) + "</tbody></table>"
        st.markdown(final_html, unsafe_allow_html=True)

    else: st.info("尚無監控資料")

# Render the fragment
render_live_dashboard()
