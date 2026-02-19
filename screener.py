#!/usr/bin/env python3
"""
SCREENER TRADER INDONESIA - VERSI FINAL + SMC + ENTRY ZONE
✅ DOWNLOAD: 4 Timeframe (1w, 1d, 1h, 15m) untuk analisis hierarkis
✅ FILTER: Hanya hari dengan volume > 0 (hapus noise sebelum listing)
✅ STRATEGI: Pullback dalam UPTREND (harga > MA50) + SMC Confirmation
✅ SMC: Order Block + BOS + FVG Detection (Multi-Timeframe)
✅ ENTRY: Zone Entry (3 Cicil: 20%, 50%, 30%) dengan Average Entry
✅ FUNDAMENTAL: OPTIONAL (bisa ON/OFF via CONFIG)
✅ OUTPUT: Max 7 saham — HANYA "✅ READY"
✅ RISK MANAGEMENT: SL Struktural, RR minimal 1:3
✅ PEMBULATAN: 100% sesuai fraksi BEI
✅ JSON COMPATIBLE: Entry (int) + Entry_Zone (string) untuk API
✅ CONCURRENT DOWNLOAD (5 workers) + Local Cache (Parquet) + Audit Report
✅ GITHUB ACTIONS READY: No input(), Auto-exit, JSON API Endpoint
"""
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import time
import os
import sys
import re
import json
from tabulate import tabulate
from colorama import init, Fore, Style
import gc
import threading
import warnings
warnings.filterwarnings('ignore')
init(autoreset=True)

# =============================================
# GLOBAL AUDIT TRACKER
# =============================================
SCREENING_AUDIT = {
    "total_processed": 0,
    "failed_liquidity": 0, "passed_liquidity": 0,
    "failed_uptrend": 0, "passed_uptrend": 0,
    "failed_pullback": 0, "passed_pullback": 0,
    "failed_volume": 0, "passed_volume": 0,
    "failed_fundamental": 0, "passed_fundamental": 0,
    "failed_smc": 0, "passed_smc": 0,
    "failed_rr": 0, "passed_rr": 0,
    "final_passed": 0,
    "detailed_failures": {}
}

# =============================================
# KONFIGURASI STRATEGI
# =============================================
CONFIG = {
    # --- Data & Download ---
    "MIN_TRADING_DAYS": 60,
    "MIN_VOLUME_VALUE": 500_000_000,
    "MIN_PRICE": 50,
    "MARKET_CAP_MIN": 2e12,
    "MARKET_CAP_MAX": 50e12,
    
    # --- Technical Indicators ---
    "MA_PERIOD": 50,
    "ATR_PERIOD": 14,
    "VOLUME_RATIO_MIN": 1.5,
    "PULLBACK_TOLERANCE": 0.05,
    "ATR_MULTIPLIER_SL": 2.0,
    "MIN_RR_RATIO": 3.0,
    "MAX_RISK_PCT": 0.08,
    
    # --- Fundamental (OPTIONAL) ---
    "FUNDAMENTAL_REQUIRED": False,
    "MIN_ROE": 10,
    "MAX_DE": 1.0,
    
    # --- SMC Configuration ---
    "SMC_SWING_WINDOW": 20,
    "SMC_OB_RALLY_MIN": 0.03,
    "SMC_FVG_MIN_GAP": 0.01,
    "SMC_REQUIRED": True,
    
    # --- Entry Zone Configuration ---
    "ENTRY_1_PCT": 0.20,  # 20% modal
    "ENTRY_2_PCT": 0.50,  # 50% modal
    "ENTRY_3_PCT": 0.30,  # 30% modal
    
    # --- Timeframe Configuration ---
    "TIMEFRAMES": ["1wk", "1d", "1h", "15m"],
    "PRIMARY_TF": "1d",      # Untuk struktur utama
    "ENTRY_TF": "1h",        # Untuk entry zone
    "PRECISION_TF": "15m",   # Untuk trigger entry
    
    # --- System Configuration ---
    "MAX_WORKERS": 5,
    "REQUEST_DELAY": 0.7,
    "JITTER_MIN": 0.2,
    "JITTER_MAX": 0.5,
    "MAX_RETRIES": 2,
    "BATCH_SIZE": 30,
    "BATCH_DELAY": 5,
    "DATA_DIR": "data_ohlc_v3",
    "MAX_LOCAL_AGE_DAYS": 1,
    "MIN_CACHE_RETENTION_DAYS": 365,
    "FORCE_DOWNLOAD": False,
    "EXCEL_PREFIX": "watchlist_trader_id_smc",
    "MAX_OUTPUT": 7
}
os.makedirs(CONFIG["DATA_DIR"], exist_ok=True)

# =============================================
# GLOBAL THREAD-SAFE OBJECTS
# =============================================
DOWNLOAD_SEMAPHORE = threading.Semaphore(CONFIG["MAX_WORKERS"])
LOG_LOCK = threading.Lock()
ADAPTIVE_THROTTLER = {"error_429_count": 0, "last_reset_time": time.time()}

# =============================================
# FUNGSI UTILITAS
# =============================================
def log_message(message, level="info"):
    with LOG_LOCK:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefix = {
            "success": f"{Fore.GREEN}[✓]",
            "warning": f"{Fore.YELLOW}[!]",
            "error": f"{Fore.RED}[✗]",
            "processing": f"{Fore.CYAN}[→]",
            "critical": f"{Fore.MAGENTA}[!]",
            "info": f"{Fore.WHITE}[i]"
        }.get(level, "[i]")
        print(f"{Style.BRIGHT}{timestamp}{Style.RESET_ALL} {prefix} {message}{Style.RESET_ALL}")

def get_tick_size(price):
    """Mengembalikan fraksi harga sesuai ketentuan BEI"""
    price = float(price)
    if price < 200: return 1
    elif price < 500: return 2
    elif price < 2000: return 5
    elif price < 5000: return 10
    else: return 25

def round_to_bei_tick(price):
    """Membulatkan harga sesuai fraksi BEI"""
    price = float(price)
    tick = get_tick_size(price)
    return round(price / tick) * tick

def calculate_entry_zone(entry_1, entry_2, entry_3):
    """
    Menghitung Entry Zone dan Average Entry
    Returns: dict dengan entry_1, entry_2, entry_3, average, zone_string
    """
    # Pastikan semua entry sesuai fraksi BEI
    e1 = int(round_to_bei_tick(entry_1))
    e2 = int(round_to_bei_tick(entry_2))
    e3 = int(round_to_bei_tick(entry_3))
    
    # Urutkan dari tertinggi ke terendah
    entries = sorted([e1, e2, e3], reverse=True)
    entry_high = entries[0]  # Entry 1 (tertinggi)
    entry_mid = entries[1]   # Entry 2 (tengah)
    entry_low = entries[2]   # Entry 3 (terendah)
    
    # Hitung Average Entry tertimbang
    average_entry = (
        entry_high * CONFIG["ENTRY_1_PCT"] +
        entry_mid * CONFIG["ENTRY_2_PCT"] +
        entry_low * CONFIG["ENTRY_3_PCT"]
    )
    average_entry = int(round_to_bei_tick(average_entry))
    
    # Buat string zone untuk tampilan
    zone_string = f"{entry_low} - {entry_high}"
    
    return {
        "entry_1": entry_high,
        "entry_2": entry_mid,
        "entry_3": entry_low,
        "average": average_entry,
        "zone": zone_string
    }

# =============================================
# MANAJEMEN DATA LOKAL (MULTI-TIMEFRAME)
# =============================================
def get_local_file_path(ticker, timeframe="1d"):
    clean_ticker = re.sub(r'[^A-Z0-9]', '', ticker.split('.')[0])
    today = datetime.now()
    year_month_dir = os.path.join(CONFIG["DATA_DIR"], str(today.year), f"{today.month:02d}")
    os.makedirs(year_month_dir, exist_ok=True)
    tf_short = timeframe.replace('m', 'min').replace('h', 'h').replace('d', 'd').replace('wk', 'w')
    filename = f"{clean_ticker}_{tf_short}_{today.strftime('%Y%m%d')}.parquet"
    return os.path.join(year_month_dir, filename)

def is_local_data_fresh(file_path, timeframe="1d"):
    if not os.path.exists(file_path): 
        return False
    if CONFIG["FORCE_DOWNLOAD"]:
        return False
    try:
        df_cache = pd.read_parquet(file_path)
        if df_cache.empty: 
            return False
        if 'date' not in df_cache.columns:
            return False
        
        last_data_date = df_cache['date'].iloc[-1]
        if isinstance(last_data_date, pd.Timestamp):
            last_data_date = last_data_date.date()
        
        today = datetime.now().date()
        days_diff = (today - last_data_date).days
        
        # Untuk intraday (1h, 15m), maksimal 1 hari
        # Untuk daily/weekly, maksimal 1 hari juga untuk akurasi
        max_age = CONFIG["MAX_LOCAL_AGE_DAYS"]
        is_fresh = days_diff <= max_age
        
        if not is_fresh:
            log_message(f"📁 Cache {os.path.basename(file_path)}: Data T-{days_diff}, perlu update", "warning")
        
        return is_fresh
    except Exception as e:
        log_message(f"📁 Error cek cache: {str(e)[:100]}", "error")
        return False

def save_ohlc_data(ticker, df, timeframe="1d"):
    try:
        file_path = get_local_file_path(ticker, timeframe)
        df['ticker'] = ticker
        df['timeframe'] = timeframe
        df['last_update'] = datetime.now()
        df.to_parquet(file_path, index=False)
        return True
    except Exception as e:
        log_message(f"{ticker}: ❌ Gagal simpan data {timeframe}: {str(e)}", "error")
        return False

def load_local_data(ticker, timeframe="1d"):
    file_path = get_local_file_path(ticker, timeframe)
    if CONFIG["FORCE_DOWNLOAD"]:
        return None
    if not os.path.exists(file_path) or not is_local_data_fresh(file_path, timeframe):
        return None
    try:
        df = pd.read_parquet(file_path)
        if len(df) < CONFIG["MIN_TRADING_DAYS"]: 
            return None
        return df
    except Exception as e:
        return None

# =============================================
# ADAPTIVE THROTTLING
# =============================================
def get_adaptive_delay():
    global ADAPTIVE_THROTTLER
    if time.time() - ADAPTIVE_THROTTLER["last_reset_time"] > 300:
        ADAPTIVE_THROTTLER["error_429_count"] = 0
        ADAPTIVE_THROTTLER["last_reset_time"] = time.time()
    base_delay = CONFIG["REQUEST_DELAY"]
    jitter = np.random.uniform(CONFIG["JITTER_MIN"], CONFIG["JITTER_MAX"])
    if ADAPTIVE_THROTTLER["error_429_count"] > 0:
        multiplier = 1.5 ** ADAPTIVE_THROTTLER["error_429_count"]
        adjusted_delay = base_delay * multiplier + jitter
        log_message(f"⚠️ Adaptive throttling aktif! Delay = {adjusted_delay:.2f}s", "warning")
        return adjusted_delay
    return base_delay + jitter

def record_throttling_error(error_msg):
    global ADAPTIVE_THROTTLER
    if "429" in str(error_msg) or "too many request" in str(error_msg).lower() or "rate limit" in str(error_msg).lower():
        ADAPTIVE_THROTTLER["error_429_count"] += 1
        log_message(f"🚨 Terdeteksi rate limit! Counter naik menjadi {ADAPTIVE_THROTTLER['error_429_count']}", "error")

# =============================================
# MULTI-TIMEFRAME DOWNLOAD
# =============================================
def download_multi_timeframe_data(ticker):
    """
    Download data untuk 4 timeframe: 1w, 1d, 1h, 15m
    Returns: dict dengan DataFrame untuk setiap timeframe
    """
    with DOWNLOAD_SEMAPHORE:
        ticker_clean = str(ticker).strip()
        data_dict = {}
        
        for tf in CONFIG["TIMEFRAMES"]:
            # Cek cache dulu
            local_data = load_local_data(ticker_clean, tf)
            if local_data is not None:
                data_dict[tf] = local_data
                continue
            
            # Download dari Yahoo Finance
            for attempt in range(CONFIG["MAX_RETRIES"]):
                try:
                    delay = get_adaptive_delay()
                    time.sleep(delay)
                    
                    # Yahoo Finance interval mapping
                    interval_map = {
                        "1wk": "1wk",
                        "1d": "1d",
                        "1h": "1h",
                        "15m": "15m"
                    }
                    interval = interval_map.get(tf, "1d")
                    
                    # Period mapping (intraday limited to 60 days)
                    if tf in ["15m", "1h"]:
                        period = "60d"
                    elif tf == "1d":
                        period = "max"
                    else:
                        period = "max"
                    
                    stock = yf.Ticker(ticker_clean)
                    df = stock.history(period=period, interval=interval, auto_adjust=True)
                    
                    if df.empty:
                        raise ValueError("Data kosong")
                    
                    df.reset_index(inplace=True)
                    df.columns = [col.lower() for col in df.columns]
                    if 'date' not in df.columns and 'datetime' in df.columns:
                        df.rename(columns={'datetime': 'date'}, inplace=True)
                    
                    # Filter volume > 0
                    df = df[df['volume'] > 0].copy()
                    
                    if len(df) < 10:  # Minimal data untuk intraday
                        log_message(f"{ticker_clean}: ⚠️ Data {tf} tidak lengkap ({len(df)} baris)", "warning")
                        continue
                    
                    # Standardisasi kolom
                    df['ticker'] = ticker_clean
                    df['timeframe'] = tf
                    
                    # Simpan ke cache
                    save_ohlc_data(ticker_clean, df, tf)
                    data_dict[tf] = df
                    
                    break
                    
                except Exception as e:
                    record_throttling_error(str(e))
                    wait_time = 1.5 ** attempt + np.random.uniform(0, 1)
                    if attempt < CONFIG["MAX_RETRIES"] - 1:
                        time.sleep(wait_time)
                    else:
                        log_message(f"{ticker_clean}: ❌ Gagal download {tf}: {str(e)[:80]}", "error")
        
        return data_dict if len(data_dict) >= 2 else None  # Minimal 2 timeframe

# =============================================
# SMC DETECTION FUNCTIONS (MULTI-TIMEFRAME)
# =============================================

def detect_smc_signals(df, timeframe="1d"):
    """
    Deteksi Smart Money Concepts signals pada dataframe
    Returns: dict dengan sinyal SMC
    """
    try:
        if df is None or len(df) < 10:
            return None
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # === 1. DETEKSI SWING HIGH/LOW ===
        window = CONFIG["SMC_SWING_WINDOW"]
        swing_high = df['high'].rolling(window=window).max().shift(1)
        swing_low = df['low'].rolling(window=window).min().shift(1)
        
        # === 2. DETEKSI BOS ===
        bos_bullish = bool(last['close'] > swing_high.iloc[-1]) if pd.notna(swing_high.iloc[-1]) else False
        bos_bearish = bool(last['close'] < swing_low.iloc[-1]) if pd.notna(swing_low.iloc[-1]) else False
        
        # === 3. DETEKSI CHoCH ===
        choch_bullish = False
        choch_bearish = False
        
        if len(df) >= 40:
            prev_swing_high = swing_high.iloc[-2] if len(swing_high) >= 2 else swing_high.iloc[-1]
            prev_swing_low = swing_low.iloc[-2] if len(swing_low) >= 2 else swing_low.iloc[-1]
            
            if pd.notna(prev_swing_high) and pd.notna(prev_swing_low):
                if last['close'] > prev_swing_high and prev['close'] < prev_swing_low:
                    choch_bullish = True
                elif last['close'] < prev_swing_low and prev['close'] > prev_swing_high:
                    choch_bearish = True
        
        # === 4. DETEKSI ORDER BLOCK ===
        ob_signal = False
        ob_low = None
        ob_high = None
        
        for i in range(max(0, len(df)-30), len(df)-1):
            if df['close'].iloc[i] > df['open'].iloc[i]:
                if i+3 < len(df):
                    rally = (df['close'].iloc[i+3] - df['close'].iloc[i]) / df['close'].iloc[i]
                    if rally > CONFIG["SMC_OB_RALLY_MIN"]:
                        ob_signal = True
                        ob_low = df['low'].iloc[i]
                        ob_high = df['high'].iloc[i]
                        break
        
        # === 5. DETEKSI FVG ===
        fvg_detected = False
        fvg_low = None
        fvg_high = None
        
        if len(df) >= 3:
            candle_1_high = df['high'].iloc[-3]
            candle_3_low = df['low'].iloc[-1]
            candle_1_low = df['low'].iloc[-3]
            candle_3_high = df['high'].iloc[-1]
            
            if candle_3_low > candle_1_high:
                gap_pct = (candle_3_low - candle_1_high) / candle_1_high
                if gap_pct >= CONFIG["SMC_FVG_MIN_GAP"]:
                    fvg_detected = True
                    fvg_low = candle_1_high
                    fvg_high = candle_3_low
        
        # === 6. CEK ZONA ===
        in_ob_zone = False
        if ob_signal and ob_low and ob_high:
            in_ob_zone = bool((last['low'] <= ob_high * 1.02) and (last['close'] >= ob_low * 0.98))
        
        in_fvg_zone = False
        if fvg_detected and fvg_low and fvg_high:
            in_fvg_zone = bool((last['low'] <= fvg_high * 1.02) and (last['close'] >= fvg_low * 0.98))
        
        return {
            'timeframe': timeframe,
            'bos_bullish': bos_bullish,
            'bos_bearish': bos_bearish,
            'choch_bullish': choch_bullish,
            'choch_bearish': choch_bearish,
            'ob_signal': ob_signal,
            'in_ob_zone': in_ob_zone,
            'ob_low': float(ob_low) if ob_low else None,
            'ob_high': float(ob_high) if ob_high else None,
            'fvg_detected': fvg_detected,
            'in_fvg_zone': in_fvg_zone,
            'fvg_low': float(fvg_low) if fvg_low else None,
            'fvg_high': float(fvg_high) if fvg_high else None,
            'swing_high': float(swing_high.iloc[-1]) if pd.notna(swing_high.iloc[-1]) else None,
            'swing_low': float(swing_low.iloc[-1]) if pd.notna(swing_low.iloc[-1]) else None
        }
    
    except Exception as e:
        return None


def calculate_entry_zone_from_smc(data_dict):
    """
    Menghitung Entry Zone berdasarkan SMC dari multi-timeframe
    Returns: dict dengan entry_1, entry_2, entry_3, average, zone
    """
    try:
        # Prioritas: 1h untuk entry zone, 15m untuk presisi
        entry_tf = CONFIG["ENTRY_TF"]
        precision_tf = CONFIG["PRECISION_TF"]
        
        # Dapatkan sinyal SMC dari timeframe entry
        smc_entry = detect_smc_signals(data_dict.get(entry_tf), entry_tf)
        smc_precision = detect_smc_signals(data_dict.get(precision_tf), precision_tf)
        
        current_price = data_dict[CONFIG["PRIMARY_TF"]]['close'].iloc[-1]
        
        # Jika ada Order Block, gunakan sebagai zona entry
        if smc_entry and smc_entry['ob_signal'] and smc_entry['ob_low'] and smc_entry['ob_high']:
            ob_high = smc_entry['ob_high']
            ob_low = smc_entry['ob_low']
            
            # Entry 1: Atas OB (antisipasinya FOMO)
            entry_1 = ob_high * 1.01
            # Entry 2: Tengah OB (main entry)
            entry_2 = (ob_high + ob_low) / 2
            # Entry 3: Bawah OB (sniper)
            entry_3 = ob_low * 0.99
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
        
        # Jika ada FVG, gunakan sebagai zona entry
        elif smc_entry and smc_entry['fvg_detected'] and smc_entry['fvg_low'] and smc_entry['fvg_high']:
            fvg_high = smc_entry['fvg_high']
            fvg_low = smc_entry['fvg_low']
            
            entry_1 = fvg_high * 1.01
            entry_2 = (fvg_high + fvg_low) / 2
            entry_3 = fvg_low * 0.99
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
        
        # Fallback: Gunakan pullback ke MA50 dengan buffer
        else:
            df_daily = data_dict[CONFIG["PRIMARY_TF"]]
            ma50 = df_daily['close'].rolling(CONFIG["MA_PERIOD"]).mean().iloc[-1]
            
            # Entry zone di sekitar MA50 dengan toleransi
            entry_1 = current_price * 1.01
            entry_2 = ma50 * 1.02
            entry_3 = ma50 * 0.98
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
    
    except Exception as e:
        log_message(f"⚠️ Error calculate_entry_zone_from_smc: {str(e)[:100]}", "warning")
        return None


def calculate_structural_sl_tp(data_dict, entry_zone, smc_data):
    """
    Menghitung SL dan TP berdasarkan struktur SMC (bukan ATR murni)
    Returns: dict dengan sl, tp1, tp2, tp3, rr_ratio
    """
    try:
        df_daily = data_dict[CONFIG["PRIMARY_TF"]]
        df_4h = data_dict.get("1h", df_daily)  # Gunakan 1h jika 4h tidak ada
        
        average_entry = entry_zone['average']
        
        # === STOP LOSS: Di bawah Strong Low ===
        # Ambil swing low terendah dari Daily atau 1H
        swing_low_daily = df_daily['low'].rolling(CONFIG["SMC_SWING_WINDOW"]).min().iloc[-1]
        swing_low_1h = df_4h['low'].rolling(CONFIG["SMC_SWING_WINDOW"]).min().iloc[-1]
        
        # Gunakan yang lebih konservatif (lebih rendah)
        structural_low = min(swing_low_daily, swing_low_1h)
        
        # SL di bawah structural low dengan buffer
        sl_raw = structural_low * 0.98
        sl = int(round_to_bei_tick(sl_raw))
        
        # Pastikan SL tidak lebih tinggi dari entry
        if sl >= average_entry:
            tick = get_tick_size(sl)
            sl = int(round_to_bei_tick(average_entry - (5 * tick)))
        
        # === TAKE PROFIT: Berdasarkan Weak High / Resistance ===
        # TP1: Resistance terdekat (swing high 1H)
        swing_high_1h = df_4h['high'].rolling(CONFIG["SMC_SWING_WINDOW"]).max().iloc[-1]
        tp1 = int(round_to_bei_tick(swing_high_1h * 1.02))
        
        # TP2: Daily BOS level
        swing_high_daily = df_daily['high'].rolling(CONFIG["SMC_SWING_WINDOW"]).max().iloc[-1]
        tp2 = int(round_to_bei_tick(swing_high_daily * 1.05))
        
        # TP3: Weak High untuk RR 1:3
        risk = average_entry - sl
        tp3_min = average_entry + (risk * CONFIG["MIN_RR_RATIO"])
        tp3 = int(round_to_bei_tick(max(tp3_min, swing_high_daily * 1.10)))
        
        # === HITUNG RR RATIO ===
        reward = tp3 - average_entry
        rr_ratio = reward / risk if risk > 0 else 0
        
        return {
            'sl': sl,
            'tp1': tp1,
            'tp2': tp2,
            'tp3': tp3,
            'rr_ratio': rr_ratio,
            'risk': risk,
            'reward': reward
        }
    
    except Exception as e:
        log_message(f"⚠️ Error calculate_structural_sl_tp: {str(e)[:100]}", "warning")
        return None

# =============================================
# SCREENING LOGIC
# =============================================
def screen_stock_final(ticker, data_dict):
    global SCREENING_AUDIT
    try:
        SCREENING_AUDIT["total_processed"] += 1
        ticker_clean = ticker.split('.JK')[0]
        
        # Pastikan ada data daily
        if CONFIG["PRIMARY_TF"] not in data_dict:
            return None
        
        df_daily = data_dict[CONFIG["PRIMARY_TF"]]
        current_price = df_daily['close'].iloc[-1]
        
        # === LAYER 0: LIKUIDITAS DASAR ===
        avg_volume_value = (df_daily['volume'].tail(20) * df_daily['close'].tail(20)).mean()
        if avg_volume_value < CONFIG["MIN_VOLUME_VALUE"] or current_price < CONFIG["MIN_PRICE"]:
            SCREENING_AUDIT["failed_liquidity"] += 1
            return None
        SCREENING_AUDIT["passed_liquidity"] += 1
        
        # === HITUNG INDIKATOR ===
        df = df_daily.copy()
        df['MA50'] = df['close'].rolling(window=CONFIG["MA_PERIOD"]).mean()
        
        try:
            df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=CONFIG["ATR_PERIOD"])
        except:
            df['ATR'] = current_price * 0.04
        
        df['volume_20d_avg'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_20d_avg']
        
        last_row = df.iloc[-1]
        ma50 = last_row['MA50']
        volume_ratio = last_row['volume_ratio']
        
        # === LAYER 1: UPTREND ===
        if current_price <= ma50:
            SCREENING_AUDIT["failed_uptrend"] += 1
            return None
        SCREENING_AUDIT["passed_uptrend"] += 1
        
        # === LAYER 2: PULLBACK ===
        pullback_min = ma50 * (1 - CONFIG["PULLBACK_TOLERANCE"])
        pullback_max = ma50 * (1 + CONFIG["PULLBACK_TOLERANCE"])
        if not (pullback_min <= current_price <= pullback_max):
            SCREENING_AUDIT["failed_pullback"] += 1
            return None
        SCREENING_AUDIT["passed_pullback"] += 1
        
        # === LAYER 3: VOLUME ===
        if volume_ratio < CONFIG["VOLUME_RATIO_MIN"]:
            SCREENING_AUDIT["failed_volume"] += 1
            return None
        SCREENING_AUDIT["passed_volume"] += 1
        
        # === LAYER 4: FUNDAMENTAL (OPTIONAL) ===
        roe = None
        de_ratio = None
        fundamental_status = "SKIP (Technical Only)"
        
        if CONFIG["FUNDAMENTAL_REQUIRED"]:
            try:
                info = yf.Ticker(ticker).info
                roe_raw = info.get('returnOnEquity')
                de_raw = info.get('debtToEquity')
                
                if roe_raw is not None:
                    roe = roe_raw * 100 if roe_raw < 1 else roe_raw
                if de_raw is not None:
                    de_ratio = de_raw / 100 if de_raw > 10 else de_raw
                
                if roe is not None and de_ratio is not None:
                    if roe >= CONFIG["MIN_ROE"] and de_ratio < CONFIG["MAX_DE"]:
                        fundamental_status = "GOOD"
                    else:
                        SCREENING_AUDIT["failed_fundamental"] += 1
                        return None
            except:
                SCREENING_AUDIT["failed_fundamental"] += 1
                return None
        
        SCREENING_AUDIT["passed_fundamental"] += 1
        
        # === LAYER 5: SMC CONFIRMATION ===
        smc_data = detect_smc_signals(df_daily, "1d")
        
        if CONFIG["SMC_REQUIRED"] and smc_data is not None:
            has_smc_signal = (
                smc_data['bos_bullish'] or 
                smc_data['choch_bullish'] or 
                smc_data['in_ob_zone'] or 
                smc_data['in_fvg_zone']
            )
            
            if not has_smc_signal:
                SCREENING_AUDIT["failed_smc"] += 1
                return None
        elif smc_data is None:
            smc_data = {}
        
        SCREENING_AUDIT["passed_smc"] += 1
        
        # === LAYER 6: ENTRY ZONE & RR ===
        entry_zone = calculate_entry_zone_from_smc(data_dict)
        if entry_zone is None:
            return None
        
        sl_tp = calculate_structural_sl_tp(data_dict, entry_zone, smc_data)
        if sl_tp is None:
            return None
        
        # Validasi RR minimal 1:3
        if sl_tp['rr_ratio'] < CONFIG["MIN_RR_RATIO"]:
            SCREENING_AUDIT["failed_rr"] += 1
            return None
        
        SCREENING_AUDIT["passed_rr"] += 1
        SCREENING_AUDIT["final_passed"] += 1
        
        # === SMC INFO TAGS ===
        smc_tags = []
        if smc_data.get('bos_bullish'):
            smc_tags.append("📈 BOS")
        if smc_data.get('choch_bullish'):
            smc_tags.append("🔄 CHoCH")
        if smc_data.get('ob_signal'):
            smc_tags.append("🟦 OB")
        if smc_data.get('fvg_detected'):
            smc_tags.append("⚡ FVG")
        
        smc_info = " + ".join(smc_tags) if smc_tags else "SMC Confirmed"
        
        # === HASIL AKHIR ===
        result = {
            "No": 0,
            "Emiten": ticker_clean,
            "Close": int(round_to_bei_tick(current_price)),
            "MA50": int(round_to_bei_tick(ma50)),
            "Vol_Ratio": round(volume_ratio, 2),
            "ROE": f"{roe:.1f}%" if roe is not None else "N/A",
            "DE": f"{de_ratio:.2f}" if de_ratio is not None else "N/A",
            "Entry": entry_zone['average'],          # Integer untuk API compatibility
            "Entry_Zone": entry_zone['zone'],        # String untuk tampilan profesional
            "SL": sl_tp['sl'],
            "TP": sl_tp['tp3'],                      # Target realistis untuk RR 1:3
            "TP_1": sl_tp['tp1'],
            "TP_2": sl_tp['tp2'],
            "TP_3": sl_tp['tp3'],
            "RR": f"1:{sl_tp['rr_ratio']:.1f}",
            "RR_Value": round(sl_tp['rr_ratio'], 2),
            "Status": "✅ READY",
            "Info": f"Fundamental: {fundamental_status} | {smc_info}",
            "Entry_Details": {
                "Entry_1": entry_zone['entry_1'],
                "Entry_2": entry_zone['entry_2'],
                "Entry_3": entry_zone['entry_3'],
                "Pct_1": CONFIG["ENTRY_1_PCT"],
                "Pct_2": CONFIG["ENTRY_2_PCT"],
                "Pct_3": CONFIG["ENTRY_3_PCT"]
            },
            "sort_key": sl_tp['rr_ratio']
        }
        
        return result
    
    except Exception as e:
        log_message(f"{ticker}: ❌ Error screening: {str(e)[:150]}", "error")
        return None

# =============================================
# AUDIT REPORT
# =============================================
def print_screening_audit_report():
    global SCREENING_AUDIT
    log_message("\n" + "="*80, "success")
    log_message("📊 LAPORAN AUDIT SCREENING", "success")
    log_message("="*80, "success")
    
    total = SCREENING_AUDIT["total_processed"]
    if total == 0:
        log_message("⚠️ Tidak ada saham yang diproses", "warning")
        return
    
    layers = [
        ("LIKUIDITAS DASAR", SCREENING_AUDIT["passed_liquidity"], SCREENING_AUDIT["failed_liquidity"]),
        ("UPTREND (Harga > MA50)", SCREENING_AUDIT["passed_uptrend"], SCREENING_AUDIT["failed_uptrend"]),
        ("PULLBACK KE MA50 (±5%)", SCREENING_AUDIT["passed_pullback"], SCREENING_AUDIT["failed_pullback"]),
        ("VOLUME KONFIRMASI (>1.5x)", SCREENING_AUDIT["passed_volume"], SCREENING_AUDIT["failed_volume"]),
        ("FUNDAMENTAL (Optional)", SCREENING_AUDIT["passed_fundamental"], SCREENING_AUDIT["failed_fundamental"]),
        ("SMC CONFIRMATION", SCREENING_AUDIT["passed_smc"], SCREENING_AUDIT["failed_smc"]),
        ("RISK-REWARD ≥1:3", SCREENING_AUDIT["passed_rr"], SCREENING_AUDIT["failed_rr"]),
        ("✅ TOTAL LOLOS", SCREENING_AUDIT["final_passed"], total - SCREENING_AUDIT["final_passed"])
    ]
    
    print(f"\n{Fore.CYAN}{'Layer Filter':<45} {'Lolos':>12} {'Gagal':>12} {'% Lolos':>10}{Style.RESET_ALL}")
    print("-" * 85)
    for name, passed, failed in layers:
        if "✅" in name:
            color = Fore.GREEN
        else:
            color = Fore.YELLOW if passed > 0 else Fore.RED
        pct_val = f"{passed/total*100:.1f}%" if total > 0 else "0.0%"
        print(f"{color}{name:<45} {passed:>12,} {failed:>12,} {pct_val:>10}{Style.RESET_ALL}")
    print("-" * 85)
    print(f"{Fore.MAGENTA}TOTAL SAHAM DIPROSES:{Style.RESET_ALL} {total:>57,}")
    log_message("="*80, "success")

# =============================================
# SAVE FUNCTIONS
# =============================================
def save_json_result(results, filename):
    """Save hasil screening ke JSON untuk API (struktur tetap kompatibel)"""
    try:
        clean_results = []
        for r in results:
            clean_r = {k: v for k, v in r.items() if k != 'sort_key'}
            clean_results.append(clean_r)
        
        json_data = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "total_saham": len(results),
            "config": {
                "strategy": "MA50 Pullback + SMC + Entry Zone",
                "min_rr": CONFIG["MIN_RR_RATIO"],
                "max_output": CONFIG["MAX_OUTPUT"],
                "fundamental_required": CONFIG["FUNDAMENTAL_REQUIRED"],
                "timeframes": CONFIG["TIMEFRAMES"]
            },
            "data": clean_results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        log_message(f"💾 Hasil JSON disimpan ke: {filename}", "success")
        return True
    except Exception as e:
        log_message(f"❌ Gagal simpan JSON: {str(e)}", "error")
        return False


def save_excel_result(results, filename):
    """Save hasil screening ke Excel dengan format profesional"""
    try:
        # Siapkan data untuk Excel
        excel_data = []
        for r in results:
            row = {
                "No": r["No"],
                "Emiten": r["Emiten"],
                "Close": r["Close"],
                "MA50": r["MA50"],
                "Vol_Ratio": r["Vol_Ratio"],
                "ROE": r["ROE"],
                "DE": r["DE"],
                "Entry_Zone": r["Entry_Zone"],  # Tampilan profesional
                "Average_Entry": r["Entry"],     # Untuk referensi
                "SL": r["SL"],
                "TP": r["TP"],
                "TP_1": r.get("TP_1", r["TP"]),
                "TP_2": r.get("TP_2", r["TP"]),
                "TP_3": r.get("TP_3", r["TP"]),
                "RR": r["RR"],
                "Status": r["Status"],
                "Info": r["Info"]
            }
            excel_data.append(row)
        
        df_excel = pd.DataFrame(excel_data)
        
        # Atur urutan kolom
        columns = ["No", "Emiten", "Close", "MA50", "Vol_Ratio", "ROE", "DE", 
                   "Entry_Zone", "Average_Entry", "SL", "TP", "TP_1", "TP_2", "TP_3", 
                   "RR", "Status", "Info"]
        df_excel = df_excel[columns]
        
        df_excel.to_excel(filename, index=False)
        log_message(f"💾 Hasil Excel disimpan ke: {filename}", "success")
        return True
    except Exception as e:
        log_message(f"❌ Gagal simpan Excel: {str(e)}", "error")
        return False

# =============================================
# MAIN FUNCTION
# =============================================
def load_tickers():
    tickers_file = "kode_saham_958.txt"
    if not os.path.exists(tickers_file):
        log_message(f"❌ File {tickers_file} tidak ditemukan!", "error")
        sys.exit(1)
    
    try:
        with open(tickers_file, "r", encoding='utf-8') as f:
            content = f.read()
            tickers = re.findall(r"['\"]([A-Z0-9]{4})['\"]", content)
            if not tickers:
                clean_content = re.sub(r"[^\w\s,]", "", content)
                words = [word.strip() for word in re.split(r'[,;\s]+', clean_content) if word.strip()]
                tickers = [word for word in words if len(word) == 4 and re.match(r'^[A-Z0-9]{4}$', word)]
            if not tickers:
                log_message("❌ Gagal mengekstrak kode saham dari file!", "error")
                sys.exit(1)
            return [f"{ticker}.JK" for ticker in tickers]
    except Exception as e:
        log_message(f"❌ Error saat membaca file: {str(e)}", "error")
        sys.exit(1)

def clean_old_data(max_age_days=365):
    total_deleted = 0
    current_date = datetime.now().date()
    for root, dirs, files in os.walk(CONFIG["DATA_DIR"]):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                try:
                    file_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                    age_days = (current_date - file_date).days
                    if age_days > max_age_days:
                        os.remove(file_path)
                        total_deleted += 1
                except:
                    pass
    return total_deleted

def main():
    start_time = time.time()
    today_str = datetime.now().strftime("%Y%m%d")
    results = []
    
    global SCREENING_AUDIT
    SCREENING_AUDIT = {k: 0 if k != "detailed_failures" else {} for k in SCREENING_AUDIT}
    SCREENING_AUDIT["detailed_failures"] = {}
    
    log_message("="*80, "success")
    log_message(f"🚀 SCREENER TRADER INDONESIA - VERSI FINAL + SMC + ENTRY ZONE", "success")
    log_message(f"📅 Tanggal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "success")
    log_message(f"🎯 FOKUS: Multi-Timeframe (1w/1d/1h/15m) + Entry Zone", "success")
    log_message(f"⚙️  FUNDAMENTAL: {'WAJIB' if CONFIG['FUNDAMENTAL_REQUIRED'] else 'OPTIONAL'}", "success")
    log_message(f"✅ HANYA output 'READY' (max {CONFIG['MAX_OUTPUT']} saham)", "success")
    log_message(f"📊 Entry Zone: 3 Cicil (20%, 50%, 30%) dengan Average Entry", "success")
    log_message("="*80, "success")
    
    tickers = load_tickers()
    total_tickers = len(tickers)
    log_message(f"✅ Berhasil memuat {total_tickers} kode saham", "success")
    
    log_message(f"\n🔍 TESTING 3 SAHAM PERTAMA...", "warning")
    test_tickers = tickers[:3]
    successful_tests = 0
    for ticker in test_tickers:
        data = download_multi_timeframe_data(ticker)
        if data is not None:
            successful_tests += 1
            log_message(f"{ticker}: ✅ BERHASIL ({len(data)} timeframe)", "success")
        else:
            log_message(f"{ticker}: ❌ GAGAL DOWNLOAD", "error")
    
    if successful_tests == 0:
        log_message("⚠️ Gagal test koneksi (0/3 berhasil). Berhenti.", "error")
        sys.exit(1)
    
    log_message(f"✅ Test berhasil ({successful_tests}/3). Lanjut screening otomatis...", "success")
    
    log_message("\n" + "="*80, "success")
    log_message(f"⏬ MEMULAI SCREENING LENGKAP {total_tickers} SAHAM...", "success")
    log_message("="*80 + "\n", "success")
    
    successful_downloads = 0
    failed_downloads = 0
    for i in range(0, total_tickers, CONFIG["BATCH_SIZE"]):
        batch = tickers[i:i + CONFIG["BATCH_SIZE"]]
        batch_num = i // CONFIG["BATCH_SIZE"] + 1
        total_batches = (total_tickers + CONFIG["BATCH_SIZE"] - 1) // CONFIG["BATCH_SIZE"]
        log_message(f"📦 BATCH {batch_num}/{total_batches} ({len(batch)} saham)", "info")
        
        download_results = {}
        for ticker in batch:
            data = download_multi_timeframe_data(ticker)
            if data is not None:
                successful_downloads += 1
                download_results[ticker] = data
            else:
                failed_downloads += 1
        
        for ticker, stock_data in download_results.items():
            if stock_data is None: continue
            result = screen_stock_final(ticker, stock_data)
            if result: results.append(result)
        
        if i + CONFIG["BATCH_SIZE"] < total_tickers:
            time.sleep(CONFIG["BATCH_DELAY"])
        gc.collect()
    
    print_screening_audit_report()
    
    log_message(f"\n📊 SCREENING SELESAI!", "success")
    log_message(f"✅ Berhasil download: {successful_downloads}/{total_tickers}", "success")
    log_message(f"🎯 Saham lolos: {len(results)}/{successful_downloads}", "success")
    
    if results:
        results.sort(key=lambda x: x["sort_key"], reverse=True)
        results = results[:CONFIG["MAX_OUTPUT"]]
        for idx, res in enumerate(results, 1):
            res["No"] = idx
        
        # Save Excel
        excel_filename = f"{CONFIG['EXCEL_PREFIX']}_{today_str}.xlsx"
        save_excel_result(results, excel_filename)
        
        # Save JSON (2 files untuk API compatibility)
        save_json_result(results, "latest_screening.json")
        save_json_result(results, f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")
        
        log_message(f"🎯 Fokus pada {len(results)} saham ini", "success")
    else:
        log_message("❌ TIDAK ADA SAHAM YANG LOLOS", "error")
        save_json_result([], "latest_screening.json")
        save_json_result([], f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")
    
    deleted_files = clean_old_data(max_age_days=CONFIG["MIN_CACHE_RETENTION_DAYS"])
    if deleted_files > 0:
        log_message(f"🧹 Membersihkan {deleted_files} file cache lama", "info")
    
    elapsed = (time.time() - start_time) / 60
    log_message(f"\n⏱️  Waktu eksekusi: {elapsed:.1f} menit", "success")
    log_message("="*80, "success")
    
    sys.exit(0)

# =============================================
# EKSEKUSI PROGRAM
# =============================================
if __name__ == "__main__":
    try:
        print(f"\n{Fore.CYAN}" + "="*80)
        print("🚀 SCREENER TRADER INDONESIA - VERSI FINAL + SMC + ENTRY ZONE")
        print("   Multi-Timeframe (1w/1d/1h/15m) + Entry Zone 3 Cicil")
        print("   JSON Compatible: Entry (int) + Entry_Zone (string)")
        print("="*80 + f"{Style.RESET_ALL}\n")
        main()
    except KeyboardInterrupt:
        log_message("\n⏹️ Program dihentikan oleh user", "error")
        sys.exit(0)
    except Exception as e:
        log_message(f"\n💥 ERROR KRITIS: {str(e)}", "error")
        import traceback
        traceback.print_exc()
        sys.exit(1)