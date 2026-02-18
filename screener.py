#!/usr/bin/env python3
"""
SCREENER TRADER INDONESIA - VERSI GITHUB ACTIONS + API ENDPOINT
✅ DOWNLOAD: period="max" → SEMUA data dari IPO sampai hari ini
✅ FILTER: Hanya hari dengan volume > 0 (hapus noise sebelum listing)
✅ STRATEGI: Pullback dalam UPTREND (harga > MA50) + SMC Confirmation
✅ SMC: Order Block + BOS + FVG Detection
✅ FUNDAMENTAL: OPTIONAL (bisa ON/OFF via CONFIG)
✅ OUTPUT: Max 7 saham — HANYA "✅ READY"
✅ RISK MANAGEMENT: SL = Entry - (2.0 × ATR), RR minimal 1:3
✅ PEMBULATAN: 100% sesuai fraksi BEI
✅ CONCURRENT DOWNLOAD (5 workers) + Local Cache (Parquet) + Audit Report
✅ GITHUB ACTIONS READY: No input(), Auto-exit
✅ API ENDPOINT: Save JSON untuk akses programmatic
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
    "FUNDAMENTAL_REQUIRED": False,  # ✅ FALSE = Skip Fundamental, TRUE = Wajib
    "MIN_ROE": 10,                   # Minimal ROE 10% (jika fundamental ON)
    "MAX_DE": 1.0,                   # Maximal Debt/Equity 1.0 (jika fundamental ON)
    
    # --- SMC Configuration ---
    "SMC_SWING_WINDOW": 20,
    "SMC_OB_RALLY_MIN": 0.03,
    "SMC_FVG_MIN_GAP": 0.01,
    "SMC_REQUIRED": True,
    
    # --- System Configuration ---
    "MAX_WORKERS": 5,
    "REQUEST_DELAY": 0.7,
    "JITTER_MIN": 0.2,
    "JITTER_MAX": 0.5,
    "MAX_RETRIES": 2,
    "BATCH_SIZE": 30,
    "BATCH_DELAY": 5,
    "DATA_DIR": "data_ohlc_v2",
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
    price = float(price)
    if price < 200: return 1
    elif price < 500: return 2
    elif price < 2000: return 5
    elif price < 5000: return 10
    else: return 25

def round_to_bei_tick(price):
    price = float(price)
    tick = get_tick_size(price)
    return round(price / tick) * tick

# =============================================
# MANAJEMEN DATA LOKAL
# =============================================
def get_local_file_path(ticker):
    clean_ticker = re.sub(r'[^A-Z0-9]', '', ticker.split('.')[0])
    today = datetime.now()
    year_month_dir = os.path.join(CONFIG["DATA_DIR"], str(today.year), f"{today.month:02d}")
    os.makedirs(year_month_dir, exist_ok=True)
    filename = f"{clean_ticker}_{today.strftime('%Y%m%d')}.parquet"
    return os.path.join(year_month_dir, filename)

def is_local_data_fresh(file_path):
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
        is_fresh = days_diff <= CONFIG["MAX_LOCAL_AGE_DAYS"]
        
        if not is_fresh:
            log_message(f"📁 Cache {os.path.basename(file_path)}: Data T-{days_diff}, perlu update", "warning")
        
        return is_fresh
    except Exception as e:
        log_message(f"📁 Error cek cache: {str(e)[:100]}", "error")
        return False

def save_ohlc_data(ticker, df):
    try:
        file_path = get_local_file_path(ticker)
        df['ticker'] = ticker
        df['last_update'] = datetime.now()
        df.to_parquet(file_path, index=False)
        log_message(f"{ticker}: 💾 Data OHLCV disimpan ke {os.path.basename(file_path)}", "success")
        return True
    except Exception as e:
        log_message(f"{ticker}: ❌ Gagal simpan data lokal: {str(e)}", "error")
        return False

def load_local_data(ticker):
    file_path = get_local_file_path(ticker)
    if CONFIG["FORCE_DOWNLOAD"]:
        log_message(f"{ticker}: 🔁 Force download diaktifkan", "info")
        return None
    if not os.path.exists(file_path) or not is_local_data_fresh(file_path):
        return None
    try:
        df = pd.read_parquet(file_path)
        if len(df) < CONFIG["MIN_TRADING_DAYS"]: 
            return None
        return df
    except Exception as e:
        log_message(f"{ticker}: ❌ Error baca data lokal: {str(e)}", "error")
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
# CONCURRENT DOWNLOAD
# =============================================
def download_stock_data_concurrent(ticker):
    with DOWNLOAD_SEMAPHORE:
        local_data = load_local_data(ticker)
        if local_data is not None:
            last_date = local_data['date'].iloc[-1]
            if isinstance(last_date, pd.Timestamp):
                last_date = last_date.date()
            today = datetime.now().date()
            days_diff = (today - last_date).days
            
            if days_diff == 0:
                status = "✅ LIVE"
                color = "success"
            elif days_diff == 1:
                status = "⚠️ T-1"
                color = "warning"
            else:
                status = f"❌ T-{days_diff}"
                color = "error"
            
            log_message(f"{ticker}: 📥 Menggunakan cache lokal {status} (Last: {last_date})", color)
            return local_data
        
        ticker_clean = str(ticker).strip()
        
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                delay = get_adaptive_delay()
                time.sleep(delay)
                log_message(f"{ticker_clean}: 🌐 Mengunduh SEMUA data (period='max')...", "processing")
                
                stock = yf.Ticker(ticker_clean)
                df = stock.history(period="max", interval="1d", auto_adjust=True)
                
                if df.empty:
                    raise ValueError("Data kosong")
                
                df.reset_index(inplace=True)
                df.columns = [col.lower() for col in df.columns]
                if 'date' not in df.columns and 'datetime' in df.columns:
                    df.rename(columns={'datetime': 'date'}, inplace=True)
                if 'date' not in df.columns and 'index' in df.columns:
                    df.rename(columns={'index': 'date'}, inplace=True)
                
                df = df[df['volume'] > 0].copy()
                
                if len(df) < CONFIG["MIN_TRADING_DAYS"]:
                    log_message(f"{ticker_clean}: ⚠️ Data tidak lengkap ({len(df)} hari)", "warning")
                    return None
                
                first_date = df['date'].iloc[0]
                last_date = df['date'].iloc[-1]
                
                if isinstance(first_date, pd.Timestamp):
                    first_date = first_date.date()
                if isinstance(last_date, pd.Timestamp):
                    last_date = last_date.date()
                
                today = datetime.now().date()
                days_diff = (today - last_date).days
                
                if days_diff == 0:
                    status_msg = "✅ LIVE"
                    color = "success"
                elif days_diff == 1:
                    status_msg = "⚠️ T-1 (Kemarin)"
                    color = "warning"
                else:
                    status_msg = f"❌ T-{days_diff} (Data Lama)"
                    color = "error"
                
                log_message(
                    f"{ticker_clean}: {status_msg} | {len(df)} hari data "
                    f"(IPO: {first_date} → Last: {last_date})",
                    color
                )
                
                save_ohlc_data(ticker_clean, df.copy())
                return df
                
            except Exception as e:
                record_throttling_error(str(e))
                wait_time = 1.5 ** attempt + np.random.uniform(0, 1)
                if "404" in str(e) or "not found" in str(e).lower():
                    log_message(f"{ticker_clean}: ⚠️ Saham tidak tersedia di Yahoo Finance", "error")
                    return None
                log_message(
                    f"{ticker_clean}: ❌ Gagal download (percobaan {attempt+1}): {str(e)[:100]}",
                    "error"
                )
                if attempt < CONFIG["MAX_RETRIES"] - 1:
                    time.sleep(wait_time)
        log_message(f"{ticker_clean}: 💥 Gagal semua percobaan download", "error")
        return None

# =============================================
# SMC DETECTION FUNCTIONS
# =============================================

def detect_smc_signals(df):
    """
    Deteksi Smart Money Concepts signals
    Returns: dict dengan sinyal SMC
    """
    try:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # === 1. DETEKSI SWING HIGH/LOW (Untuk BOS/CHoCH) ===
        window = CONFIG["SMC_SWING_WINDOW"]
        swing_high = df['high'].rolling(window=window).max().shift(1)
        swing_low = df['low'].rolling(window=window).min().shift(1)
        
        # === 2. DETEKSI BOS (Break of Structure) ===
        bos_bullish = bool(last['close'] > swing_high.iloc[-1])
        bos_bearish = bool(last['close'] < swing_low.iloc[-1])
        
        # === 3. DETEKSI CHoCH (Change of Character) ===
        choch_bullish = False
        choch_bearish = False
        
        if len(df) >= 40:
            prev_swing_high = swing_high.iloc[-2] if len(swing_high) >= 2 else swing_high.iloc[-1]
            prev_swing_low = swing_low.iloc[-2] if len(swing_low) >= 2 else swing_low.iloc[-1]
            
            if last['close'] > prev_swing_high and prev['close'] < prev_swing_low:
                choch_bullish = True
            elif last['close'] < prev_swing_low and prev['close'] > prev_swing_high:
                choch_bearish = True
        
        # === 4. DETEKSI ORDER BLOCK (Bullish) ===
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
        
        # === 5. DETEKSI FVG (Fair Value Gap) ===
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
            elif candle_3_high < candle_1_low:
                gap_pct = (candle_1_low - candle_3_high) / candle_1_low
                if gap_pct >= CONFIG["SMC_FVG_MIN_GAP"]:
                    fvg_detected = True
                    fvg_low = candle_3_high
                    fvg_high = candle_1_low
        
        # === 6. CEK APAKAH HARGA DI AREA ORDER BLOCK ===
        in_ob_zone = False
        if ob_signal and ob_low and ob_high:
            in_ob_zone = bool((last['low'] <= ob_high * 1.02) and (last['close'] >= ob_low * 0.98))
        
        # === 7. CEK APAKAH HARGA DI AREA FVG ===
        in_fvg_zone = False
        if fvg_detected and fvg_low and fvg_high:
            in_fvg_zone = bool((last['low'] <= fvg_high * 1.02) and (last['close'] >= fvg_low * 0.98))
        
        return {
            'bos_bullish': bos_bullish,
            'bos_bearish': bos_bearish,
            'choch_bullish': choch_bullish,
            'choch_bearish': choch_bearish,
            'ob_signal': ob_signal,
            'in_ob_zone': in_ob_zone,
            'ob_low': ob_low,
            'ob_high': ob_high,
            'fvg_detected': fvg_detected,
            'in_fvg_zone': in_fvg_zone,
            'fvg_low': fvg_low,
            'fvg_high': fvg_high,
            'swing_high': float(swing_high.iloc[-1]) if pd.notna(swing_high.iloc[-1]) else None,
            'swing_low': float(swing_low.iloc[-1]) if pd.notna(swing_low.iloc[-1]) else None
        }
    
    except Exception as e:
        log_message(f"⚠️ Error detect_smc_signals: {str(e)[:100]}", "warning")
        return None


def enhance_with_smc(result, df, smc_data):
    """
    Enhance hasil screening dengan data SMC
    """
    if smc_data is None:
        return result
    
    # === UPDATE ENTRY ZONE JIKA ADA ORDER BLOCK ===
    if smc_data['ob_signal'] and smc_data['in_ob_zone'] and smc_data['ob_low'] and smc_data['ob_high']:
        ob_entry = (smc_data['ob_low'] + smc_data['ob_high']) / 2
        if ob_entry < result['Entry'] * 1.02:
            result['Entry'] = int(round_to_bei_tick(ob_entry))
            result['Info'] += " | 🎯 Entry di OB"
    
    # === UPDATE ENTRY JIKA ADA FVG ===
    if smc_data['fvg_detected'] and smc_data['in_fvg_zone'] and smc_data['fvg_low']:
        fvg_entry = smc_data['fvg_low'] * 1.01
        if fvg_entry < result['Entry']:
            result['Entry'] = int(round_to_bei_tick(fvg_entry))
            result['Info'] += " | ⚡ Entry di FVG"
    
    # === UPDATE TP JIKA ADA FVG ===
    if smc_data['fvg_detected'] and smc_data['fvg_high']:
        fvg_tp = smc_data['fvg_high'] * 1.05
        if fvg_tp > result['TP']:
            result['TP'] = int(round_to_bei_tick(fvg_tp))
    
    # === UPDATE TP JIKA ADA SWING HIGH (Resistance) ===
    if smc_data['swing_high']:
        resistance_tp = smc_data['swing_high'] * 1.05
        if resistance_tp > result['TP']:
            result['TP'] = int(round_to_bei_tick(resistance_tp))
    
    # === RECALCULATE RR ===
    risk_points = result['Entry'] - result['SL']
    reward_points = result['TP'] - result['Entry']
    if risk_points > 0:
        new_rr = reward_points / risk_points
        result['RR'] = f"1:{new_rr:.1f}"
        result['sort_key'] = new_rr
    
    # === TAMBAH INFO SMC ===
    smc_info = []
    if smc_data['bos_bullish']:
        smc_info.append("📈 BOS")
    if smc_data['choch_bullish']:
        smc_info.append("🔄 CHoCH")
    if smc_data['ob_signal']:
        smc_info.append("🟦 OB")
    if smc_data['fvg_detected']:
        smc_info.append("⚡ FVG")
    
    if smc_info:
        result['Info'] += " | " + " + ".join(smc_info)
    
    return result

# =============================================
# SCREENING LOGIC — HYBRID (MA50 + SMC + FUNDAMENTAL OPTIONAL)
# =============================================
def screen_stock_final(ticker, data):
    global SCREENING_AUDIT
    try:
        SCREENING_AUDIT["total_processed"] += 1
        ticker_clean = ticker.split('.JK')[0]
        current_price = data['close'].iloc[-1]
        
        # === LAYER 0: LIKUIDITAS DASAR ===
        avg_volume_value = (data['volume'].tail(20) * data['close'].tail(20)).mean()
        if avg_volume_value < CONFIG["MIN_VOLUME_VALUE"] or current_price < CONFIG["MIN_PRICE"]:
            SCREENING_AUDIT["failed_liquidity"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Likuiditas (Vol×Harga:{avg_volume_value:,.0f} < Rp500jt)"
            return None
        SCREENING_AUDIT["passed_liquidity"] += 1
        
        # === HITUNG INDIKATOR ===
        df = data.copy()
        df['MA50'] = df['close'].rolling(window=CONFIG["MA_PERIOD"]).mean()
        
        try:
            atr_col = f'ATR_{CONFIG["ATR_PERIOD"]}'
            df[atr_col] = ta.atr(df['high'], df['low'], df['close'], length=CONFIG["ATR_PERIOD"])
            df['ATR'] = df[atr_col]
        except:
            df['ATR'] = current_price * 0.04
        
        df['volume_20d_avg'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_20d_avg']
        
        last_row = df.iloc[-1]
        ma50 = last_row['MA50']
        atr = last_row['ATR'] if pd.notna(last_row['ATR']) else current_price * 0.04
        volume_ratio = last_row['volume_ratio']
        
        # === LAYER 1: UPTREND (HARGA > MA50) ===
        if current_price <= ma50:
            SCREENING_AUDIT["failed_uptrend"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ BUKAN UPTREND (harga:{current_price:,.0f} <= MA50:{ma50:,.0f})"
            return None
        SCREENING_AUDIT["passed_uptrend"] += 1
        
        # === LAYER 2: PULLBACK KE MA50 (±5%) ===
        pullback_min = ma50 * (1 - CONFIG["PULLBACK_TOLERANCE"])
        pullback_max = ma50 * (1 + CONFIG["PULLBACK_TOLERANCE"])
        if not (pullback_min <= current_price <= pullback_max):
            SCREENING_AUDIT["failed_pullback"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Belum di zona pullback (harga:{current_price:,.0f} vs MA50:{ma50:,.0f})"
            return None
        SCREENING_AUDIT["passed_pullback"] += 1
        
        # === LAYER 3: VOLUME KONFIRMASI (>1.5x) ===
        if volume_ratio < CONFIG["VOLUME_RATIO_MIN"]:
            SCREENING_AUDIT["failed_volume"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Volume tidak konfirmasi (ratio:{volume_ratio:.2f} < {CONFIG['VOLUME_RATIO_MIN']})"
            return None
        SCREENING_AUDIT["passed_volume"] += 1
        
        # === 🆕 LAYER 4: FUNDAMENTAL (OPTIONAL - BISA ON/OFF) ===
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
                        SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Value trap (ROE:{roe:.1f}% DE:{de_ratio:.2f})"
                        return None
                else:
                    SCREENING_AUDIT["failed_fundamental"] += 1
                    SCREENING_AUDIT["detailed_failures"][ticker_clean] = "❌ Data fundamental tidak tersedia"
                    return None
            except Exception as e:
                SCREENING_AUDIT["failed_fundamental"] += 1
                SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Error ambil fundamental: {str(e)[:50]}"
                return None
        else:
            log_message(f"{ticker_clean}: ⏭️ Skip fundamental check (Technical Only)", "info")
        
        SCREENING_AUDIT["passed_fundamental"] += 1
        
        # === LAYER 5: SMC CONFIRMATION ===
        smc_data = detect_smc_signals(df)
        
        if CONFIG["SMC_REQUIRED"] and smc_data is not None:
            has_smc_signal = (
                smc_data['bos_bullish'] or 
                smc_data['choch_bullish'] or 
                smc_data['in_ob_zone'] or 
                smc_data['in_fvg_zone']
            )
            
            if not has_smc_signal:
                SCREENING_AUDIT["failed_smc"] += 1
                SCREENING_AUDIT["detailed_failures"][ticker_clean] = "❌ Tidak ada konfirmasi SMC"
                return None
        elif smc_data is None:
            smc_data = {}
        
        SCREENING_AUDIT["passed_smc"] += 1
        
        # === LAYER 6: HITUNG LEVEL TRADING & RR ===
        entry = round_to_bei_tick(current_price)
        sl_raw = entry - (CONFIG["ATR_MULTIPLIER_SL"] * atr)
        sl_raw = max(sl_raw, entry * (1 - CONFIG["MAX_RISK_PCT"]), CONFIG["MIN_PRICE"])
        sl = round_to_bei_tick(sl_raw)
        if sl >= entry:
            tick = get_tick_size(sl)
            sl = max(CONFIG["MIN_PRICE"], sl - tick)
        
        risk_points = entry - sl
        if risk_points <= 0:
            SCREENING_AUDIT["failed_rr"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ Risk negatif (Entry:{entry} SL:{sl})"
            return None
        
        tp_raw = entry + (risk_points * CONFIG["MIN_RR_RATIO"])
        tp = round_to_bei_tick(tp_raw)
        if tp <= entry:
            tick = get_tick_size(tp)
            tp = tp + tick
        
        reward_points = tp - entry
        rr_ratio_value = reward_points / risk_points if risk_points > 0 else 0
        rr_ratio_str = f"1:{rr_ratio_value:.1f}" if rr_ratio_value > 0 else "N/A"
        
        if rr_ratio_value < CONFIG["MIN_RR_RATIO"]:
            SCREENING_AUDIT["failed_rr"] += 1
            SCREENING_AUDIT["detailed_failures"][ticker_clean] = f"❌ RR rendah (1:{rr_ratio_value:.1f} < 1:{CONFIG['MIN_RR_RATIO']})"
            return None
        SCREENING_AUDIT["passed_rr"] += 1
        
        # === HASIL AKHIR ===
        result = {
            "No": 0,
            "Emiten": ticker_clean,
            "Close": int(round_to_bei_tick(current_price)),
            "MA50": int(round_to_bei_tick(ma50)),
            "Vol_Ratio": round(volume_ratio, 2),
            "ROE": f"{roe:.1f}%" if roe is not None else "N/A",
            "DE": f"{de_ratio:.2f}" if de_ratio is not None else "N/A",
            "Entry": int(entry),
            "SL": int(sl),
            "TP": int(tp),
            "RR": rr_ratio_str,
            "Status": "✅ READY",
            "Info": f"Fundamental: {fundamental_status}",
            "sort_key": rr_ratio_value
        }
        
        if fundamental_status == "SKIP (Technical Only)":
            result['Info'] += " ⚠️ Pure Technical"
        elif fundamental_status == "UNKNOWN":
            result['Info'] += " (verifikasi manual 2 menit di idx.co.id)"
        
        # Enhance dengan SMC
        result = enhance_with_smc(result, df, smc_data)
        
        # Validasi RR ulang setelah enhancement
        final_rr = float(result['RR'].split(':')[1]) if ':' in result['RR'] else 0
        if final_rr < CONFIG["MIN_RR_RATIO"]:
            SCREENING_AUDIT["failed_rr"] += 1
            return None
        
        SCREENING_AUDIT["final_passed"] += 1
        
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
    log_message("📊 LAPORAN AUDIT SCREENING - HYBRID (MA50 + SMC + FUNDAMENTAL OPTIONAL)", "success")
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
        ("🆕 SMC CONFIRMATION", SCREENING_AUDIT["passed_smc"], SCREENING_AUDIT["failed_smc"]),
        ("RISK-REWARD ≥1:3", SCREENING_AUDIT["passed_rr"], SCREENING_AUDIT["failed_rr"]),
        ("✅ TOTAL LOLOS", SCREENING_AUDIT["final_passed"], total - SCREENING_AUDIT["final_passed"])
    ]
    
    print(f"\n{Fore.CYAN}{'Layer Filter':<45} {'Lolos':>12} {'Gagal':>12} {'% Lolos':>10}{Style.RESET_ALL}")
    print("-" * 85)
    for name, passed, failed in layers:
        if "✅" in name:
            color = Fore.GREEN
            pct_val = f"{passed/total*100:.1f}%" if total > 0 else "0.0%"
        else:
            color = Fore.YELLOW if passed > 0 else Fore.RED
            pct_val = f"{passed/total*100:.1f}%" if total > 0 else "0.0%"
        print(f"{color}{name:<45} {passed:>12,} {failed:>12,} {pct_val:>10}{Style.RESET_ALL}")
    print("-" * 85)
    print(f"{Fore.MAGENTA}TOTAL SAHAM DIPROSES:{Style.RESET_ALL} {total:>57,}")
    log_message("="*80, "success")

# =============================================
# FUNGSI UTAMA
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

# =============================================
# ✅ FUNGSI BARU: SAVE JSON RESULT (API ENDPOINT)
# =============================================
def save_json_result(results, filename):
    """Save hasil screening ke JSON untuk API"""
    try:
        # Bersihkan data untuk JSON (hapus sort_key yang tidak perlu)
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
                "strategy": "MA50 Pullback + SMC",
                "min_rr": CONFIG["MIN_RR_RATIO"],
                "max_output": CONFIG["MAX_OUTPUT"],
                "fundamental_required": CONFIG["FUNDAMENTAL_REQUIRED"]
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

# =============================================
# FUNGSI MAIN
# =============================================
def main():
    start_time = time.time()
    today_str = datetime.now().strftime("%Y%m%d")
    results = []
    
    global SCREENING_AUDIT
    SCREENING_AUDIT = {k: 0 if k != "detailed_failures" else {} for k in SCREENING_AUDIT}
    SCREENING_AUDIT["detailed_failures"] = {}
    
    log_message("="*80, "success")
    log_message(f"🚀 SCREENER TRADER INDONESIA - VERSI FINAL + SMC", "success")
    log_message(f"📅 Tanggal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "success")
    log_message(f"🎯 FOKUS: MA50 Pullback + SMC Confirmation (BOS/OB/FVG)", "success")
    log_message(f"⚙️  FUNDAMENTAL: {'WAJIB' if CONFIG['FUNDAMENTAL_REQUIRED'] else 'OPTIONAL (Skip)'}", "success")
    if CONFIG["FUNDAMENTAL_REQUIRED"]:
        log_message(f"📊 Syarat: ROE >{CONFIG['MIN_ROE']}% + D/E <{CONFIG['MAX_DE']}", "info")
    else:
        log_message(f"📊 Mode: Pure Technical Analysis (Lebih Cepat & Lebih Banyak Saham)", "info")
    log_message(f"✅ HANYA output 'READY' (max {CONFIG['MAX_OUTPUT']} saham)", "success")
    log_message(f"📊 Volume Ratio WAJIB: Hari ini >{CONFIG['VOLUME_RATIO_MIN']}x rata-rata 20 hari", "success")
    log_message(f"📁 Cache Check: Berdasarkan tanggal data", "success")
    log_message("="*80, "success")
    
    tickers = load_tickers()
    total_tickers = len(tickers)
    log_message(f"✅ Berhasil memuat {total_tickers} kode saham", "success")
    
    log_message(f"\n🔍 TESTING 3 SAHAM PERTAMA...", "warning")
    test_tickers = tickers[:3]
    successful_tests = 0
    for ticker in test_tickers:
        data = download_stock_data_concurrent(ticker)
        if data is not None:
            successful_tests += 1
            last_date = data['date'].iloc[-1]
            if isinstance(last_date, pd.Timestamp):
                last_date = last_date.date()
            log_message(f"{ticker}: ✅ BERHASIL ({len(data)} hari data, Last: {last_date})", "success")
        else:
            log_message(f"{ticker}: ❌ GAGAL DOWNLOAD", "error")
    
    # ✅ UBAH DARI INPUT() MENJADI AUTO-CONTINUE
    if successful_tests == 0:
        log_message("⚠️ Gagal test koneksi (0/3 berhasil). Berhenti untuk hemat kuota.", "error")
        sys.exit(1)
    
    log_message(f"✅ Test berhasil ({successful_tests}/3). Lanjut screening otomatis...", "success")
    
    log_message("\n" + "="*80, "success")
    log_message(f"⏬ MEMULAI SCREENING LENGKAP {total_tickers} SAHAM...", "success")
    log_message(f"⏱️  Perkiraan selesai: {datetime.now() + timedelta(minutes=12):%H:%M}", "info")
    log_message("="*80 + "\n", "success")
    
    successful_downloads = 0
    failed_downloads = 0
    for i in range(0, total_tickers, CONFIG["BATCH_SIZE"]):
        batch = tickers[i:i + CONFIG["BATCH_SIZE"]]
        batch_num = i // CONFIG["BATCH_SIZE"] + 1
        total_batches = (total_tickers + CONFIG["BATCH_SIZE"] - 1) // CONFIG["BATCH_SIZE"]
        log_message(f"📦 BATCH {batch_num}/{total_batches} ({len(batch)} saham) - CONCURRENT DOWNLOAD", "info")
        
        download_results = {}
        for ticker in batch:
            data = download_stock_data_concurrent(ticker)
            if data is not None:
                successful_downloads += 1
                download_results[ticker] = data
            else:
                failed_downloads += 1
                download_results[ticker] = None
        
        log_message(f"🔬 ANALISIS TEKNIKAL + SMC ({len([d for d in download_results.values() if d is not None])} saham)...", "processing")
        for ticker, stock_data in download_results.items():
            if stock_data is None: continue
            result = screen_stock_final(ticker, stock_data)
            if result: results.append(result)
        
        if i + CONFIG["BATCH_SIZE"] < total_tickers:
            log_message(f"\n⏳ Menunggu {CONFIG['BATCH_DELAY']} detik...", "info")
            time.sleep(CONFIG["BATCH_DELAY"])
        gc.collect()
    
    print_screening_audit_report()
    
    log_message(f"\n📊 SCREENING SELESAI!", "success")
    log_message(f"✅ Berhasil download: {successful_downloads}/{total_tickers}", "success")
    log_message(f"❌ Gagal download: {failed_downloads}/{total_tickers}", "error")
    log_message(f"🎯 Saham lolos screening: {len(results)}/{successful_downloads} ({len(results)/max(successful_downloads,1)*100:.1f}%)", "success")
    
    if results:
        results.sort(key=lambda x: x["sort_key"], reverse=True)
        results = results[:CONFIG["MAX_OUTPUT"]]
        for idx, res in enumerate(results, 1):
            res["No"] = idx
        
        df_results = pd.DataFrame(results)
        required_columns = ["No", "Emiten", "Close", "MA50", "Vol_Ratio", "ROE", "DE", "Entry", "SL", "TP", "RR", "Status", "Info"]
        display_df = df_results[required_columns].copy()
        
        log_message(f"\n📋 HASIL SCREENING ({len(results)} SAHAM) - 13 KOLOM:", "success")
        print("\n")
        table = tabulate(display_df, headers='keys', tablefmt='fancy_grid', showindex=False, numalign="right")
        table_colored = table.replace("✅ READY", f"{Fore.GREEN}✅ READY{Style.RESET_ALL}")
        print(table_colored + "\n")
        
        # Save Excel (dated filename)
        excel_filename = f"{CONFIG['EXCEL_PREFIX']}_{today_str}.xlsx"
        df_results.to_excel(excel_filename, index=False)
        log_message(f"💾 Hasil Excel disimpan ke: {excel_filename}", "success")
        
        # ✅ SAVE JSON (2 FILE: STABLE + DATED)
        save_json_result(results, "latest_screening.json")  # URL API stabil
        save_json_result(results, f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")  # History
        
        unknown_fund = sum(1 for r in results if "SKIP" in r["Info"] or "UNKNOWN" in r["Info"])
        if unknown_fund > 0:
            log_message(f"\n💡 {unknown_fund} saham adalah Pure Technical (verifikasi manual jika perlu)", "info")
        log_message(f"🎯 Esok hari: Fokus pada {len(results)} saham ini — TIDAK PERLU CARI SAHAM LAIN", "success")
    else:
        log_message("❌ TIDAK ADA SAHAM YANG LOLOS SCREENING HARI INI", "error")
        log_message("💡 Normal! Pasar tidak selalu menyediakan setup valid. Hold cash besok.", "info")
        if CONFIG["FUNDAMENTAL_REQUIRED"]:
            log_message("💡 Tips: Coba set FUNDAMENTAL_REQUIRED = False untuk hasil lebih banyak", "info")
        
        # ✅ Tetap save JSON meski kosong (untuk API consistency)
        save_json_result([], "latest_screening.json")
        save_json_result([], f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")
    
    deleted_files = clean_old_data(max_age_days=CONFIG["MIN_CACHE_RETENTION_DAYS"])
    if deleted_files > 0:
        log_message(f"🧹 Membersihkan {deleted_files} file cache lama (>365 hari)", "info")
    
    elapsed = (time.time() - start_time) / 60
    log_message(f"\n⏱️  Waktu eksekusi: {elapsed:.1f} menit", "success")
    log_message("="*80, "success")
    log_message("✅ STRATEGI KITA: Disiplin pada 7 saham ini — bukan chasing 'cuan cepat'", "success")
    log_message("✅ TUJUAN: Mendapatkan uang via compounding konsisten", "success")
    log_message("="*80, "success")

# =============================================
# EKSEKUSI PROGRAM
# =============================================
if __name__ == "__main__":
    try:
        print(f"\n{Fore.CYAN}" + "="*80)
        print("🚀 S C R E E N E R   T R A D E R   I N D O N E S I A - V E R S I   F I N A L   +   S M C")
        print("   Data dari IPO + MA50 Pullback + SMC Confirmation (BOS/OB/FVG)")
        print(f"   Fundamental: {'WAJIB' if CONFIG['FUNDAMENTAL_REQUIRED'] else 'OPTIONAL (Pure Technical)'}")
        print("   Cache Check: Berdasarkan Tanggal Data (Bukan Tanggal File)")
        print("   API Endpoint: latest_screening.json (URL Stabil)")
        print("="*80 + f"{Style.RESET_ALL}\n")
        main()
        # ✅ PENTING: Exit dengan kode 0 agar GitHub Actions menganggap sukses
        sys.exit(0)
    except KeyboardInterrupt:
        log_message("\n⏹️ Program dihentikan oleh user", "error")
        sys.exit(0)
    except Exception as e:
        log_message(f"\n💥 ERROR KRITIS: {str(e)}", "error")
        import traceback
        traceback.print_exc()
        sys.exit(1)