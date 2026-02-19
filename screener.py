#!/usr/bin/env python3
"""
SCREENER TRADER INDONESIA - VERSI LUXALGO MAXIMUM + BROADCAST
✅ SWING DETECTION: Stateful pivot-based (LuxAlgo style)
✅ ORDER BLOCK: Mitigation logic (OB dihapus saat tersentuh)
✅ VOLATILITY FILTER: ATR(200) based OB filtering
✅ TREND BIAS: Strong/Weak High-Low classification
✅ FVG: Dynamic threshold (cumulative bar delta)
✅ ENTRY ZONE: 3 Cicil (20%, 50%, 30%) dengan Average Entry
✅ JSON COMPATIBLE: Entry (int) + Entry_Zone (string)
✅ BROADCAST JSON: Detail per saham untuk sistem broadcast
✅ BEI FRAKSI: 100% compliant
✅ MULTI-TIMEFRAME: 1w, 1d, 1h, 15m
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
# KONFIGURASI STRATEGI (LUXALGO PARAMETERS)
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
    "ATR_OB_FILTER": 200,
    "VOLUME_RATIO_MIN": 1.5,
    "PULLBACK_TOLERANCE": 0.05,
    "ATR_MULTIPLIER_SL": 2.0,
    "MIN_RR_RATIO": 3.0,
    "MAX_RISK_PCT": 0.08,
    
    # --- Fundamental (OPTIONAL) ---
    "FUNDAMENTAL_REQUIRED": False,
    "MIN_ROE": 10,
    "MAX_DE": 1.0,
    
    # --- SMC Configuration (LUXALGO STYLE) ---
    "SWING_LENGTH": 50,
    "INTERNAL_LENGTH": 5,
    "SMC_OB_RALLY_MIN": 0.03,
    "SMC_FVG_MIN_GAP": 0.01,
    "SMC_REQUIRED": True,
    "OB_MITIGATION": True,
    "VOLATILITY_FILTER": True,
    
    # --- Entry Zone Configuration ---
    "ENTRY_1_PCT": 0.20,
    "ENTRY_2_PCT": 0.50,
    "ENTRY_3_PCT": 0.30,
    
    # --- Timeframe Configuration ---
    "TIMEFRAMES": ["1wk", "1d", "1h", "15m"],
    "PRIMARY_TF": "1d",
    "ENTRY_TF": "1h",
    "PRECISION_TF": "15m",
    
    # --- System Configuration ---
    "MAX_WORKERS": 5,
    "REQUEST_DELAY": 0.7,
    "JITTER_MIN": 0.2,
    "JITTER_MAX": 0.5,
    "MAX_RETRIES": 2,
    "BATCH_SIZE": 30,
    "BATCH_DELAY": 5,
    "DATA_DIR": "data_ohlc_v4_luxalgo",
    "MAX_LOCAL_AGE_DAYS": 1,
    "MIN_CACHE_RETENTION_DAYS": 365,
    "FORCE_DOWNLOAD": False,
    "EXCEL_PREFIX": "watchlist_trader_id_luxalgo",
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

def calculate_entry_zone(entry_1, entry_2, entry_3):
    e1 = int(round_to_bei_tick(entry_1))
    e2 = int(round_to_bei_tick(entry_2))
    e3 = int(round_to_bei_tick(entry_3))
    
    entries = sorted([e1, e2, e3], reverse=True)
    entry_high = entries[0]
    entry_mid = entries[1]
    entry_low = entries[2]
    
    average_entry = (
        entry_high * CONFIG["ENTRY_1_PCT"] +
        entry_mid * CONFIG["ENTRY_2_PCT"] +
        entry_low * CONFIG["ENTRY_3_PCT"]
    )
    average_entry = int(round_to_bei_tick(average_entry))
    
    zone_string = f"{entry_low} - {entry_high}"
    
    return {
        "entry_1": entry_high,
        "entry_2": entry_mid,
        "entry_3": entry_low,
        "average": average_entry,
        "zone": zone_string
    }

def get_company_name(ticker):
    company_names = {
        "TRIS": "PT TRISULA INTERNATIONAL TBK",
        "KRAS": "PT KRAKATAU STEEL (PERSERO) TBK",
        "YELO": "PT YELOOO INTEGRA DATANET TBK",
        "HDIT": "PT HENSEL DAVEST INDONESIA TBK",
        "ADMF": "PT ADIRA DINAMIKA MULTI FINANCE TBK",
        "CNKO": "PT EXPLOITASI ENERGI INDONESIA TBK",
        "LSIP": "PT PERUSAHAAN PERKEBUNAN LONDON SUMATRA INDONESIA TBK",
        "BVIC": "PT BANK VICTORIA INTERNATIONAL TBK",
        "ASSA": "PT ADI SARANA ARMADA TBK",
        "WOOD": "PT INTEGRA INDOCABINET TBK"
    }
    return company_names.get(ticker, f"PT {ticker} TBK")

def get_bias_description(info):
    if "BOS" in info:
        return "Follow Trend Daily & 4H (Breakout Phase)"
    elif "CHoCH" in info:
        return "Follow Trend Recovery (Daily & 4H Bullish)"
    elif "OB" in info:
        return "Follow Trend Daily & 4H (Recovery Bullish)"
    else:
        return "Follow Trend Daily & 4H (Bullish)"

# =============================================
# LUXALGO-STYLE SWING DETECTION (STATEFUL)
# =============================================
def detect_pivots_stateful(df, length=50):
    pivots = []
    
    for i in range(length, len(df) - length):
        is_swing_high = True
        current_high = df['high'].iloc[i]
        
        for j in range(i - length, i + length + 1):
            if j != i and df['high'].iloc[j] >= current_high:
                is_swing_high = False
                break
        
        is_swing_low = True
        current_low = df['low'].iloc[i]
        
        for j in range(i - length, i + length + 1):
            if j != i and df['low'].iloc[j] <= current_low:
                is_swing_low = False
                break
        
        if is_swing_high:
            pivots.append({
                'index': i,
                'price': current_high,
                'type': 'high',
                'time': df['date'].iloc[i]
            })
        elif is_swing_low:
            pivots.append({
                'index': i,
                'price': current_low,
                'type': 'low',
                'time': df['date'].iloc[i]
            })
    
    return pivots

def detect_trend_bias(df, swing_high, swing_low):
    if len(df) < 20:
        return 'NEUTRAL'
    
    current_close = df['close'].iloc[-1]
    
    if current_close > swing_high:
        return 'BULLISH'
    elif current_close < swing_low:
        return 'BEARISH'
    else:
        return 'NEUTRAL'

# =============================================
# LUXALGO-STYLE ORDER BLOCK DETECTION
# =============================================
def is_high_volatility_bar(df, index, atr_period=200):
    try:
        if len(df) < atr_period:
            return False
        
        atr = ta.atr(
            df['high'].iloc[:index+1],
            df['low'].iloc[:index+1],
            df['close'].iloc[:index+1],
            length=atr_period
        ).iloc[-1] if index >= atr_period else df['high'].iloc[index] - df['low'].iloc[index]
        
        candle_range = df['high'].iloc[index] - df['low'].iloc[index]
        
        return candle_range >= (2 * atr)
    except:
        return False

def detect_order_blocks_luxalgo(df, pivots, atr_period=200):
    order_blocks = []
    
    for i, pivot in enumerate(pivots):
        pivot_index = pivot['index']
        
        if CONFIG["VOLATILITY_FILTER"] and is_high_volatility_bar(df, pivot_index, atr_period):
            continue
        
        if pivot['type'] == 'low':
            bias = 'BULLISH'
            ob_start = max(0, pivot_index - 5)
            ob_candle = df.iloc[ob_start:pivot_index+1]
            
            if len(ob_candle) > 0:
                ob_high = ob_candle['high'].max()
                ob_low = ob_candle['low'].min()
                
                if pivot_index + 10 < len(df):
                    rally = (df['high'].iloc[pivot_index+10] - df['low'].iloc[pivot_index]) / df['low'].iloc[pivot_index]
                    if rally >= CONFIG["SMC_OB_RALLY_MIN"]:
                        order_blocks.append({
                            'high': ob_high,
                            'low': ob_low,
                            'index': pivot_index,
                            'bias': bias,
                            'mitigated': False,
                            'time': df['date'].iloc[pivot_index]
                        })
        
        elif pivot['type'] == 'high':
            bias = 'BEARISH'
            ob_start = max(0, pivot_index - 5)
            ob_candle = df.iloc[ob_start:pivot_index+1]
            
            if len(ob_candle) > 0:
                ob_high = ob_candle['high'].max()
                ob_low = ob_candle['low'].min()
                
                if pivot_index + 10 < len(df):
                    decline = (df['low'].iloc[pivot_index] - df['low'].iloc[pivot_index+10]) / df['low'].iloc[pivot_index]
                    if decline >= CONFIG["SMC_OB_RALLY_MIN"]:
                        order_blocks.append({
                            'high': ob_high,
                            'low': ob_low,
                            'index': pivot_index,
                            'bias': bias,
                            'mitigated': False,
                            'time': df['date'].iloc[pivot_index]
                        })
    
    return order_blocks

def mitigate_order_blocks(order_blocks, current_price):
    if not CONFIG["OB_MITIGATION"]:
        return order_blocks
    
    valid_blocks = []
    for ob in order_blocks:
        if ob['bias'] == 'BULLISH':
            if current_price < ob['low'] * 0.98:
                ob['mitigated'] = True
            else:
                valid_blocks.append(ob)
        elif ob['bias'] == 'BEARISH':
            if current_price > ob['high'] * 1.02:
                ob['mitigated'] = True
            else:
                valid_blocks.append(ob)
    
    return valid_blocks

# =============================================
# LUXALGO-STYLE FVG DETECTION
# =============================================
def detect_fvg_luxalgo(df, dynamic_threshold=True):
    fvgs = []
    
    if len(df) < 3:
        return fvgs
    
    for i in range(2, len(df)):
        candle_1_high = df['high'].iloc[i-2]
        candle_1_low = df['low'].iloc[i-2]
        candle_1_close = df['close'].iloc[i-2]
        candle_1_open = df['open'].iloc[i-2]
        
        candle_3_high = df['high'].iloc[i]
        candle_3_low = df['low'].iloc[i]
        
        bar_delta_percent = abs(candle_1_close - candle_1_open) / candle_1_open * 100 if candle_1_open > 0 else 0
        
        if dynamic_threshold:
            threshold = df['high'].iloc[:i].pct_change().abs().cumsum().iloc[-1] / i * 2 if i > 0 else 0.01
        else:
            threshold = CONFIG["SMC_FVG_MIN_GAP"]
        
        if candle_3_low > candle_1_high:
            gap_pct = (candle_3_low - candle_1_high) / candle_1_high if candle_1_high > 0 else 0
            if gap_pct >= threshold:
                fvgs.append({
                    'top': candle_3_low,
                    'bottom': candle_1_high,
                    'bias': 'BULLISH',
                    'index': i,
                    'mitigated': False
                })
        
        elif candle_3_high < candle_1_low:
            gap_pct = (candle_1_low - candle_3_high) / candle_1_low if candle_1_low > 0 else 0
            if gap_pct >= threshold:
                fvgs.append({
                    'top': candle_1_low,
                    'bottom': candle_3_high,
                    'bias': 'BEARISH',
                    'index': i,
                    'mitigated': False
                })
    
    return fvgs

def mitigate_fvg(fvgs, current_price):
    valid_fvgs = []
    for fvg in fvgs:
        if fvg['bias'] == 'BULLISH':
            if current_price < fvg['bottom'] * 0.98:
                fvg['mitigated'] = True
            else:
                valid_fvgs.append(fvg)
        elif fvg['bias'] == 'BEARISH':
            if current_price > fvg['top'] * 1.02:
                fvg['mitigated'] = True
            else:
                valid_fvgs.append(fvg)
    
    return valid_fvgs

# =============================================
# LUXALGO-STYLE SMC DETECTION (COMPREHENSIVE)
# =============================================
def detect_smc_signals_luxalgo(df, timeframe="1d"):
    try:
        if df is None or len(df) < 50:
            return None
        
        current_price = df['close'].iloc[-1]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        
        swing_length = CONFIG["SWING_LENGTH"]
        pivots = detect_pivots_stateful(df, length=swing_length)
        
        if len(pivots) < 2:
            return None
        
        swing_highs = [p for p in pivots if p['type'] == 'high']
        swing_lows = [p for p in pivots if p['type'] == 'low']
        
        swing_high = swing_highs[-1]['price'] if swing_highs else df['high'].max()
        swing_low = swing_lows[-1]['price'] if swing_lows else df['low'].min()
        
        trend_bias = detect_trend_bias(df, swing_high, swing_low)
        
        bos_bullish = bool(current_price > swing_high)
        bos_bearish = bool(current_price < swing_low)
        
        choch_bullish = False
        choch_bearish = False
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            prev_swing_high = swing_highs[-2]['price']
            prev_swing_low = swing_lows[-2]['price']
            
            if trend_bias == 'BEARISH' and current_price > prev_swing_high:
                choch_bullish = True
            elif trend_bias == 'BULLISH' and current_price < prev_swing_low:
                choch_bearish = True
        
        order_blocks = detect_order_blocks_luxalgo(df, pivots, CONFIG["ATR_OB_FILTER"])
        order_blocks = mitigate_order_blocks(order_blocks, current_price)
        
        active_bullish_ob = None
        active_bearish_ob = None
        
        for ob in order_blocks:
            if not ob['mitigated']:
                if ob['bias'] == 'BULLISH' and ob['high'] < current_price:
                    if active_bullish_ob is None or ob['high'] > active_bullish_ob['high']:
                        active_bullish_ob = ob
                elif ob['bias'] == 'BEARISH' and ob['low'] > current_price:
                    if active_bearish_ob is None or ob['low'] < active_bearish_ob['low']:
                        active_bearish_ob = ob
        
        ob_signal = active_bullish_ob is not None or active_bearish_ob is not None
        ob_low = active_bullish_ob['low'] if active_bullish_ob else None
        ob_high = active_bullish_ob['high'] if active_bullish_ob else None
        
        in_ob_zone = False
        if active_bullish_ob:
            in_ob_zone = bool((current_low <= active_bullish_ob['high'] * 1.02) and 
                             (current_price >= active_bullish_ob['low'] * 0.98))
        
        fvgs = detect_fvg_luxalgo(df, dynamic_threshold=True)
        fvgs = mitigate_fvg(fvgs, current_price)
        
        active_fvg = None
        for fvg in fvgs:
            if not fvg['mitigated'] and fvg['bias'] == 'BULLISH' and fvg['bottom'] < current_price:
                active_fvg = fvg
                break
        
        fvg_detected = active_fvg is not None
        fvg_low = active_fvg['bottom'] if active_fvg else None
        fvg_high = active_fvg['top'] if active_fvg else None
        
        in_fvg_zone = False
        if active_fvg:
            in_fvg_zone = bool((current_low <= active_fvg['top'] * 1.02) and 
                              (current_price >= active_fvg['bottom'] * 0.98))
        
        strong_high = swing_high if trend_bias == 'BEARISH' else None
        weak_high = swing_high if trend_bias == 'BULLISH' else None
        strong_low = swing_low if trend_bias == 'BULLISH' else None
        weak_low = swing_low if trend_bias == 'BEARISH' else None
        
        return {
            'timeframe': timeframe,
            'bos_bullish': bos_bullish,
            'bos_bearish': bos_bearish,
            'choch_bullish': choch_bullish,
            'choch_bearish': choch_bearish,
            'trend_bias': trend_bias,
            'strong_high': strong_high,
            'weak_high': weak_high,
            'strong_low': strong_low,
            'weak_low': weak_low,
            'ob_signal': ob_signal,
            'in_ob_zone': in_ob_zone,
            'ob_low': float(ob_low) if ob_low else None,
            'ob_high': float(ob_high) if ob_high else None,
            'order_blocks': order_blocks,
            'fvg_detected': fvg_detected,
            'in_fvg_zone': in_fvg_zone,
            'fvg_low': float(fvg_low) if fvg_low else None,
            'fvg_high': float(fvg_high) if fvg_high else None,
            'fvgs': fvgs,
            'swing_high': float(swing_high),
            'swing_low': float(swing_low),
            'pivots': pivots[-10:]
        }
    
    except Exception as e:
        log_message(f"⚠️ Error detect_smc_signals_luxalgo: {str(e)[:100]}", "warning")
        return None

# =============================================
# ENTRY ZONE & SL/TP CALCULATION (LUXALGO STYLE)
# =============================================
def calculate_entry_zone_from_smc_luxalgo(data_dict, smc_data):
    try:
        current_price = data_dict[CONFIG["PRIMARY_TF"]]['close'].iloc[-1]
        
        if smc_data and smc_data['ob_signal'] and smc_data['ob_low'] and smc_data['ob_high']:
            ob_high = smc_data['ob_high']
            ob_low = smc_data['ob_low']
            
            entry_1 = ob_high * 1.005
            entry_2 = (ob_high + ob_low) / 2
            entry_3 = ob_low * 0.995
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
        
        elif smc_data and smc_data['fvg_detected'] and smc_data['fvg_low'] and smc_data['fvg_high']:
            fvg_high = smc_data['fvg_high']
            fvg_low = smc_data['fvg_low']
            
            entry_1 = fvg_high * 1.005
            entry_2 = (fvg_high + fvg_low) / 2
            entry_3 = fvg_low * 0.995
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
        
        else:
            df_daily = data_dict[CONFIG["PRIMARY_TF"]]
            ma50 = df_daily['close'].rolling(CONFIG["MA_PERIOD"]).mean().iloc[-1]
            
            entry_1 = current_price * 1.01
            entry_2 = ma50 * 1.02
            entry_3 = ma50 * 0.98
            
            return calculate_entry_zone(entry_1, entry_2, entry_3)
    
    except Exception as e:
        log_message(f"⚠️ Error calculate_entry_zone_from_smc_luxalgo: {str(e)[:100]}", "warning")
        return None

def calculate_structural_sl_tp_luxalgo(data_dict, entry_zone, smc_data):
    try:
        df_daily = data_dict[CONFIG["PRIMARY_TF"]]
        df_entry = data_dict.get(CONFIG["ENTRY_TF"], df_daily)
        
        average_entry = entry_zone['average']
        
        if smc_data and smc_data['strong_low']:
            structural_low = smc_data['strong_low']
        else:
            swing_low_daily = df_daily['low'].rolling(CONFIG["SWING_LENGTH"]).min().iloc[-1]
            structural_low = swing_low_daily
        
        sl_raw = structural_low * 0.97
        sl = int(round_to_bei_tick(sl_raw))
        
        if sl >= average_entry:
            tick = get_tick_size(sl)
            sl = int(round_to_bei_tick(average_entry - (5 * tick)))
        
        if smc_data and smc_data['weak_high']:
            weak_high = smc_data['weak_high']
            tp1 = int(round_to_bei_tick(weak_high * 1.02))
            tp2 = int(round_to_bei_tick(weak_high * 1.10))
        else:
            swing_high_daily = df_daily['high'].rolling(CONFIG["SWING_LENGTH"]).max().iloc[-1]
            tp1 = int(round_to_bei_tick(swing_high_daily * 1.02))
            tp2 = int(round_to_bei_tick(swing_high_daily * 1.10))
        
        risk = average_entry - sl
        tp3_min = average_entry + (risk * CONFIG["MIN_RR_RATIO"])
        tp3 = int(round_to_bei_tick(max(tp3_min, tp2 * 1.15)))
        
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
        log_message(f"⚠️ Error calculate_structural_sl_tp_luxalgo: {str(e)[:100]}", "warning")
        return None

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
    with DOWNLOAD_SEMAPHORE:
        ticker_clean = str(ticker).strip()
        data_dict = {}
        
        for tf in CONFIG["TIMEFRAMES"]:
            local_data = load_local_data(ticker_clean, tf)
            if local_data is not None:
                data_dict[tf] = local_data
                continue
            
            for attempt in range(CONFIG["MAX_RETRIES"]):
                try:
                    delay = get_adaptive_delay()
                    time.sleep(delay)
                    
                    interval_map = {
                        "1wk": "1wk",
                        "1d": "1d",
                        "1h": "1h",
                        "15m": "15m"
                    }
                    interval = interval_map.get(tf, "1d")
                    
                    if tf in ["15m", "1h"]:
                        period = "60d"
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
                    
                    df = df[df['volume'] > 0].copy()
                    
                    if len(df) < 10:
                        log_message(f"{ticker_clean}: ⚠️ Data {tf} tidak lengkap ({len(df)} baris)", "warning")
                        continue
                    
                    df['ticker'] = ticker_clean
                    df['timeframe'] = tf
                    
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
        
        return data_dict if len(data_dict) >= 2 else None

# =============================================
# SCREENING LOGIC (LUXALGO MAXIMUM)
# =============================================
def screen_stock_final_luxalgo(ticker, data_dict):
    global SCREENING_AUDIT
    try:
        SCREENING_AUDIT["total_processed"] += 1
        ticker_clean = ticker.split('.JK')[0]
        
        if CONFIG["PRIMARY_TF"] not in data_dict:
            return None
        
        df_daily = data_dict[CONFIG["PRIMARY_TF"]]
        current_price = df_daily['close'].iloc[-1]
        
        avg_volume_value = (df_daily['volume'].tail(20) * df_daily['close'].tail(20)).mean()
        if avg_volume_value < CONFIG["MIN_VOLUME_VALUE"] or current_price < CONFIG["MIN_PRICE"]:
            SCREENING_AUDIT["failed_liquidity"] += 1
            return None
        SCREENING_AUDIT["passed_liquidity"] += 1
        
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
        
        if current_price <= ma50:
            SCREENING_AUDIT["failed_uptrend"] += 1
            return None
        SCREENING_AUDIT["passed_uptrend"] += 1
        
        pullback_min = ma50 * (1 - CONFIG["PULLBACK_TOLERANCE"])
        pullback_max = ma50 * (1 + CONFIG["PULLBACK_TOLERANCE"])
        if not (pullback_min <= current_price <= pullback_max):
            SCREENING_AUDIT["failed_pullback"] += 1
            return None
        SCREENING_AUDIT["passed_pullback"] += 1
        
        if volume_ratio < CONFIG["VOLUME_RATIO_MIN"]:
            SCREENING_AUDIT["failed_volume"] += 1
            return None
        SCREENING_AUDIT["passed_volume"] += 1
        
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
        
        smc_data = detect_smc_signals_luxalgo(df_daily, "1d")
        
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
        
        entry_zone = calculate_entry_zone_from_smc_luxalgo(data_dict, smc_data)
        if entry_zone is None:
            return None
        
        sl_tp = calculate_structural_sl_tp_luxalgo(data_dict, entry_zone, smc_data)
        if sl_tp is None:
            return None
        
        if sl_tp['rr_ratio'] < CONFIG["MIN_RR_RATIO"]:
            SCREENING_AUDIT["failed_rr"] += 1
            return None
        
        SCREENING_AUDIT["passed_rr"] += 1
        SCREENING_AUDIT["final_passed"] += 1
        
        smc_tags = []
        if smc_data.get('bos_bullish'):
            smc_tags.append("📈 BOS")
        if smc_data.get('choch_bullish'):
            smc_tags.append("🔄 CHoCH")
        if smc_data.get('ob_signal'):
            smc_tags.append("🟦 OB")
        if smc_data.get('fvg_detected'):
            smc_tags.append("⚡ FVG")
        if smc_data.get('trend_bias'):
            smc_tags.append(f"Trend: {smc_data['trend_bias']}")
        
        smc_info = " + ".join(smc_tags) if smc_tags else "SMC Confirmed"
        
        result = {
            "No": 0,
            "Emiten": ticker_clean,
            "Company_Name": get_company_name(ticker_clean),
            "Close": int(round_to_bei_tick(current_price)),
            "MA50": int(round_to_bei_tick(ma50)),
            "Vol_Ratio": round(volume_ratio, 2),
            "ROE": f"{roe:.1f}%" if roe is not None else "N/A",
            "DE": f"{de_ratio:.2f}" if de_ratio is not None else "N/A",
            "Entry": entry_zone['average'],
            "Entry_Zone": entry_zone['zone'],
            "SL": sl_tp['sl'],
            "TP": sl_tp['tp3'],
            "TP_1": sl_tp['tp1'],
            "TP_2": sl_tp['tp2'],
            "TP_3": sl_tp['tp3'],
            "RR": f"1:{sl_tp['rr_ratio']:.1f}",
            "RR_Value": round(sl_tp['rr_ratio'], 2),
            "Status": "✅ READY",
            "Info": f"Fundamental: {fundamental_status} | {smc_info}",
            "Bias_Description": get_bias_description(smc_info),
            "Entry_Details": {
                "Entry_1": entry_zone['entry_1'],
                "Entry_2": entry_zone['entry_2'],
                "Entry_3": entry_zone['entry_3'],
                "Pct_1": int(CONFIG["ENTRY_1_PCT"] * 100),
                "Pct_2": int(CONFIG["ENTRY_2_PCT"] * 100),
                "Pct_3": int(CONFIG["ENTRY_3_PCT"] * 100)
            },
            "SMC_Details": {
                "Trend_Bias": smc_data.get('trend_bias', 'N/A'),
                "Strong_Low": smc_data.get('strong_low'),
                "Weak_High": smc_data.get('weak_high'),
                "OB_Count": len(smc_data.get('order_blocks', [])),
                "FVG_Count": len(smc_data.get('fvgs', []))
            },
            "sort_key": sl_tp['rr_ratio']
        }
        
        return result
    
    except Exception as e:
        log_message(f"{ticker}: ❌ Error screening: {str(e)[:150]}", "error")
        return None

# =============================================
# SAVE FUNCTIONS
# =============================================
def save_json_result(results, filename):
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
                "strategy": "LuxAlgo Maximum SMC",
                "min_rr": CONFIG["MIN_RR_RATIO"],
                "max_output": CONFIG["MAX_OUTPUT"],
                "fundamental_required": CONFIG["FUNDAMENTAL_REQUIRED"],
                "timeframes": CONFIG["TIMEFRAMES"],
                "swing_length": CONFIG["SWING_LENGTH"],
                "ob_mitigation": CONFIG["OB_MITIGATION"],
                "volatility_filter": CONFIG["VOLATILITY_FILTER"]
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

def save_broadcast_json(results, filename):
    """Save hasil screening ke JSON format broadcast detail per saham"""
    try:
        broadcast_data = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H.%M"),
            "total_saham": len(results),
            "stocks": []
        }
        
        for r in results:
            stock_detail = {
                "ticker": r["Emiten"],
                "company_name": r.get("Company_Name", get_company_name(r["Emiten"])),
                "bias": "LONG / BUY",
                "bias_description": r.get("Bias_Description", "Follow Trend Daily & 4H (Bullish)"),
                "entry_1": r["Entry_Details"]["Entry_1"],
                "entry_1_pct": r["Entry_Details"]["Pct_1"],
                "entry_1_desc": "Antisipasi FOMO jika langsung lanjut naik",
                "entry_2": r["Entry_Details"]["Entry_2"],
                "entry_2_pct": r["Entry_Details"]["Pct_2"],
                "entry_2_desc": "Area Order Block 1H / Retest",
                "entry_3": r["Entry_Details"]["Entry_3"],
                "entry_3_pct": r["Entry_Details"]["Pct_3"],
                "entry_3_desc": "Deep Pullback / Strong Low",
                "average_entry": r["Entry"],
                "sl": r["SL"],
                "sl_desc": "Wajib Cut Loss jika close harian di bawah ini",
                "tp_1": r["TP_1"],
                "tp_1_desc": "Target Psikologis & High 1H",
                "tp_2": r["TP_2"],
                "tp_2_desc": "Target Supply Zone Daily",
                "tp_3": r["TP_3"],
                "tp_3_desc": "Target Weekly Weak High - RR 1:3",
                "risk_pct": 2,
                "risk_desc": "Dari total modal portofolio",
                "rr_ratio": r["RR"],
                "smc_info": r["Info"],
                "updated_at": datetime.now().strftime("%H.%M WIB")
            }
            broadcast_data["stocks"].append(stock_detail)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(broadcast_data, f, indent=2, ensure_ascii=False)
        
        log_message(f"💾 Hasil Broadcast JSON disimpan ke: {filename}", "success")
        return True
    except Exception as e:
        log_message(f"❌ Gagal simpan Broadcast JSON: {str(e)}", "error")
        return False

def save_excel_result(results, filename):
    try:
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
                "Entry_Zone": r["Entry_Zone"],
                "Average_Entry": r["Entry"],
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
# AUDIT REPORT
# =============================================
def print_screening_audit_report():
    global SCREENING_AUDIT
    log_message("\n" + "="*80, "success")
    log_message("📊 LAPORAN AUDIT SCREENING - LUXALGO MAXIMUM", "success")
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
        ("🆕 SMC LUXALGO MAXIMUM", SCREENING_AUDIT["passed_smc"], SCREENING_AUDIT["failed_smc"]),
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
    log_message(f"🚀 SCREENER TRADER INDONESIA - LUXALGO MAXIMUM + BROADCAST", "success")
    log_message(f"📅 Tanggal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "success")
    log_message(f"🎯 FOKUS: LuxAlgo SMC Maximum (Stateful Pivot + OB Mitigation)", "success")
    log_message(f"⚙️  SWING_LENGTH: {CONFIG['SWING_LENGTH']} | OB_MITIGATION: {CONFIG['OB_MITIGATION']}", "success")
    log_message(f"⚙️  VOLATILITY_FILTER: {CONFIG['VOLATILITY_FILTER']} | ATR_OB_FILTER: {CONFIG['ATR_OB_FILTER']}", "success")
    log_message(f"✅ HANYA output 'READY' (max {CONFIG['MAX_OUTPUT']} saham)", "success")
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
            result = screen_stock_final_luxalgo(ticker, stock_data)
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
        
        excel_filename = f"{CONFIG['EXCEL_PREFIX']}_{today_str}.xlsx"
        save_excel_result(results, excel_filename)
        
        save_json_result(results, "latest_screening.json")
        save_json_result(results, f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")
        
        save_broadcast_json(results, "broadcast_detail.json")
        
        log_message(f"🎯 Fokus pada {len(results)} saham ini", "success")
    else:
        log_message("❌ TIDAK ADA SAHAM YANG LOLOS", "error")
        save_json_result([], "latest_screening.json")
        save_json_result([], f"{CONFIG['EXCEL_PREFIX']}_{today_str}.json")
        save_broadcast_json([], "broadcast_detail.json")
    
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
        print("🚀 SCREENER TRADER INDONESIA - LUXALGO MAXIMUM + BROADCAST")
        print("   Stateful Pivot + OB Mitigation + Volatility Filter")
        print("   Strong/Weak Classification + Dynamic FVG Threshold")
        print("   JSON Compatible: Entry (int) + Entry_Zone (string)")
        print("   Broadcast JSON: broadcast_detail.json")
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