#!/usr/bin/env python3
"""
================================================================================
RANKER - EMPIRICAL HIT RATE + FOREIGN FLOW ANALYSIS
================================================================================
✅ Baca latest_screening.json (hasil dari screener.py)
✅ Fetch historical data dari Stockbit API untuk setiap saham
✅ Hitung Empirical Hit Rate (pola divergensi akumulasi)
✅ Hitung Confidence Ranking (0-100 score)
✅ Hitung Foreign Flow Metrics (1 hari, 1 minggu)
✅ Output ranked_screening.json (lengkap dengan semua data screener + ranking)

Author: Stock Screener Team
Version: 1.0.0
Last Updated: 2026-02-25
================================================================================
"""

# =============================================
# IMPORTS & CONFIGURATION
# =============================================
import json
import requests
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "STOCKBIT_BASE_URL": "https://exodus.stockbit.com/company-price-feed/historical/summary",
    "REQUEST_DELAY": 1.5,  # Delay antar request (detik)
    "MAX_RETRIES": 3,
    "TIMEOUT": 30,
    "INPUT_FILE": "latest_screening.json",
    "OUTPUT_FILE": "ranked_screening.json",
    "HISTORY_DIR": "history",
    
    # Hit Rate Calculation
    "HIT_RATE_WINDOW": 12,  # Hari untuk analisis historis
    "MIN_SIGNALS_FOR_HIT_RATE": 2,  # Minimal sinyal untuk hit rate valid
    
    # Thresholds
    "FOREIGN_SIGNIFICANT_THRESHOLD": 10_000_000_000,  # Rp10Miliar
    "PRICE_DROP_THRESHOLD": -5.0,  # -5%
    "REBOUND_THRESHOLD": 10.0,  # +10% rebound dalam 3 hari
    "VOLUME_RATIO_THRESHOLD": 1.0,  # Volume > average
    
    # Ranking Weights
    "WEIGHT_LIQUIDITY": 25,
    "WEIGHT_FOREIGN_PARTICIPATION": 25,
    "WEIGHT_HIT_RATE": 40,
    "WEIGHT_RECENCY": 10,
}

# =============================================
# CLASS 1: STOCKBIT API FETCHER
# =============================================
class StockbitFetcher:
    """
    Fetch historical data dari Stockbit API dengan throttling & retry
    """
    
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv('STOCKBIT_BEARER_TOKEN')
        if not self.token:
            raise ValueError(
                "STOCKBIT_BEARER_TOKEN not set! "
                "Please set it in GitHub Secrets or environment variables."
            )
        
        self.headers = {
            'accept': 'application/json',
            'authorization': f'Bearer {self.token}',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch(self, symbol: str, period: str = 'daily', 
              limit: int = 50, days_back: int = 60) -> Optional[Dict]:
        """
        Fetch historical data dari Stockbit API
        
        Args:
            symbol: Stock code (e.g., 'MINA')
            period: 'daily' or 'weekly'
            limit: Number of items per page
            days_back: How many days to look back for date range
        
        Returns:
            API response dict or None if error
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        period_map = {
            'daily': 'HS_PERIOD_DAILY',
            'weekly': 'HS_PERIOD_WEEKLY'
        }
        
        params = {
            'period': period_map.get(period, 'HS_PERIOD_DAILY'),
            'start_date': start_date,
            'end_date': end_date,
            'limit': limit,
            'page': 1
        }
        
        url = f"{CONFIG['STOCKBIT_BASE_URL']}/{symbol}"
        
        for attempt in range(CONFIG['MAX_RETRIES']):
            try:
                # Throttling
                if attempt > 0:
                    wait_time = CONFIG['REQUEST_DELAY'] * (attempt + 1)
                    logger.warning(f"⏳ Retry {attempt + 1}/{CONFIG['MAX_RETRIES']} for {symbol}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    time.sleep(CONFIG['REQUEST_DELAY'])
                
                response = self.session.get(url, params=params, timeout=CONFIG['TIMEOUT'])
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('message') != 'Successfully get the historical summary':
                    logger.warning(f"⚠️ API returned unexpected message for {symbol}: {data.get('message')}")
                    return None
                
                return data
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.error(f"🚨 Rate limit hit for {symbol}. Waiting...")
                    time.sleep(5)
                    continue
                logger.error(f"❌ HTTP Error for {symbol}: {str(e)}")
                return None
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request Error for {symbol}: {str(e)}")
                if attempt < CONFIG['MAX_RETRIES'] - 1:
                    continue
                return None
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON Decode Error for {symbol}: {str(e)}")
                return None
        
        logger.error(f"❌ All retries failed for {symbol}")
        return None


# =============================================
# CLASS 2: DATA PARSER
# =============================================
class DataParser:
    """
    Parse Stockbit API response ke format standar
    """
    
    @staticmethod
    def parse(api_response: Dict, period: str = 'daily') -> List[Dict]:
        """
        Parse Stockbit API response to standardized format
        
        Args:
            api_response: Raw JSON from Stockbit API
            period: 'daily' or 'weekly'
        
        Returns:
            List of standardized data dicts
        """
        if not api_response:
            return []
        
        if api_response.get('message') != 'Successfully get the historical summary':
            return []
        
        results = []
        data_result = api_response.get('data', {}).get('result', [])
        
        for item in data_result:
            # Skip empty/invalid entries
            if not item.get('date') or item.get('volume') == 0:
                continue
            
            try:
                results.append({
                    'date': item['date'],
                    'ohlc': {
                        'open': item.get('open', 0),
                        'high': item.get('high', 0),
                        'low': item.get('low', 0),
                        'close': item.get('close', 0)
                    },
                    'volume': item.get('volume', 0),
                    'frequency': item.get('frequency', 0),
                    'value': item.get('value', 0),
                    'foreign': {
                        'buy': item.get('foreign_buy', 0),
                        'sell': item.get('foreign_sell', 0),
                        'net': item.get('net_foreign', 0)
                    },
                    'change': {
                        'point': item.get('change', 0),
                        'percent': item.get('change_percentage', 0)
                    },
                    'average': item.get('average', 0),
                    'period': period
                })
            except (KeyError, TypeError) as e:
                logger.warning(f"⚠️ Skip invalid data entry: {str(e)}")
                continue
        
        # Sort by date descending (newest first)
        results.sort(key=lambda x: x['date'], reverse=True)
        
        return results


# =============================================
# CLASS 3: EMPIRICAL HIT RATE CALCULATOR
# =============================================
class HitRateCalculator:
    """
    Hitung Empirical Hit Rate untuk setiap saham
    Pattern: Foreign Buy Signifikan + Harga Turun >5% → Rebound >10% dalam 3 hari
    """
    
    @staticmethod
    def calculate(history: List[Dict], 
                  window: int = None,
                  min_signals: int = None) -> Dict:
        """
        Calculate empirical hit rate for "Divergensi Akumulasi" pattern
        
        Args:
            history: List of parsed data dicts (sorted newest first)
            window: Lookback period for analysis
            min_signals: Minimum signals needed for valid hit rate
        
        Returns:
            Dict with hit_rate, total_signals, success_count
        """
        window = window or CONFIG['HIT_RATE_WINDOW']
        min_signals = min_signals or CONFIG['MIN_SIGNALS_FOR_HIT_RATE']
        
        if len(history) < window + 3:  # Need room for follow-through
            return {
                'hit_rate': None,
                'total_signals': 0,
                'success_count': 0,
                'note': f'Insufficient data (need {window + 3} days, have {len(history)})'
            }
        
        # Reverse to chronological order for easier indexing
        data = history[::-1]
        
        signals = []
        
        for i in range(len(data) - window - 3):
            day = data[i]
            
            # Detect "Divergensi Akumulasi" pattern
            foreign_net = day['foreign']['net']
            change_pct = day['change']['percent']
            volume = day['volume']
            
            # Calculate average volume for significance check
            avg_volume = sum(d['volume'] for d in data[i:i+window]) / window if window > 0 else volume
            
            # Pattern criteria:
            # 1. Foreign buy > threshold
            # 2. Price down > threshold
            # 3. Volume above average
            if (foreign_net > CONFIG['FOREIGN_SIGNIFICANT_THRESHOLD'] and 
                change_pct < CONFIG['PRICE_DROP_THRESHOLD'] and 
                volume > (avg_volume * CONFIG['VOLUME_RATIO_THRESHOLD'])):
                
                # Check follow-through: rebound > threshold in next 3 days
                follow_up_return = 0
                days_checked = 0
                
                for j in range(1, 4):
                    if i + j < len(data):
                        follow_up_return += data[i + j]['change']['percent']
                        days_checked += 1
                
                if days_checked > 0:
                    success = follow_up_return > CONFIG['REBOUND_THRESHOLD']
                    
                    signals.append({
                        'date': day['date'],
                        'success': success,
                        'follow_up_return': follow_up_return,
                        'foreign_net': foreign_net,
                        'change_pct': change_pct,
                        'volume': volume
                    })
        
        if len(signals) < min_signals:
            return {
                'hit_rate': None,
                'total_signals': len(signals),
                'success_count': sum(1 for s in signals if s['success']),
                'note': f'Only {len(signals)} signals (min {min_signals} needed)'
            }
        
        success_count = sum(1 for s in signals if s['success'])
        hit_rate = success_count / len(signals)
        
        return {
            'hit_rate': hit_rate,
            'total_signals': len(signals),
            'success_count': success_count,
            'note': f"{success_count}/{len(signals)} patterns succeeded ({hit_rate*100:.1f}%)"
        }


# =============================================
# CLASS 4: CONFIDENCE RANKING
# =============================================
class ConfidenceRanker:
    """
    Hitung Confidence Score (0-100) berdasarkan multiple factors
    """
    
    @staticmethod
    def calculate(stock_data: Dict, history: List[Dict]) -> Dict:
        """
        Calculate overall confidence score (0-100) for ranking
        
        Args:
            stock_data: Current stock metadata (avg volume, avg foreign, etc.)
            history: Historical data list
        
        Returns:
            Dict with confidence_score, tier, and breakdown
        """
        score = 0
        breakdown = []
        
        # 1. Liquidity Score (0-25)
        avg_volume = stock_data.get('avg_volume', 0)
        if avg_volume > 5_000_000:
            score += CONFIG['WEIGHT_LIQUIDITY']
            breakdown.append("✅ High liquidity (>5M lots)")
        elif avg_volume > 1_000_000:
            score += int(CONFIG['WEIGHT_LIQUIDITY'] * 0.6)
            breakdown.append("🟡 Moderate liquidity (1-5M lots)")
        elif avg_volume > 500_000:
            score += int(CONFIG['WEIGHT_LIQUIDITY'] * 0.3)
            breakdown.append("🟠 Low liquidity (500K-1M lots)")
        else:
            breakdown.append("❌ Very low liquidity (<500K lots)")
        
        # 2. Foreign Participation Score (0-25)
        avg_foreign = stock_data.get('avg_foreign_abs', 0)
        if avg_foreign > 50_000_000_000:  # >Rp50B
            score += CONFIG['WEIGHT_FOREIGN_PARTICIPATION']
            breakdown.append("✅ High foreign participation")
        elif avg_foreign > 10_000_000_000:  # >Rp10B
            score += int(CONFIG['WEIGHT_FOREIGN_PARTICIPATION'] * 0.6)
            breakdown.append("🟡 Moderate foreign participation")
        elif avg_foreign > 5_000_000_000:  # >Rp5B
            score += int(CONFIG['WEIGHT_FOREIGN_PARTICIPATION'] * 0.3)
            breakdown.append("🟠 Low foreign participation")
        else:
            breakdown.append("❌ Minimal foreign participation")
        
        # 3. Empirical Hit Rate Score (0-40)
        hit_rate_data = HitRateCalculator.calculate(history)
        
        if hit_rate_data['hit_rate'] is not None:
            hr_score = int(hit_rate_data['hit_rate'] * CONFIG['WEIGHT_HIT_RATE'])
            score += hr_score
            breakdown.append(f"✅ Hit rate: {hit_rate_data['hit_rate']*100:.0f}% ({hit_rate_data['note']})")
        else:
            # Fallback: consistency score if not enough signals
            consistency = sum(1 for h in history[:12] if h['foreign']['net'] != 0) / min(12, len(history))
            fallback_score = int(consistency * CONFIG['WEIGHT_HIT_RATE'] * 0.5)
            score += fallback_score
            breakdown.append(f"🟡 Limited signal data, using consistency: {consistency*100:.0f}%")
        
        # 4. Recency Bonus (0-10)
        if history and history[0]['foreign']['net'] != 0:
            score += CONFIG['WEIGHT_RECENCY']
            breakdown.append("✅ Recent foreign activity")
        
        # Cap at 100
        score = min(score, 100)
        
        # Determine tier
        if score >= 70:
            tier = "HIGH"
        elif score >= 40:
            tier = "MODERATE"
        else:
            tier = "LOW"
        
        return {
            'confidence_score': score,
            'tier': tier,
            'empirical_hit_rate': hit_rate_data.get('hit_rate'),
            'hit_rate_signals': hit_rate_data.get('total_signals', 0),
            'hit_rate_success': hit_rate_data.get('success_count', 0),
            'breakdown': breakdown
        }


# =============================================
# CLASS 5: FOREIGN FLOW ANALYZER
# =============================================
class ForeignFlowAnalyzer:
    """
    Hitung Net Foreign 1 hari & 1 minggu
    """
    
    @staticmethod
    def calculate(history: List[Dict]) -> Dict:
        """
        Calculate Net Foreign 1 day & 1 week from history list
        
        Args:
            history: List of parsed data dicts (sorted newest first)
        
        Returns:
            Dict with foreign flow metrics
        """
        if not history:
            return {
                'net_foreign_1d': 0,
                'net_foreign_1w': 0,
                'net_foreign_1d_formatted': 'Rp0',
                'net_foreign_1w_formatted': 'Rp0',
                'trend_1w': 'NEUTRAL',
                'direction_1d': 'NEUTRAL',
                'direction_1w': 'NEUTRAL'
            }
        
        # Net Foreign 1 hari (paling recent)
        net_1d = history[0]['foreign']['net']
        
        # Net Foreign 1 minggu (sum 5 hari trading terakhir)
        # Filter hanya daily data untuk akurasi
        daily_items = [h for h in history if h['period'] == 'daily'][:5]
        net_1w = sum(item['foreign']['net'] for item in daily_items)
        
        # Helper: Format Rupiah
        def format_rupiah(value: int) -> str:
            if abs(value) >= 1_000_000_000_000:
                return f"{'+' if value > 0 else ''}Rp{value/1e12:.2f}T"
            elif abs(value) >= 1_000_000_000:
                return f"{'+' if value > 0 else ''}Rp{value/1e9:.2f}B"
            elif abs(value) >= 1_000_000:
                return f"{'+' if value > 0 else ''}Rp{value/1e6:.2f}M"
            return f"{'+' if value > 0 else ''}Rp{value:,}"
        
        # Determine trend
        def get_trend(value: int, threshold: int = 10_000_000_000) -> str:
            if value > threshold:
                return 'ACCUMULATION'
            elif value < -threshold:
                return 'DISTRIBUTION'
            return 'NEUTRAL'
        
        def get_direction(value: int) -> str:
            if value > 0:
                return 'BUY'
            elif value < 0:
                return 'SELL'
            return 'NEUTRAL'
        
        return {
            'net_foreign_1d': net_1d,
            'net_foreign_1w': net_1w,
            'net_foreign_1d_formatted': format_rupiah(net_1d),
            'net_foreign_1w_formatted': format_rupiah(net_1w),
            'trend_1w': get_trend(net_1w),
            'direction_1d': get_direction(net_1d),
            'direction_1w': get_direction(net_1w)
        }


# =============================================
# MAIN RANKER CLASS
# =============================================
class StockRanker:
    """
    Main orchestrator - gabungkan semua logic
    """
    
    def __init__(self):
        self.fetcher = StockbitFetcher()
        self.parser = DataParser()
        self.results = []
        self.summary = {
            'total_screened': 0,
            'total_ranked': 0,
            'high_confidence': 0,
            'moderate_confidence': 0,
            'low_confidence': 0,
            'failed': 0
        }
    
    def load_screener_results(self) -> List[Dict]:
        """Load hasil dari latest_screening.json"""
        input_path = Path(CONFIG['INPUT_FILE'])
        
        if not input_path.exists():
            logger.error(f"❌ Input file not found: {CONFIG['INPUT_FILE']}")
            raise FileNotFoundError(f"Input file not found: {CONFIG['INPUT_FILE']}")
        
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            stocks = data.get('data', [])
            self.summary['total_screened'] = len(stocks)
            
            logger.info(f"✅ Loaded {len(stocks)} stocks from {CONFIG['INPUT_FILE']}")
            return stocks
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {CONFIG['INPUT_FILE']}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Error loading screener results: {str(e)}")
            raise
    
    def process_stock(self, stock: Dict) -> Optional[Dict]:
        """Process single stock: fetch data, calculate ranking, return enriched result"""
        ticker = stock.get('Emiten', '')
        
        if not ticker:
            logger.warning("⚠️ Skip stock without ticker")
            return None
        
        logger.info(f"  📊 Processing {ticker}...")
        
        try:
            # Fetch historical data dari Stockbit API
            daily_response = self.fetcher.fetch(ticker, period='daily', limit=50, days_back=60)
            
            if not daily_response:
                logger.warning(f"    ⚠️ Failed to fetch data for {ticker}")
                self.summary['failed'] += 1
                return None
            
            # Parse data
            history = self.parser.parse(daily_response, period='daily')
            
            if not history:
                logger.warning(f"    ⚠️ No valid data parsed for {ticker}")
                self.summary['failed'] += 1
                return None
            
            # Calculate metrics
            foreign_flow = ForeignFlowAnalyzer.calculate(history)
            
            # Calculate stock stats for ranking
            avg_volume = sum(d['volume'] for d in history[:12]) / min(12, len(history))
            avg_foreign = sum(abs(d['foreign']['net']) for d in history[:12]) / min(12, len(history))
            
            stock_stats = {
                'avg_volume': avg_volume,
                'avg_foreign_abs': avg_foreign
            }
            
            # Calculate confidence ranking (Empirical Hit Rate)
            ranking = ConfidenceRanker.calculate(stock_stats, history)
            
            # Update tier counters
            if ranking['tier'] == 'HIGH':
                self.summary['high_confidence'] += 1
            elif ranking['tier'] == 'MODERATE':
                self.summary['moderate_confidence'] += 1
            else:
                self.summary['low_confidence'] += 1
            
            # Build ranked result - PRESERVE ALL SCREENER DATA
            ranked_stock = {
                # Preserve all original screener fields
                **stock,
                
                # Add ranking section
                'ranking': {
                    'confidence_score': ranking['confidence_score'],
                    'tier': ranking['tier'],
                    'empirical_hit_rate': ranking['empirical_hit_rate'],
                    'hit_rate_signals': ranking['hit_rate_signals'],
                    'hit_rate_success': ranking['hit_rate_success'],
                    'breakdown': ranking['breakdown']
                },
                
                # Add foreign flow section
                'foreign_flow': foreign_flow,
                
                # Add metadata
                'ranked_at': datetime.utcnow().isoformat() + 'Z',
                'data_points': len(history)
            }
            
            self.summary['total_ranked'] += 1
            logger.info(f"    ✅ {ticker}: Score {ranking['confidence_score']}/100 ({ranking['tier']})")
            
            return ranked_stock
            
        except Exception as e:
            logger.error(f"    ❌ Error processing {ticker}: {str(e)}")
            self.summary['failed'] += 1
            return None
    
    def save_results(self, output_path: str = None):
        """Save final results to JSON"""
        output_path = output_path or CONFIG['OUTPUT_FILE']
        
        # Sort by confidence_score descending
        self.results.sort(key=lambda x: x['ranking']['confidence_score'], reverse=True)
        
        # Assign ranking number
        for idx, result in enumerate(self.results, 1):
            result['Rank'] = idx
        
        output = {
            'status': 'success',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'date': datetime.now().strftime('%Y-%m-%d'),
            'time': datetime.now().strftime('%H.%M WIB'),
            'total_saham': len(self.results),
            'config': {
                'strategy': 'LuxAlgo Maximum SMC + Empirical Hit Rate Ranking',
                'min_rr': 3.0,
                'max_output': 7,
                'fundamental_required': False,
                'ranking_enabled': True,
                'hit_rate_window': CONFIG['HIT_RATE_WINDOW'],
                'foreign_threshold': CONFIG['FOREIGN_SIGNIFICANT_THRESHOLD']
            },
            'summary': self.summary,
            'stocks': self.results
        }
        
        # Create output directory if not exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Results saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving results: {str(e)}")
            return False
    
    def save_history_files(self):
        """Save individual history files for each stock (for bot access)"""
        Path(CONFIG['HISTORY_DIR']).mkdir(parents=True, exist_ok=True)
        
        for result in self.results:
            ticker = result.get('Emiten', '')
            if not ticker:
                continue
            
            # Fetch again for history (or cache from processing)
            daily_response = self.fetcher.fetch(ticker, period='daily', limit=50, days_back=60)
            
            if daily_response:
                history = self.parser.parse(daily_response, period='daily')
                
                history_output = {
                    'meta': {
                        'symbol': ticker,
                        'last_updated': datetime.utcnow().isoformat() + 'Z',
                        'total_days': len(history),
                        'currency': 'IDR'
                    },
                    'history': history
                }
                
                history_path = Path(CONFIG['HISTORY_DIR']) / f"{ticker}.json"
                
                try:
                    with open(history_path, 'w', encoding='utf-8') as f:
                        json.dump(history_output, f, indent=2, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save history for {ticker}: {str(e)}")
    
    def run(self):
        """Main execution"""
        start_time = time.time()
        
        logger.info("=" * 80)
        logger.info("🚀 RANKER - EMPIRICAL HIT RATE + FOREIGN FLOW ANALYSIS")
        logger.info("=" * 80)
        logger.info(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Input: {CONFIG['INPUT_FILE']}")
        logger.info(f"📁 Output: {CONFIG['OUTPUT_FILE']}")
        logger.info("=" * 80)
        
        try:
            # Load screener results
            stocks = self.load_screener_results()
            
            if not stocks:
                logger.warning("⚠️ No stocks to process. Exiting.")
                self.save_results()
                return
            
            # Process each stock
            logger.info(f"\n🔍 Processing {len(stocks)} stocks...")
            for stock in stocks:
                result = self.process_stock(stock)
                if result:
                    self.results.append(result)
            
            # Save results
            logger.info(f"\n💾 Saving results...")
            self.save_results()
            
            # Save history files (optional, for bot access)
            logger.info(f"\n📚 Saving history files...")
            self.save_history_files()
            
            # Print summary
            elapsed = (time.time() - start_time) / 60
            
            logger.info("\n" + "=" * 80)
            logger.info("📊 RANKING SUMMARY")
            logger.info("=" * 80)
            logger.info(f"✅ Total Screened: {self.summary['total_screened']}")
            logger.info(f"✅ Total Ranked: {self.summary['total_ranked']}")
            logger.info(f"🟢 High Confidence: {self.summary['high_confidence']}")
            logger.info(f"🟡 Moderate Confidence: {self.summary['moderate_confidence']}")
            logger.info(f"🔴 Low Confidence: {self.summary['low_confidence']}")
            logger.info(f"❌ Failed: {self.summary['failed']}")
            logger.info(f"⏱️  Execution Time: {elapsed:.2f} minutes")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"💥 CRITICAL ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


# =============================================
# MAIN EXECUTION
# =============================================
if __name__ == "__main__":
    try:
        ranker = StockRanker()
        ranker.run()
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Program interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 FATAL ERROR: {str(e)}")
        sys.exit(1)