# -*- coding: utf-8 -*-
"""
SPUS Quantitative Analyzer v18 (Institutional SMC Upgrade)

- Implements data fallbacks (Alpha Vantage) and validation.
- Fetches a wide range of metrics for 6-factor modeling.
- Includes robust data fetching for tickers and fundamentals.
- Modular functions to be called by analysis script.
- REWORKED: find_order_blocks for SMC (BOS, Mitigation, Validation).
- ADDED: 'entry_signal' filter based on proximity to validated OBs.
- MODIFIED: Risk logic to use dynamic 'Final Stop Loss' comparing
  ATR vs. 'Cut Loss' (last swing low).
- ADDED: Last Dividend and News List fetching.
- ADDED: pct_above_support metric for filtering.
"""

import requests
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import time
import os
import logging
from datetime import datetime
import json
import numpy as np
from bs4 import BeautifulSoup
import random

# --- Define Base Directory ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Load CONFIG at module level ---
def load_config(path='config.json'):
    config_path = os.path.join(BASE_DIR, path)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info(f"Successfully loaded configuration from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"FATAL: Configuration file '{config_path}' not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"FATAL: Could not decode JSON from '{config_path}'. Check for syntax errors.")
        return None

CONFIG = load_config('config.json')
if CONFIG is None:
    logging.critical("Failed to load config.json. Module may not function.")

# --- 1. DATA RELIABILITY & SOURCING ---

def fetch_spus_tickers_from_csv(local_path):
    """Helper to parse the local CSV file."""
    ticker_column_name = 'StockTicker'
    if not os.path.exists(local_path):
        logging.error(f"Local SPUS holdings file not found at: {local_path}")
        return None
    try:
        holdings_df = pd.read_csv(local_path)
    except pd.errors.ParserError:
        logging.warning("Pandas ParserError. Trying again with 'skiprows'...")
        for i in range(1, 10):
            try:
                holdings_df = pd.read_csv(local_path, skiprows=i)
                if ticker_column_name in holdings_df.columns:
                    logging.info(f"Successfully parsed CSV by skipping {i} rows.")
                    break
            except Exception:
                continue
        else:
            logging.error(f"Failed to parse CSV from {local_path} even after skipping 9 rows.")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during local CSV read/parse: {e}")
        return None
    
    if ticker_column_name not in holdings_df.columns:
        logging.error(f"CSV from {local_path} found, but '{ticker_column_name}' column not found.")
        return None
    
    return holdings_df[ticker_column_name].tolist()

def fetch_spus_tickers_from_web(url="https://www.sp-funds.com/spus/"):
    """Fallback to scrape SPUS holdings page if CSV fails."""
    logging.info(f"Attempting to scrape tickers from {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the table - this selector is specific and may break
        holdings_table = soup.find('table', {'id': 'etf-holdings'})
        if not holdings_table:
            # Fallback selector
            holdings_table = soup.find('table', class_='holdings-table')
            
        if not holdings_table:
            logging.error(f"Could not find holdings table on {url}. Web scrape failed.")
            return None
            
        tickers = []
        rows = holdings_table.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) > 1:
                ticker = cells[1].get_text(strip=True)
                if ticker and ticker.isalpha() and ticker.upper() not in ["CASH", "OTHER"]:
                    tickers.append(ticker)
        
        if not tickers:
            logging.warning(f"Found table on {url}, but extracted no tickers.")
            return None
            
        logging.info(f"Successfully scraped {len(tickers)} tickers from {url}.")
        return tickers

    except requests.exceptions.RequestException as e:
        logging.error(f"Error scraping {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error parsing HTML from {url}: {e}")
        return None

# --- (This is the NEW, REWRITTEN function) ---
def find_order_blocks(hist_df_full, ticker="TICKER"):
    """
    Finds the most recent Bullish and Bearish Order Blocks based on
    Smart Money Concepts (SMC):
    1. Find Swing Highs/Lows (Pivots).
    2. Find the most recent Break of Structure (BOS) (close > SH or close < SL).
    3. Identify the opposing candle cluster *before* the BOS as the Order Block.
    4. Check if the OB was mitigated (touched) and then validated (bounced).
    
    Args:
        hist_df_full (pd.DataFrame): The price history.
        ticker (str): The ticker symbol (for logging).

    Returns:
        dict: A dictionary with OB price ranges, validation status, and last swing points.
    """
    
    # --- 1. Initialize & Load Config ---
    smc_config = CONFIG.get('TECHNICALS', {}).get('SMC_ORDER_BLOCKS', {})
    lookback = smc_config.get('LOOKBACK_PERIOD', 252)
    pivots_n = smc_config.get('PIVOT_BARS', 5)
    cluster_size = smc_config.get('CLUSTER_SIZE', 2)
    validation_lookback = smc_config.get('VALIDATION_LOOKBACK', 10)

    # Base return object
    ob_data = {
        'bullish_ob_low': np.nan, 'bullish_ob_high': np.nan, 'bullish_ob_validated': False,
        'bearish_ob_low': np.nan, 'bearish_ob_high': np.nan, 'bearish_ob_validated': False,
        'last_swing_low': np.nan, 'last_swing_high': np.nan
    }

    if len(hist_df_full) < lookback:
        logging.warning(f"[{ticker}] Not enough history ({len(hist_df_full)} days) for SMC analysis (needs {lookback}).")
        return ob_data

    try:
        hist_df = hist_df_full.iloc[-lookback:].copy()
        
        # --- 2. Find Swing Highs/Lows ---
        hist_df['sh'] = hist_df.ta.pivothigh(left=pivots_n, right=pivots_n)
        hist_df['sl'] = hist_df.ta.pivotlow(left=pivots_n, right=pivots_n)
        
        swing_highs = hist_df[hist_df['sh'].notna()]
        swing_lows = hist_df[hist_df['sl'].notna()]

        if swing_highs.empty or swing_lows.empty:
            logging.warning(f"[{ticker}] No swing points found in the last {lookback} days.")
            return ob_data
            
        last_sh = swing_highs.iloc[-1]
        last_sl = swing_lows.iloc[-1]
        ob_data['last_swing_low'] = last_sl.sl
        ob_data['last_swing_high'] = last_sh.sh

        # --- 3. Find Most Recent Bullish OB (BOS Up) ---
        # Look for a close *above* the last swing high
        bos_up_candles = hist_df.loc[last_sh.name:][hist_df['Close'] > last_sh.sh]
        
        if not bos_up_candles.empty:
            first_bos_candle = bos_up_candles.iloc[0]
            
            # Find the opposing (bearish) candle cluster *before* the BOS
            candles_before_bos = hist_df.loc[:first_bos_candle.name].iloc[:-1]
            bearish_candles = candles_before_bos[candles_before_bos['Close'] < candles_before_bos['Open']]
            
            if not bearish_candles.empty:
                last_bearish_cluster = bearish_candles.iloc[-cluster_size:]
                ob_data['bullish_ob_low'] = last_bearish_cluster['Low'].min()
                ob_data['bullish_ob_high'] = last_bearish_cluster['High'].max()
                logging.info(f"[{ticker}] Found Bullish BOS at {first_bos_candle.name}. OB Zone: {ob_data['bullish_ob_low']:.2f}-{ob_data['bullish_ob_high']:.2f}")

                # --- 4. Check Bullish OB Validation ---
                history_after_bos = hist_df.loc[first_bos_candle.name:].iloc[1:]
                if not history_after_bos.empty:
                    # Check for mitigation (price returns to touch the OB)
                    candles_that_touched = history_after_bos[history_after_bos['Low'] <= ob_data['bullish_ob_high']]
                    
                    if not candles_that_touched.empty:
                        # Check for invalidation (price closes *through* the OB)
                        invalidated = (candles_that_touched['Close'] < ob_data['bullish_ob_low']).any()
                        
                        if not invalidated:
                            # Check for reaction/bounce (e.g., makes a new high after touch)
                            first_touch_idx = candles_that_touched.index[0]
                            reaction_candles = hist_df.loc[first_touch_idx:].iloc[1:validation_lookback+1]
                            
                            if not reaction_candles.empty:
                                # A simple validation: price moves up significantly or makes a new high
                                bounced = reaction_candles['High'].max() > first_bos_candle.High
                                if bounced:
                                    ob_data['bullish_ob_validated'] = True
                                    logging.info(f"[{ticker}] Bullish OB was validated (mitigated and bounced).")
                        else:
                             logging.info(f"[{ticker}] Bullish OB was invalidated (price closed below zone).")

        # --- 5. Find Most Recent Bearish OB (BOS Down) ---
        # Look for a close *below* the last swing low
        bos_down_candles = hist_df.loc[last_sl.name:][hist_df['Close'] < last_sl.sl]
        
        if not bos_down_candles.empty:
            first_bos_candle = bos_down_candles.iloc[0]
            
            # Find the opposing (bullish) candle cluster *before* the BOS
            candles_before_bos = hist_df.loc[:first_bos_candle.name].iloc[:-1]
            bullish_candles = candles_before_bos[candles_before_bos['Close'] > candles_before_bos['Open']]
            
            if not bullish_candles.empty:
                last_bullish_cluster = bullish_candles.iloc[-cluster_size:]
                ob_data['bearish_ob_low'] = last_bullish_cluster['Low'].min()
                ob_data['bearish_ob_high'] = last_bullish_cluster['High'].max()
                logging.info(f"[{ticker}] Found Bearish BOS at {first_bos_candle.name}. OB Zone: {ob_data['bearish_ob_low']:.2f}-{ob_data['bearish_ob_high']:.2f}")

                # --- 6. Check Bearish OB Validation ---
                history_after_bos = hist_df.loc[first_bos_candle.name:].iloc[1:]
                if not history_after_bos.empty:
                    # Check for mitigation (price returns to touch the OB)
                    candles_that_touched = history_after_bos[history_after_bos['High'] >= ob_data['bearish_ob_low']]
                    
                    if not candles_that_touched.empty:
                        # Check for invalidation (price closes *through* the OB)
                        invalidated = (candles_that_touched['Close'] > ob_data['bearish_ob_high']).any()
                        
                        if not invalidated:
                            # Check for reaction/bounce (e.g., makes a new low after touch)
                            first_touch_idx = candles_that_touched.index[0]
                            reaction_candles = hist_df.loc[first_touch_idx:].iloc[1:validation_lookback+1]
                            
                            if not reaction_candles.empty:
                                bounced = reaction_candles['Low'].min() < first_bos_candle.Low
                                if bounced:
                                    ob_data['bearish_ob_validated'] = True
                                    logging.info(f"[{ticker}] Bearish OB was validated (mitigated and bounced).")
                        else:
                             logging.info(f"[{ticker}] Bearish OB was invalidated (price closed above zone).")

        return ob_data
                
    except Exception as e:
        logging.warning(f"[{ticker}] Error in find_order_blocks: {e}", exc_info=True)
        # Return default structure on error
        return {
            'bullish_ob_low': np.nan, 'bullish_ob_high': np.nan, 'bullish_ob_validated': False,
            'bearish_ob_low': np.nan, 'bearish_ob_high': np.nan, 'bearish_ob_validated': False,
            'last_swing_low': np.nan, 'last_swing_high': np.nan
        }

def fetch_spus_tickers():
    """
    Tries to fetch tickers from local CSV first, then falls back to web scraping.
    """
    if CONFIG is None:
        logging.error("fetch_spus_tickers: CONFIG is None. Cannot find CSV path.")
        return []
        
    local_path = os.path.join(BASE_DIR, CONFIG['SPUS_HOLDINGS_CSV_PATH'])
    
    # 1. Try local CSV
    tickers = fetch_spus_tickers_from_csv(local_path)
    
    # 2. Try web scrape fallback
    if tickers is None:
        logging.warning("Local CSV failed, trying web scrape fallback...")
        tickers = fetch_spus_tickers_from_web()

    if tickers is None:
        logging.critical("All ticker sources (CSV, Web) failed. Returning empty list.")
        return []

    # Clean list
    tickers = [s for s in tickers if isinstance(s, str) and s and 'CASH' not in s.upper() and 'OTHER' not in s.upper()]
    # Remove potential header artifacts
    tickers = [t for t in tickers if t != 'StockTicker' and len(t) < 6]
    
    logging.info(f"Successfully fetched {len(tickers)} unique ticker symbols for SPUS.")
    return list(set(tickers)) # Return unique list

def fetch_data_yfinance(ticker_obj):
    """Fetches history and info from yfinance."""
    try:
        hist = ticker_obj.history(period=CONFIG.get("HISTORICAL_DATA_PERIOD", "5y"))
        info = ticker_obj.info
        
        # Add earnings data
        # *** CORRECTED FOR DEPRECATION WARNING ***
        earnings = {
            "earnings": ticker_obj.income_stmt,
            "quarterly_earnings": ticker_obj.quarterly_income_stmt,
            "calendar": ticker_obj.calendar,
            "news": ticker_obj.news
        }
        
        if hist.empty or info is None:
            logging.warning(f"[{ticker_obj.ticker}] yfinance returned empty history or info.")
            return None
            
        return {"hist": hist, "info": info, "earnings_data": earnings, "source": "yfinance"}
    except Exception as e:
        logging.error(f"[{ticker_obj.ticker}] yfinance data fetch error: {e}")
        return None

def fetch_data_alpha_vantage(ticker, api_key):
    """Fallback data provider: Alpha Vantage."""
    if not api_key or api_key == "YOUR_API_KEY_1": # Check against a default placeholder
        logging.warning(f"[{ticker}] Alpha Vantage API key ('{api_key}') is not set. Skipping fallback.")
        return None

    logging.info(f"[{ticker}] Using Alpha Vantage fallback with a rotated key.")
    base_url = "https://www.alphavantage.co/query"
    hist_data = None
    info_data = None

    try:
        # 1. Fetch History
        hist_params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": ticker,
            "outputsize": "full",
            "apikey": api_key
        }
        response = requests.get(base_url, params=hist_params, timeout=10)
        response.raise_for_status()
        hist_json = response.json()
        
        if "Time Series (Daily)" in hist_json:
            hist_data = pd.DataFrame.from_dict(hist_json["Time Series (Daily)"], orient='index')
            hist_data.index = pd.to_datetime(hist_data.index)
            hist_data = hist_data.astype(float)
            hist_data.rename(columns={
                '1. open': 'Open',
                '2. high': 'High',
                '3. low': 'Low',
                '4. close': 'Close',
                '5. adjusted close': 'Adj Close',
                '6. volume': 'Volume'
            }, inplace=True)
            hist_data = hist_data.sort_index()
            # Note: AV data needs more mapping for dividends/splits if required
            hist_data['Dividends'] = 0.0
            hist_data['Stock Splits'] = 0.0
        else:
            logging.warning(f"[{ticker}] AV History fetch warning: {hist_json.get('Note') or hist_json.get('Error Message')}")

        # 2. Fetch Info
        info_params = {"function": "OVERVIEW", "symbol": ticker, "apikey": api_key}
        response = requests.get(base_url, params=info_params, timeout=10)
        response.raise_for_status()
        info_data = response.json()
        if not info_data or "Symbol" not in info_data:
             logging.warning(f"[{ticker}] AV Info fetch warning: {info_data.get('Note') or info_data.get('Error Message')}")
             info_data = None

    except requests.exceptions.RequestException as e:
        logging.error(f"[{ticker}] Alpha Vantage request error: {e}")
        return None
    except Exception as e:
        logging.error(f"[{ticker}] Alpha Vantage processing error: {e}")
        return None

    if hist_data is not None and info_data is not None:
        return {"hist": hist_data, "info": info_data, "earnings_data": {}, "source": "alpha_vantage"}
    else:
        return None

def is_data_valid(data, source="yfinance"):
    """
    Validation layer to check for critical missing data.
    """
    if data is None:
        return False
        
    info = data.get("info", {})
    
    if source == "yfinance":
        # Check yfinance info dict
        key_fields = ['sector', 'marketCap', 'trailingEps', 'priceToBook', 'returnOnEquity', 'forwardPE']
    else:
        # Check Alpha Vantage info dict (keys are different)
        key_fields = ['Sector', 'MarketCapitalization', 'EPS', 'BookValue', 'ReturnOnEquityTTM', 'ForwardPE']
    
    missing_fields = [f for f in key_fields if info.get(f) is None or info.get(f) == 0 or info.get(f) == "None"]
    
    if len(missing_fields) > 2: # Allow 2 fields to be missing
        logging.warning(f"[{info.get('symbol', 'TICKER')}] Data from {source} failed validation. Missing: {missing_fields}")
        return False
        
    return True

def parse_ticker_data(data, ticker_symbol):
    """
    Parses data from either yfinance or Alpha Vantage into a common format.
    Also calculates all TA metrics.
    """
    hist = data['hist']
    info = data['info']
    earnings_data = data['earnings_data']
    source = data['source']
    
    parsed = {'ticker': ticker_symbol, 'success': True, 'source': source}
    parsed['data_warning'] = None
    
    try:
        # --- 0. Clean History & Get Last Price ---
        if not hist.index.is_unique:
             hist = hist[~hist.index.duplicated(keep='first')]
        last_price = hist['Close'].iloc[-1]
        parsed['last_price'] = last_price
    
        # --- 1. Value Factors ---
        if source == "yfinance":
            parsed['forwardPE'] = info.get('forwardPE')
            parsed['priceToBook'] = info.get('priceToBook')
            parsed['marketCap'] = info.get('marketCap')
            # *** CORRECTED FOR KEYERROR ***
            parsed['Sector'] = info.get('sector', 'Unknown')
            parsed['enterpriseToEbitda'] = info.get('enterpriseToEbitda')
            parsed['freeCashflow'] = info.get('freeCashflow')
            parsed['trailingEps'] = info.get('trailingEps')
        else: # Alpha Vantage mapping
            parsed['forwardPE'] = float(info.get('ForwardPE', 'nan'))
            parsed['priceToBook'] = float(info.get('PriceToBookRatio', 'nan'))
            parsed['marketCap'] = float(info.get('MarketCapitalization', 'nan'))
            # *** CORRECTED FOR KEYERROR ***
            parsed['Sector'] = info.get('Sector', 'Unknown')
            parsed['enterpriseToEbitda'] = float(info.get('EVToEBITDA', 'nan'))
            parsed['freeCashflow'] = None # Not in AV Overview
            parsed['trailingEps'] = float(info.get('EPS', 'nan'))
        
        # Derived Value Metrics
        parsed['P/FCF'] = (parsed['marketCap'] / parsed['freeCashflow']) if parsed['marketCap'] and parsed['freeCashflow'] else np.nan
        
        bvps = None
        if parsed['priceToBook'] and last_price:
            bvps = last_price / parsed['priceToBook']
        
        # *** CORRECTED FOR RUNTIMEWARNING ***
        if parsed['trailingEps'] and bvps and parsed['trailingEps'] > 0 and bvps > 0:
            parsed['grahamNumber'] = (22.5 * parsed['trailingEps'] * bvps) ** 0.5
            parsed['grahamValuation'] = "Undervalued (Graham)" if last_price < parsed['grahamNumber'] else "Overvalued (Graham)"
        else:
            parsed['grahamNumber'] = np.nan
            parsed['grahamValuation'] = "N/A (Unprofitable)" if parsed['trailingEps'] and parsed['trailingEps'] <= 0 else "N/A (Missing Data)"

        # --- 2. Momentum Factors ---
        monthly_hist = hist['Close'].resample('ME').last()
        if len(monthly_hist) >= 13:
            price_1m_ago = monthly_hist.iloc[-2]
            price_13m_ago = monthly_hist.iloc[-13]
            parsed['momentum_12m'] = ((price_1m_ago - price_13m_ago) / price_13m_ago) * 100 if price_13m_ago else np.nan
        else:
            parsed['momentum_12m'] = np.nan
            
        if len(monthly_hist) >= 4:
            price_1m_ago = monthly_hist.iloc[-2]
            price_4m_ago = monthly_hist.iloc[-4]
            parsed['momentum_3m'] = ((price_1m_ago - price_4m_ago) / price_4m_ago) * 100 if price_4m_ago else np.nan
        else:
            parsed['momentum_3m'] = np.nan

        daily_returns = hist['Close'].pct_change().dropna()
        if len(daily_returns) >= 252:
            returns_1y = daily_returns.iloc[-252:]
            parsed['volatility_1y'] = returns_1y.std() * np.sqrt(252)
        else:
            parsed['volatility_1y'] = np.nan

        parsed['risk_adjusted_momentum'] = (parsed['momentum_12m'] / parsed['volatility_1y']) if parsed['momentum_12m'] and parsed['volatility_1y'] else np.nan

        # --- 3. Quality Factors ---
        if source == "yfinance":
            parsed['returnOnEquity'] = info.get('returnOnEquity')
            parsed['profitMargins'] = info.get('profitMargins')
            parsed['returnOnAssets'] = info.get('returnOnAssets')
            parsed['debtToEquity'] = info.get('debtToEquity')
        else: # Alpha Vantage mapping
            parsed['returnOnEquity'] = float(info.get('ReturnOnEquityTTM', 'nan'))
            parsed['profitMargins'] = float(info.get('ProfitMargin', 'nan'))
            parsed['returnOnAssets'] = float(info.get('ReturnOnAssetsTTM', 'nan'))
            parsed['debtToEquity'] = float(info.get('DebtToEquityRatio', 'nan'))

        # Earnings volatility (simplified)
        # *** CORRECTED FOR DEPRECATION WARNING ***
        if source == "yfinance" and earnings_data.get("quarterly_earnings") is not None and not earnings_data["quarterly_earnings"].empty:
            
            quarterly_data = earnings_data["quarterly_earnings"]
            
            if 'Net Income' in quarterly_data.index:
                q_eps = quarterly_data.loc['Net Income']
            elif 'Earnings' in quarterly_data.columns:
                 # Fallback for old structure, just in case
                 q_eps = quarterly_data['Earnings']
            else:
                 logging.warning(f"[{ticker_symbol}] Could not find 'Net Income' (index) or 'Earnings' (column) in quarterly data.")
                 q_eps = pd.Series([np.nan]) # Create a series to avoid errors
                 
            q_eps = pd.to_numeric(q_eps, errors='coerce').dropna()

            if not q_eps.empty and q_eps.abs().mean() != 0:
                parsed['earnings_volatile'] = (q_eps.std() / q_eps.abs().mean()) > 0.5 # Coeff of variation
            else:
                parsed['earnings_volatile'] = np.nan
                
            parsed['earnings_negative'] = parsed.get('trailingEps', 0) < 0
        else:
            parsed['earnings_volatile'] = np.nan
            parsed['earnings_negative'] = np.nan if pd.isna(parsed.get('trailingEps')) else parsed.get('trailingEps', 0) < 0
            
        # --- 4. Size Factors ---
        if source == "yfinance":
            parsed['floatShares'] = info.get('floatShares')
            parsed['averageVolume'] = info.get('averageVolume')
        else: # Alpha Vantage mapping
            parsed['floatShares'] = float(info.get('SharesOutstanding', 'nan')) # Proxy, AV uses SharesOutstanding
            parsed['averageVolume'] = float(info.get('50DayMovingAverage', 'nan')) # Bad proxy, but it's something
        
        parsed['floatAdjustedMarketCap'] = (parsed['floatShares'] * last_price) if parsed['floatShares'] and last_price else parsed['marketCap']

        # --- 5. Low Volatility Factors ---
        if source == "yfinance":
            parsed['beta'] = info.get('beta')
        else: # Alpha Vantage mapping
            parsed['beta'] = float(info.get('Beta', 'nan'))
        # volatility_1y already calculated in Momentum

        # --- 6. Technical Factors ---
        cfg = CONFIG['TECHNICALS']
        
        # *** ADD THIS WARNING CHECK ***
        min_hist_len = max(cfg.get('LONG_MA_WINDOW', 200), 252) # Use longest MA or 1 year
        if len(hist) < min_hist_len:
            warning_msg = f"Short history ({len(hist)} days). TA/Risk metrics may be N/A or unreliable."
            parsed['data_warning'] = warning_msg
            logging.warning(f"[{ticker_symbol}] {warning_msg}")
        # *** END OF NEW CHECK ***
        
        # Calculate other indicators using append
        hist.ta.rsi(length=cfg['RSI_WINDOW'], append=True)
        hist.ta.sma(length=cfg['SHORT_MA_WINDOW'], append=True)
        hist.ta.sma(length=cfg['LONG_MA_WINDOW'], append=True)
        hist.ta.macd(fast=cfg['MACD_SHORT_SPAN'], slow=cfg['MACD_LONG_SPAN'], signal=cfg['MACD_SIGNAL_SPAN'], append=True)
        hist.ta.adx(length=cfg['ADX_WINDOW'], append=True)
        
        # *** FIX: Calculate ATR separately and assign it directly ***
        atr_col = f'ATR_{cfg["ATR_WINDOW"]}'
        atr_series = hist.ta.atr(length=cfg['ATR_WINDOW'])
        if atr_series is not None:
            hist[atr_col] = atr_series
        # *** END OF FIX ***

        # Define column names
        rsi_col = f'RSI_{cfg["RSI_WINDOW"]}'
        short_ma_col = f'SMA_{cfg["SHORT_MA_WINDOW"]}'
        long_ma_col = f'SMA_{cfg["LONG_MA_WINDOW"]}'
        macd_h_col = f'MACDh_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
        adx_col = f'ADX_{cfg["ADX_WINDOW"]}'
        # atr_col is already defined above

        # Parse the last value from each column
        parsed['RSI'] = hist[rsi_col].iloc[-1] if rsi_col in hist.columns and not hist[rsi_col].isnull().all() else np.nan
        parsed['ATR'] = hist[atr_col].iloc[-1] if atr_col in hist.columns and not hist[atr_col].isnull().all() else np.nan
        parsed['ADX'] = hist[adx_col].iloc[-1] if adx_col in hist.columns and not hist[adx_col].isnull().all() else np.nan

        last_short_ma = hist[short_ma_col].iloc[-1] if short_ma_col in hist.columns and not hist[short_ma_col].isnull().all() else np.nan
        last_long_ma = hist[long_ma_col].iloc[-1] if long_ma_col in hist.columns and not hist[long_ma_col].isnull().all() else np.nan
        
        parsed['Price_vs_SMA50'] = (last_price / last_short_ma) if last_short_ma else np.nan
        parsed['Price_vs_SMA200'] = (last_price / last_long_ma) if last_long_ma else np.nan
        
        # Old Trend/MACD signals (for compatibility)
        hist_val = hist[macd_h_col].iloc[-1] if macd_h_col in hist.columns else np.nan
        if not pd.isna(last_short_ma) and not pd.isna(last_long_ma):
            if last_short_ma > last_long_ma:
                parsed['Trend (50/200 Day MA)'] = 'Confirmed Uptrend' if last_price > last_short_ma else 'Uptrend (Correction)'
            else:
                parsed['Trend (50/200 Day MA)'] = 'Confirmed Downtrend' if last_price < last_short_ma else 'Downtrend (Rebound)'
        else:
            parsed['Trend (50/200 Day MA)'] = 'N/A'
            
        if macd_h_col in hist.columns and len(hist) >= 2 and not pd.isna(hist_val):
            prev_hist = hist[macd_h_col].iloc[-2]
            if not pd.isna(prev_hist):
                if hist_val > 0 and prev_hist <= 0: parsed['MACD_Signal'] = "Bullish Crossover"
                elif hist_val < 0 and prev_hist >= 0: parsed['MACD_Signal'] = "Bearish Crossover"
                elif hist_val > 0: parsed['MACD_Signal'] = "Bullish"
                else: parsed['MACD_Signal'] = "Bearish"
            else: parsed['MACD_Signal'] = "N/A"
        else: parsed['MACD_Signal'] = "N/A"

        # --- 7. Other Info ---
        parsed['hist_df'] = hist # For plotly charts
        
        # Support/Resistance
        lookback = CONFIG.get('SR_LOOKBACK_PERIOD', 90)
        recent_hist = hist.iloc[-lookback:] if len(hist) >= lookback else hist
        parsed['Support_90d'] = recent_hist['Low'].min()
        parsed['Resistance_90d'] = recent_hist['High'].max()
        
        # --- NEW: Calculate % Above Support ---
        support_90d = parsed.get('Support_90d')
        if pd.notna(support_90d) and pd.notna(last_price) and last_price > 0:
            parsed['pct_above_support'] = ((last_price - support_90d) / last_price) * 100
        else:
            parsed['pct_above_support'] = np.nan
        # --- END OF NEW ---
        
        # --- MODIFIED: Calculate SMC Order Blocks ---
        # The 'lookback' param is now handled inside the function via CONFIG
        ob_data = find_order_blocks(hist, ticker=ticker_symbol)
        parsed.update(ob_data) # This adds all keys (bullish_ob_low, etc.)
        # --- END OF MODIFICATION ---
        
        # --- NEW: Entry Signal Logic ---
        smc_config = CONFIG.get('TECHNICALS', {}).get('SMC_ORDER_BLOCKS', {})
        proximity_pct = smc_config.get('ENTRY_PROXIMITY_PERCENT', 2.0) / 100.0
        entry_signal = "No Trade"
        
        bullish_ob_low = parsed.get('bullish_ob_low', np.nan)
        bullish_ob_high = parsed.get('bullish_ob_high', np.nan)
        bullish_ob_validated = parsed.get('bullish_ob_validated', False)
        
        bearish_ob_low = parsed.get('bearish_ob_low', np.nan)
        bearish_ob_high = parsed.get('bearish_ob_high', np.nan)
        bearish_ob_validated = parsed.get('bearish_ob_validated', False)

        # Long Entry Logic
        if bullish_ob_validated and pd.notna(bullish_ob_high) and pd.notna(bullish_ob_low):
            # Price must be within the zone or X% above it
            entry_zone_top = bullish_ob_high * (1 + proximity_pct)
            if last_price >= bullish_ob_low and last_price <= entry_zone_top:
                entry_signal = "Buy near Bullish OB"
                logging.info(f"[{ticker_symbol}] Entry Signal: Price {last_price} is near validated Bullish OB ({bullish_ob_low:.2f}-{bullish_ob_high:.2f})")

        # Short Entry Logic
        elif bearish_ob_validated and pd.notna(bearish_ob_low) and pd.notna(bearish_ob_high):
             # Price must be within the zone or X% below it
            entry_zone_bottom = bearish_ob_low * (1 - proximity_pct)
            if last_price <= bearish_ob_high and last_price >= entry_zone_bottom:
                entry_signal = "Sell near Bearish OB"
                logging.info(f"[{ticker_symbol}] Entry Signal: Price {last_price} is near validated Bearish OB ({bearish_ob_low:.2f}-{bearish_ob_high:.2f})")
        
        parsed['entry_signal'] = entry_signal
        # --- END OF NEW ENTRY LOGIC ---

        # News & Earnings Date
        if source == "yfinance":
            try:
                news = earnings_data.get('news', [])
                if news:
                    # --- MODIFIED: Pass top 5 news items ---
                    parsed['news_list'] = [item.get('title', 'N/A') for item in news[:5]] # Get top 5 headlines
                    # --- END OF MODIFICATION ---
                    
                    now_ts = datetime.now().timestamp()
                    recent_news_ts = now_ts - (CONFIG.get('NEWS_LOOKBACK_HOURS', 48) * 3600)
                    parsed['recent_news'] = "Yes" if any(item.get('providerPublishTime', 0) > recent_news_ts for item in news) else "No"
                else:
                    parsed['news_list'] = [] # --- NEW ---
                    parsed['recent_news'] = "No"

                # --- NEW: Last Dividend Info ---
                last_div_date_ts = info.get('lastDividendDate') # This is usually a timestamp
                last_div_value = info.get('lastDividendValue')

                if last_div_date_ts and pd.notna(last_div_date_ts) and last_div_value:
                    parsed['last_dividend_date'] = pd.to_datetime(last_div_date_ts, unit='s').strftime('%Y-%m-%d')
                    parsed['last_dividend_value'] = last_div_value
                else:
                    # Fallback: check the history dataframe
                    divs = hist[hist['Dividends'] > 0]
                    if not divs.empty:
                        parsed['last_dividend_date'] = divs.index[-1].strftime('%Y-%m-%d')
                        parsed['last_dividend_value'] = divs['Dividends'].iloc[-1]
                    else:
                        parsed['last_dividend_date'] = "N/A"
                        parsed['last_dividend_value'] = np.nan
                # --- END OF NEW DIVIDEND LOGIC ---

                calendar = earnings_data.get('calendar', {})
                if calendar and 'Earnings Date' in calendar and calendar['Earnings Date']:
                    date_val = calendar['Earnings Date'][0]
                    parsed['next_earnings_date'] = pd.to_datetime(date_val).strftime('%Y-%m-%d') if pd.notna(date_val) else "N/A"
                else:
                    parsed['next_earnings_date'] = "N/A"
            except Exception as e:
                 logging.warning(f"[{ticker_symbol}] Error parsing news/calendar: {e}")
                 parsed['news_list'] = []
                 parsed['recent_news'] = "N/A"
                 parsed['next_earnings_date'] = "N/A"
                 parsed['last_dividend_date'] = "N/A" # Add to error handler
                 parsed['last_dividend_value'] = np.nan # Add to error handler
        else:
             parsed['news_list'] = [] # --- NEW ---
             parsed['recent_news'] = "N/A (AV)"
             parsed['next_earnings_date'] = info.get('DividendDate', 'N/A (AV)') # Bad proxy
             # --- NEW: Add dividend for AV ---
             parsed['last_dividend_date'] = info.get('DividendDate', 'N/A (AV)') # AV's 'DividendDate' is often *last*
             parsed['last_dividend_value'] = float(info.get('DividendPerShare', 'nan'))
             # --- END OF NEW ---


        # --- 8. Risk Management (MODIFIED with Cut-Loss Filter) ---
        rm_config = CONFIG.get('RISK_MANAGEMENT', {})
        atr_sl_mult = rm_config.get('ATR_STOP_LOSS_MULTIPLIER', 1.5)
        fib_target_mult = 1.618 # Fibonacci 1.618 Extension Target
        risk_per_trade_usd = rm_config.get('RISK_PER_TRADE_AMOUNT', 500)
        use_cut_loss_filter = rm_config.get('USE_CUT_LOSS_FILTER', True)
        
        atr = parsed.get('ATR')
        last_price = parsed.get('last_price')
        support_90d = parsed.get('Support_90d')
        last_swing_low = parsed.get('last_swing_low', np.nan)
        
        risk_per_share = np.nan
        final_stop_loss_price = np.nan
        stop_loss_price_atr = np.nan
        stop_loss_price_cutloss = np.nan
        
        # Method 1: Calculate ATR Stop
        if pd.notna(atr) and atr > 0:
            stop_loss_price_atr = last_price - (atr * atr_sl_mult)
            parsed['Stop Loss (ATR)'] = stop_loss_price_atr
        
        # Method 2: Calculate Cut-Loss Stop (last swing low)
        if pd.notna(last_swing_low) and last_swing_low < last_price:
            stop_loss_price_cutloss = last_swing_low
            parsed['Stop Loss (Cut Loss)'] = stop_loss_price_cutloss
            
        # Determine Final Stop Loss
        # Prefer the *tighter* (higher) stop between ATR and Cut-Loss
        if use_cut_loss_filter and pd.notna(stop_loss_price_atr) and pd.notna(stop_loss_price_cutloss):
            final_stop_loss_price = max(stop_loss_price_atr, stop_loss_price_cutloss)
            if final_stop_loss_price == stop_loss_price_cutloss:
                parsed['SL_Method'] = "Cut-Loss"
            else:
                parsed['SL_Method'] = "ATR"
            logging.info(f"[{ticker_symbol}] SL Filter: ATR ({stop_loss_price_atr:.2f}) vs CutLoss ({stop_loss_price_cutloss:.2f}). Chose: {parsed['SL_Method']}")
        
        # Fallbacks if one is missing
        elif pd.notna(stop_loss_price_atr):
            final_stop_loss_price = stop_loss_price_atr
            parsed['SL_Method'] = "ATR"
        elif pd.notna(stop_loss_price_cutloss):
            final_stop_loss_price = stop_loss_price_cutloss
            parsed['SL_Method'] = "Cut-Loss"
            
        # Final Fallbacks (90d Low or 10%)
        elif pd.notna(support_90d) and support_90d < last_price:
            final_stop_loss_price = support_90d
            parsed['SL_Method'] = "90-Day Low"
        else:
            if pd.notna(last_price):
                final_stop_loss_price = last_price * 0.90 # 10% hard stop
                parsed['SL_Method'] = "10% Fallback"

        # --- Calculate Take Profit, R/R, and Position Size using FINAL Stop ---
        
        # First, define the risk_per_share based on the final_stop_loss_price
        if pd.notna(final_stop_loss_price) and final_stop_loss_price < last_price:
            risk_per_share = last_price - final_stop_loss_price
        else:
            risk_per_share = np.nan # No valid stop found

        if pd.notna(risk_per_share) and risk_per_share > 0:
            # Calculate Fibonacci Take Profit (1.618 extension)
            take_profit_price = last_price + (risk_per_share * fib_target_mult)
            reward_per_share = take_profit_price - last_price
            
            # Position Sizing
            position_size_shares = risk_per_trade_usd / risk_per_share
            position_size_usd = position_size_shares * last_price
            
            # Final Metrics
            parsed['Stop Loss Price'] = final_stop_loss_price # For compatibility
            parsed['Final Stop Loss'] = final_stop_loss_price
            parsed['Take Profit Price'] = take_profit_price
            parsed['Risk/Reward Ratio'] = reward_per_share / risk_per_share
            parsed['Risk % (to Stop)'] = (risk_per_share / last_price) * 100
            parsed['Position Size (Shares)'] = position_size_shares
            parsed['Position Size (USD)'] = position_size_usd
            parsed['Risk Per Trade (USD)'] = risk_per_trade_usd
        else:
            # Set all to nan if no valid stop loss was found
            parsed['Stop Loss Price'] = np.nan
            parsed['Final Stop Loss'] = np.nan
            parsed['Take Profit Price'] = np.nan
            parsed['Risk/Reward Ratio'] = np.nan
            parsed['Risk % (to Stop)'] = np.nan
            parsed['Position Size (Shares)'] = np.nan
            parsed['Position Size (USD)'] = np.nan
            parsed['Risk Per Trade (USD)'] = risk_per_trade_usd
            if 'SL_Method' not in parsed:
                parsed['SL_Method'] = "N/A"
        
        # Fill missing ATR/CutLoss keys if they weren't calculated
        if 'Stop Loss (ATR)' not in parsed:
            parsed['Stop Loss (ATR)'] = np.nan
        if 'Stop Loss (Cut Loss)' not in parsed:
            parsed['Stop Loss (Cut Loss)'] = np.nan

        return parsed
        
    except Exception as e:
        logging.error(f"[{ticker_symbol}] Fatal error in parse_ticker_data: {e}", exc_info=True)
        parsed['success'] = False
        return parsed


# --- MAIN PROCESS FUNCTION ---

def process_ticker(ticker):
    """
    Main ticker processing function.
    Attempts yfinance, validates, falls back to Alpha Vantage, validates,
    then parses all data and calculates metrics.
    """
    if CONFIG is None:
        logging.error(f"process_ticker ({ticker}): CONFIG is None.")
        return {'ticker': ticker, 'success': False, 'error': 'Config not loaded'}
        
    # 1. Attempt yfinance
    ticker_obj = yf.Ticker(ticker)
    yf_data = fetch_data_yfinance(ticker_obj)
    
    data_to_parse = None
    
    if is_data_valid(yf_data, source="yfinance"):
        data_to_parse = yf_data
    else:
        # 2. Attempt Alpha Vantage Fallback
        logging.warning(f"[{ticker}] yfinance data invalid. Trying Alpha Vantage fallback.")
        
        # *** CORRECTED FOR API KEY ROTATION ***
        av_keys_list = CONFIG.get('DATA_PROVIDERS', {}).get('ALPHA_VANTAGE_API_KEYS', [])
        
        if not av_keys_list:
            logging.error(f"[{ticker}] Alpha Vantage fallback failed: No API keys found in config.json under ALPHA_VANTAGE_API_KEYS.")
            av_data = None
        else:
            # Select a random key from the list
            selected_av_key = random.choice(av_keys_list)
            av_data = fetch_data_alpha_vantage(ticker, selected_av_key)
        
        if is_data_valid(av_data, source="alpha_vantage"):
            data_to_parse = av_data
        else:
            logging.error(f"[{ticker}] All data providers failed or returned invalid data.")
            return {'ticker': ticker, 'success': False, 'error': 'All data providers failed'}
            
    # 3. Parse and Calculate
    try:
        parsed_data = parse_ticker_data(data_to_parse, ticker)
        return parsed_data
    except Exception as e:
        logging.critical(f"[{ticker}] Unhandled exception in parse_ticker_data: {e}", exc_info=True)
        return {'ticker': ticker, 'success': False, 'error': f'Parsing error: {e}'}


# --- DEPRECATED FUNCTIONS (Kept for compatibility if old app calls them) ---

def calculate_support_resistance(hist_df):
    """DEPRECATED: Logic is now inside parse_ticker_data"""
    logging.warning("Called deprecated function: calculate_support_resistance")
    if hist_df is None or hist_df.empty:
        return None, None, None, None, None, None
    try:
        lookback_period = CONFIG.get('SR_LOOKBACK_PERIOD', 90)
        recent_hist = hist_df.iloc[-lookback_period:] if len(hist_df) >= lookback_period else hist_df
        
        support_val = recent_hist['Low'].min()
        support_date = recent_hist['Low'].idxmin()
        resistance_val = recent_hist['High'].max()
        resistance_date = recent_hist['High'].idxmax()
        
        high_low_diff = resistance_val - support_val
        fib_161_8_level = resistance_val + (high_low_diff * 0.618) if high_low_diff > 0 else None
        # *** SYNTAX ERROR FIX: Added 'else None' ***
        fib_61_8_level = resistance_val - (high_low_diff * 0.618) if high_low_diff > 0 else None
        return support_val, support_date, resistance_val, resistance_date, fib_161_8_level, fib_161_8_level
    except Exception as e:
        logging.error(f"Error in deprecated calculate_support_resistance: {e}")
        return None, None, None, None, None, None

def calculate_financials_and_fair_price(ticker_obj, last_price, ticker):
    """DEPRECATED: Logic is now inside parse_ticker_data/fetch_data_yfinance"""
    logging.warning("Called deprecated function: calculate_financials_and_fair_price")
    try:
        info = ticker_obj.info
        pe_ratio = info.get('forwardPE')
        pb_ratio = info.get('priceToBook')
        market_cap = info.get('marketCap')
        sector = info.get('sector', 'Unknown')
        eps = info.get('trailingEps')
        
        graham_number = None
        valuation_signal = "N/A"

        if eps and pb_ratio and eps > 0 and pb_ratio > 0 and last_price:
            bvps = last_price / pb_ratio
            graham_number = (22.5 * eps * bvps) ** 0.5
            valuation_signal = "Undervalued (Graham)" if last_price < graham_number else "Overvalued (Graham)"
        elif eps and eps <= 0:
            valuation_signal = "Unprofitable (EPS < 0)"

        return {
            'Forward P/E': pe_ratio,
            'P/B Ratio': pb_ratio,
            'Market Cap': market_cap,
            'Sector': sector,
            'Graham Number': graham_number,
            'Valuation (Graham)': valuation_signal,
            # Other fields are missing as this is deprecated
        }
    except Exception as e:
        logging.error(f"Error in deprecated calculate_financials_and_fair_price for {ticker}: {e}")
        return {'Sector': 'Error', 'Valuation (Graham)': 'Error'}
