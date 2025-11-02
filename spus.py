# -*- coding: utf-8 -*-
"""
SPUS Quantitative Analyzer v15 (Research Grade)

- Implements data fallbacks (Alpha Vantage) and validation.
- Fetches a wide range of metrics for 6-factor modeling:
  (Value, Momentum, Quality, Size, Low Volatility, Technical)
- Includes robust data fetching for tickers and fundamentals.
- Modular functions to be called by analysis script.
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

def fetch_spus_tickers():
    """
    Tries to fetch tickers from local CSV first, then falls back to web scraping.
    """
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
        earnings = {
            "earnings": ticker_obj.earnings,
            "quarterly_earnings": ticker_obj.quarterly_earnings,
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
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        logging.warning(f"[{ticker}] Alpha Vantage API key not set. Skipping fallback.")
        return None

    logging.info(f"[{ticker}] Using Alpha Vantage fallback.")
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
            parsed['sector'] = info.get('sector', 'Unknown')
            parsed['enterpriseToEbitda'] = info.get('enterpriseToEbitda')
            parsed['freeCashflow'] = info.get('freeCashflow')
            parsed['trailingEps'] = info.get('trailingEps')
        else: # Alpha Vantage mapping
            parsed['forwardPE'] = float(info.get('ForwardPE', 'nan'))
            parsed['priceToBook'] = float(info.get('PriceToBookRatio', 'nan'))
            parsed['marketCap'] = float(info.get('MarketCapitalization', 'nan'))
            parsed['sector'] = info.get('Sector', 'Unknown')
            parsed['enterpriseToEbitda'] = float(info.get('EVToEBITDA', 'nan'))
            parsed['freeCashflow'] = None # Not in AV Overview
            parsed['trailingEps'] = float(info.get('EPS', 'nan'))
        
        # Derived Value Metrics
        parsed['P/FCF'] = (parsed['marketCap'] / parsed['freeCashflow']) if parsed['marketCap'] and parsed['freeCashflow'] else np.nan
        
        bvps = None
        if parsed['priceToBook'] and last_price:
            bvps = last_price / parsed['priceToBook']
        
        if parsed['trailingEps'] and bvps and parsed['trailingEps'] > 0:
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
        if source == "yfinance" and earnings_data.get("quarterly_earnings") is not None and not earnings_data["quarterly_earnings"].empty:
            q_eps = earnings_data["quarterly_earnings"]['Earnings']
            parsed['earnings_volatile'] = q_eps.std() / q_eps.abs().mean() > 0.5 # Coeff of variation
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
        hist.ta.rsi(length=cfg['RSI_WINDOW'], append=True)
        hist.ta.sma(length=cfg['SHORT_MA_WINDOW'], append=True)
        hist.ta.sma(length=cfg['LONG_MA_WINDOW'], append=True)
        hist.ta.macd(fast=cfg['MACD_SHORT_SPAN'], slow=cfg['MACD_LONG_SPAN'], signal=cfg['MACD_SIGNAL_SPAN'], append=True)
        hist.ta.adx(length=cfg['ADX_WINDOW'], append=True)
        hist.ta.atr(length=cfg['ATR_WINDOW'], append=True)
        
        rsi_col = f'RSI_{cfg["RSI_WINDOW"]}'
        short_ma_col = f'SMA_{cfg["SHORT_MA_WINDOW"]}'
        long_ma_col = f'SMA_{cfg["LONG_MA_WINDOW"]}'
        macd_h_col = f'MACDh_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
        adx_col = f'ADX_{cfg["ADX_WINDOW"]}'
        atr_col = f'ATR_{cfg["ATR_WINDOW"]}'

        parsed['RSI'] = hist[rsi_col].iloc[-1] if rsi_col in hist.columns else np.nan
        parsed['ATR'] = hist[atr_col].iloc[-1] if atr_col in hist.columns else np.nan
        parsed['ADX'] = hist[adx_col].iloc[-1] if adx_col in hist.columns else np.nan

        last_short_ma = hist[short_ma_col].iloc[-1] if short_ma_col in hist.columns else np.nan
        last_long_ma = hist[long_ma_col].iloc[-1] if long_ma_col in hist.columns else np.nan
        
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
        
        # News & Earnings Date
        if source == "yfinance":
            try:
                news = earnings_data.get('news', [])
                if news:
                    parsed['latest_headline'] = news[0].get('title', "N/A")
                    now_ts = datetime.now().timestamp()
                    recent_news_ts = now_ts - (CONFIG.get('NEWS_LOOKBACK_HOURS', 48) * 3600)
                    parsed['recent_news'] = "Yes" if any(item.get('providerPublishTime', 0) > recent_news_ts for item in news) else "No"
                else:
                    parsed['latest_headline'] = "N/A"
                    parsed['recent_news'] = "No"

                calendar = earnings_data.get('calendar', {})
                if calendar and 'Earnings Date' in calendar and calendar['Earnings Date']:
                    date_val = calendar['Earnings Date'][0]
                    parsed['next_earnings_date'] = pd.to_datetime(date_val).strftime('%Y-%m-%d') if pd.notna(date_val) else "N/A"
                else:
                    parsed['next_earnings_date'] = "N/A"
            except Exception as e:
                 logging.warning(f"[{ticker_symbol}] Error parsing news/calendar: {e}")
                 parsed['latest_headline'] = "N/A"
                 parsed['recent_news'] = "N/A"
                 parsed['next_earnings_date'] = "N/A"
        else:
             parsed['latest_headline'] = "N/A (AV)"
             parsed['recent_news'] = "N/A (AV)"
             parsed['next_earnings_date'] = info.get('DividendDate', 'N/A (AV)') # Bad proxy

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
        av_key = CONFIG.get('DATA_PROVIDERS', {}).get('ALPHA_VANTAGE_API_KEY', None)
        av_data = fetch_data_alpha_vantage(ticker, av_key)
        
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
        fib_61_8_level = resistance_val - (high_low_diff * 0.618) if high_low_diff > 0 else None

        return support_val, support_date, resistance_val, resistance_date, fib_61_8_level, fib_161_8_level
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
