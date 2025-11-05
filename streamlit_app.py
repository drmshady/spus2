import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime
import sys
import glob
import numpy as np
import streamlit.components.v1 as components
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import openpyxl
from openpyxl.styles import Font
import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.stats.mstats import winsorize
import pytz 
import pickle 

# --- ⭐️ 1. Set Page Configuration FIRST ⭐️ ---
st.set_page_config(
    page_title="SPUS Quant Analyzer",
    page_icon="https.www.sp-funds.com/wp-content/uploads/2019/07/favicon-32x32.png", 
    layout="wide"
)

# --- DEFINE TIMEZONE ---
SAUDI_TZ = pytz.timezone('Asia/Riyadh')

# --- Path Fix & Import ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from spus import (
        load_config,
        fetch_spus_tickers,
        process_ticker
    )
except ImportError as e:
    st.error(f"Error: Failed to import 'spus.py'. Details: {e}")
    st.stop()

# --- ReportLab Import (Optional) ---
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.units import inch
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    logging.warning("Module 'reportlab' not found. PDF report generation will be disabled.")

# --- ⭐️ 2. Custom CSS (Corrected v2) ⭐️ ---
def load_css():
    """Injects custom CSS for a modern, minimal, card-based theme."""
    st.markdown(f"""
    <style>
        @import url('https.fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        html, body, [data-testid="stAppViewContainer"], [data-testid="stSidebar"] {{
            font-family: 'Inter', sans-serif;
        }}
        h1 {{ font-weight: 700; }}
        h2 {{ font-weight: 600; }}
        h3 {{ font-weight: 600; margin-top: 20px; margin-bottom: 0px; }}
        .main .block-container {{
            padding-top: 2rem; padding-bottom: 2rem;
            padding-left: 2.5rem; padding-right: 2.5rem;
        }}
        [data-testid="stSidebar"] {{
            border-right: 1px solid var(--gray-800); padding: 1.5rem;
        }}
        [data-testid="stSidebar"] h2 {{ font-size: 1.5rem; font-weight: 700; }}
        [data-testid="stSidebar"] .stButton > button, [data-testid="stSidebar"] .stDownloadButton > button {{
            width: 100%; border-radius: 8px; font-weight: 600;
        }}
        
        /* --- ⭐️ CORRECTED: Radio-to-Tabs styling v2 ⭐️ --- */
        [data-testid="stRadio"] > label[data-baseweb="radio"] {{
            display: none; /* Hides the "Navigation:" label */
        }}
        [data-testid="stRadio"] > div[role="radiogroup"] {{
            display: flex;
            flex-direction: row;
            justify-content: stretch; 
            border-bottom: 2px solid var(--gray-800);
            margin-bottom: 1.5rem;
            width: 100%;
        }}
        [data-testid="stRadio"] input[type="radio"] {{
            display: none; /* Hide the actual <input> element */
        }}
        
        /* --- THIS IS THE FIX --- */
        /* 1. Hide the visual radio button circle */
        [data-testid="stRadio"] label[data-baseweb="radio"] > div:first-child {{
            display: none;
        }}
        
        /* 2. Style the text container (which is now the last-child) */
        [data-testid="stRadio"] label[data-baseweb="radio"] > div:last-child {{
            padding: 10px 15px;
            font-weight: 500;
            cursor: pointer;
            border: 2px solid transparent;
            border-bottom: none;
            margin-bottom: -2px; 
            transition: all 0.2s ease;
            width: auto;      
            flex-grow: 1;     
            text-align: center;
        }}
        /* --- END OF FIX --- */
        
        /* The selected "tab" */
        [data-testid="stRadio"] input[type="radio"]:checked + div:last-child {{
            border-color: var(--gray-800);
            border-bottom-color: var(--secondary-background-color); 
            border-radius: 8px 8px 0 0;
            background-color: var(--secondary-background-color);
            color: var(--primary);
            font-weight: 600;
        }}
        /* Hover effect */
        [data-testid="stRadio"] input[type="radio"]:not(:checked) + div:last-child:hover {{
            background-color: var(--gray-900);
            border-radius: 8px 8px 0 0;
        }}
        
        [data-testid="stMetric"] {{
            background-color: var(--background-color);
            border: 1px solid var(--gray-800); border-radius: 8px;
            padding: 1rem 1.25rem;
        }}
        
        /* --- NEW: Style for Entry Signal Delta --- */
        [data-testid="stMetricDelta"] > div:first-child {{
            font-weight: 600;
            font-size: 0.8rem;
            letter-spacing: 0.5px;
        }}

    </style>
    """, unsafe_allow_html=True)

# --- ⭐️ 3. Core Analysis Logic (MODIFIED - Moved to Global Scope) ⭐️ ---

def calculate_robust_zscore_grouped(group_series):
    """Applies robust Z-score (MAD) to a pandas group."""
    series = pd.to_numeric(group_series, errors='coerce')
    median = series.median()
    mad = (series - median).abs().median()
    if mad == 0:
        std = series.std()
        if std == 0 or pd.isna(std):
            return pd.Series(0.0, index=group_series.index)
        mean = series.mean()
        return (series - mean) / std
    z_score = (series - median) / (1.4826 * mad)
    return z_score

def calculate_all_z_scores(df, config):
    """
    Calculates sector-relative Z-scores for all factor components.
    Implements statistical robustness checks.
    """
    logging.info("Calculating Z-Scores...")
    df_analysis = df.copy()
    
    factor_defs = config.get('FACTOR_DEFINITIONS', {})
    stat_config = config.get('STATISTICAL', {})
    win_limit = stat_config.get('WINSORIZE_LIMIT', 0.05)
    min_sector_size = stat_config.get('MIN_SECTOR_SIZE_FOR_MEDIAN', 5)

    sector_counts = df_analysis['Sector'].value_counts()
    small_sectors = sector_counts[sector_counts < min_sector_size].index
    logging.info(f"Small sectors (<{min_sector_size} stocks) found: {list(small_sectors)}. Global medians will be used.")

    all_components = []
    for factor in factor_defs.keys():
        all_components.extend(factor_defs[factor]['components'])
    
    for comp in all_components:
        col = comp['name']
        if col not in df_analysis.columns:
            logging.warning(f"Factor component '{col}' not found in data. Skipping.")
            continue
            
        df_analysis[col] = pd.to_numeric(df_analysis[col], errors='coerce')
        lower = df_analysis[col].quantile(win_limit)
        upper = df_analysis[col].quantile(1 - win_limit)
        if pd.notna(lower) and pd.notna(upper) and lower < upper:
            df_analysis[col] = df_analysis[col].clip(lower, upper)
        
        global_median = df_analysis[col].median()
        if global_median == 0: global_median = 1e-6 
        
        sector_medians = df_analysis.groupby('Sector')[col].median()
        sector_medians.loc[small_sectors] = global_median
        sector_medians = sector_medians.fillna(global_median)
        sector_medians[sector_medians == 0] = global_median
        
        df_analysis[f"{col}_Sector_Median"] = df_analysis['Sector'].map(sector_medians)
        df_analysis[f"{col}_Rel_Ratio"] = df_analysis[col] / df_analysis[f"{col}_Sector_Median"]
        
        z_col_name = f"Z_{col}"
        df_analysis[z_col_name] = df_analysis.groupby('Sector')[f"{col}_Rel_Ratio"].transform(calculate_robust_zscore_grouped)
        
        if not comp['high_is_good']:
            df_analysis[z_col_name] = df_analysis[z_col_name] * -1.0
            
        df_analysis[z_col_name] = df_analysis[z_col_name].fillna(0)

    logging.info("Combining components into final Z-Scores...")
    for factor, details in factor_defs.items():
        z_cols_to_average = [f"Z_{c['name']}" for c in details['components'] if f"Z_{c['name']}" in df_analysis.columns]
        if z_cols_to_average:
            df_analysis[f"Z_{factor}"] = df_analysis[z_cols_to_average].mean(axis=1)
        else:
            df_analysis[f"Z_{factor}"] = 0.0
            
    return df_analysis


def generate_quant_report(CONFIG, progress_callback=None):
    """
    Core logic, decoupled from Streamlit.
    Fetches data, runs analysis, calculates Z-scores, and generates reports.
    *** Includes persistent local caching via pickle. ***
    """
    
    def report_progress(percent, text):
        if progress_callback:
            progress_callback(percent, text)
        logging.info(f"Progress: {percent*100:.0f}% - {text}")

    report_progress(0.01, "Starting analysis...")
    
    # --- 1. Fetch Tickers ---
    report_progress(0.05, "(1/7) Fetching SPUS ticker list...")
    ticker_symbols = fetch_spus_tickers()
    if not ticker_symbols:
        report_progress(1.0, "Error: No ticker symbols found. Analysis cancelled.")
        return None, None, None
        
    exclude_tickers = CONFIG.get('EXCLUDE_TICKERS', [])
    ticker_symbols = [t for t in ticker_symbols if t not in exclude_tickers]
    
    limit = CONFIG.get('TICKER_LIMIT', 0)
    if limit > 0:
        ticker_symbols = ticker_symbols[:limit]
        report_progress(0.07, f"(1/7) Analysis limited to {limit} tickers.")
    
    # --- 2. Process Tickers Concurrently (with Caching) ---
    MAX_WORKERS = CONFIG.get('MAX_CONCURRENT_WORKERS', 10)
    report_progress(0.1, f"(2/7) Checking cache for {len(ticker_symbols)} tickers...")

    CACHE_DIR = os.path.join(BASE_DIR, "cache")
    os.makedirs(CACHE_DIR, exist_ok=True)
    CACHE_TTL_SECONDS = 6 * 3600 # 6 hours
    
    results_list = []
    all_histories = {}
    tickers_to_fetch = []
    current_time = time.time()
    
    for ticker in ticker_symbols:
        cache_path = os.path.join(CACHE_DIR, f"{ticker}.pkl")
        
        if os.path.exists(cache_path):
            try:
                cache_mod_time = os.path.getmtime(cache_path)
                if (current_time - cache_mod_time) < CACHE_TTL_SECONDS:
                    with open(cache_path, 'rb') as f:
                        result = pickle.load(f)
                    
                    if result.get('success', False):
                        if 'hist_df' in result:
                            all_histories[ticker] = result.pop('hist_df')
                        results_list.append(result)
                        continue 
            except Exception as e:
                logging.warning(f"Failed to load cache for {ticker}, will re-fetch: {e}")
                
        tickers_to_fetch.append(ticker)
    
    cached_count = len(results_list)
    report_progress(0.15, f"(2/7) Loaded {cached_count} tickers from cache. Fetching {len(tickers_to_fetch)} new tickers...")
    
    processed_count = 0
    total_to_fetch = len(tickers_to_fetch)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in tickers_to_fetch}
        
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result(timeout=60) 
                
                if result.get('success', False):
                    cache_path = os.path.join(CACHE_DIR, f"{ticker}.pkl")
                    try:
                        with open(cache_path, 'wb') as f:
                            pickle.dump(result, f)
                    except Exception as e:
                        logging.warning(f"Failed to save cache for {ticker}: {e}")
                    
                    if 'hist_df' in result:
                        all_histories[ticker] = result.pop('hist_df') 
                    results_list.append(result)
                
                else:
                    logging.error(f"Failed to process {ticker}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logging.error(f"Error processing {ticker} in main loop: {e}", exc_info=True)
            
            processed_count += 1
            if total_to_fetch > 0:
                percent_done = 0.15 + (0.55 * (processed_count / total_to_fetch)) 
                report_progress(percent_done, f"(2/7) Processing: {ticker} ({processed_count}/{total_to_fetch})")

    end_time = time.time()
    report_progress(0.7, f"(3/7) Data fetch complete. Time taken: {end_time - start_time:.2f}s")

    if not results_list:
        report_progress(1.0, "Error: No data successfully processed. Analysis cancelled.")
        return None, None, None
        
    results_df = pd.DataFrame(results_list)
    results_df.set_index('ticker', inplace=True)
    
    report_progress(0.75, "(4/7) Risk metrics calculated in spus.py.")
    
    # --- 4. Factor Z-Score Calculation ---
    report_progress(0.8, "(5/7) Calculating robust Z-Scores...")
    # --- ⭐️ MODIFIED: Calls the global function ⭐️ ---
    results_df = calculate_all_z_scores(results_df, CONFIG)
    
    # --- 5. Save Reports (Excel, PDF, CSV) ---
    report_progress(0.9, "(6/7) Generating reports...")
    
    results_df.sort_values(by='Z_Value', ascending=False, inplace=True)

    results_df_display = results_df.rename(columns={
        'last_price': 'Last Price', 'Sector': 'Sector', 'marketCap': 'Market Cap',
        'forwardPE': 'Forward P/E', 'priceToBook': 'P/B Ratio', 'grahamValuation': 'Valuation (Graham)',
        'momentum_12m': 'Momentum (12M %)', 'volatility_1y': 'Volatility (1Y)',
        'returnOnEquity': 'ROE (%)', 'debtToEquity': 'Debt/Equity', 'profitMargins': 'Profit Margin (%)',
        'beta': 'Beta', 'RSI': 'RSI (14)', 'ADX': 'ADX (14)',
        'Final Stop Loss': 'Stop Loss' # <-- MODIFIED
    })
    
    pct_cols = ['ROE (%)', 'Profit Margin (%)', 'Momentum (12M %)', 'Risk % (to Stop)']
    for col in pct_cols:
        if col in results_df_display.columns:
            results_df_display[col] = results_df_display[col] * 100

    data_sheets = {
        'Top 20 (By Value)': results_df_display.sort_values(by='Z_Value', ascending=False).head(20),
        'Top 20 (By Momentum)': results_df_display.sort_values(by='Z_Momentum', ascending=False).head(20),
        'Top 20 (By Quality)': results_df_display.sort_values(by='Z_Quality', ascending=False).head(20),
        'Top Bullish Technicals': results_df_display.sort_values(by='Z_Technical', ascending=False).head(20),
        'Top Undervalued (Graham)': results_df_display[results_df_display['Valuation (Graham)'] == 'Undervalued (Graham)'].sort_values(by='Z_Value', ascending=False).head(20),
        'All Results (Raw)': results_df
    }

    excel_file_path = os.path.join(BASE_DIR, CONFIG['LOGGING']['EXCEL_FILE_PATH'])
    try:
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            for sheet_name, df_sheet in data_sheets.items():
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=True)
        report_progress(0.92, f"Excel report saved: {excel_file_path}")
    except Exception as e:
        logging.error(f"Failed to save Excel file: {e}")

    if REPORTLAB_AVAILABLE:
        try:
            timestamp = datetime.now(SAUDI_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            base_pdf_path = os.path.splitext(excel_file_path)[0]
            pdf_file_path = f"{base_pdf_path}_{datetime.now(SAUDI_TZ).strftime('%Y%m%d_%H%M%S')}.pdf"
            
            doc = SimpleDocTemplate(pdf_file_path, pagesize=landscape(letter))
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='Left', alignment=TA_LEFT))
            
            elements = [Paragraph(f"SPUS Quant Report - {timestamp}", styles['h1'])]
            
            pdf_cols = ['Last Price', 'Z_Value', 'Z_Momentum', 'Z_Quality', 'Risk/Reward Ratio']
            
            for sheet_name, df_sheet in data_sheets.items():
                if sheet_name == 'All Results (Raw)': continue 
                
                elements.append(Paragraph(sheet_name, styles['h2']))
                
                cols_to_show = [col for col in pdf_cols if col in df_sheet.columns]
                df_pdf = df_sheet.head(15).reset_index()[['ticker'] + cols_to_show]
                
                df_pdf = df_pdf.fillna('N/A')
                for col in cols_to_show:
                    if col in df_pdf.select_dtypes(include=[np.number]).columns:
                        df_pdf[col] = df_pdf[col].round(2)
                
                data = [df_pdf.columns.tolist()] + df_pdf.values.tolist()
                
                col_widths = [1.2*inch] + [1*inch] * len(cols_to_show)
                table = Table(data, hAlign='LEFT', colWidths=col_widths)
                
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.green),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 7),
                ]))
                elements.append(table)
                elements.append(Spacer(1, 0.25*inch))
                
            doc.build(elements)
            report_progress(0.95, f"PDF report saved: {pdf_file_path}")
        except Exception as e:
            logging.error(f"Failed to create PDF report: {e}")
    
    try:
        results_dir = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('RESULTS_DIR', 'results_history'))
        os.makedirs(results_dir, exist_ok=True)
        timestamp_csv = datetime.now(SAUDI_TZ).strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(results_dir, f"quant_results_{timestamp_csv}.csv")
        results_df.to_csv(csv_path)
        report_progress(0.98, f"Timestamped CSV saved: {csv_path}")
    except Exception as e:
        logging.error(f"Failed to save timestamped CSV: {e}")

    report_progress(1.0, "Analysis complete.")
    
    return results_df, all_histories, data_sheets

# --- ⭐️ 4. Streamlit UI Functions ⭐️ ---

@st.cache_data(show_spinner=False, ttl=3600) 
def load_analysis_data(_config, run_timestamp):
    """
    Streamlit cache wrapper for the core analysis function.
    """
    progress_bar = st.progress(0, text="Starting analysis...")
    status_text = st.empty()
    
    def st_progress_callback(percent, text):
        progress_bar.progress(percent, text=text)
        status_text.info(text)
        
    logging.info(f"Cache miss or manual run. Running full analysis... (Timestamp: {run_timestamp})")
    
    df, histories, sheets = generate_quant_report(_config, st_progress_callback)
    
    progress_bar.empty()
    status_text.empty()
    
    if df is None:
        st.error("Analysis failed. Check logs.")
        return None, None, None, None
        
    return df, histories, sheets, datetime.now(SAUDI_TZ).timestamp()

def get_latest_reports(excel_base_path):
    """Gets paths for the latest Excel and PDF reports."""
    base_dir = os.path.dirname(excel_base_path)
    excel_name_no_ext = os.path.splitext(os.path.basename(excel_base_path))[0]
    
    latest_pdf = None
    pdf_pattern = os.path.join(base_dir, f"{excel_name_no_ext}_*.pdf")
    pdf_files = glob.glob(pdf_pattern)
    if pdf_files:
        latest_pdf = max(pdf_files, key=os.path.getmtime)
        
    excel_path = excel_base_path if os.path.exists(excel_base_path) else None
    return excel_path, latest_pdf

# --- ✅ MODIFIED FUNCTION ---
def create_price_chart(hist_df, ticker_data): # <-- MODIFIED SIGNATURE
    """Creates an interactive Plotly Price Chart with SMAs, MACD, and OB Zones."""
    
    ticker = ticker_data.name # <-- Get ticker from data
    cfg = CONFIG['TECHNICALS']
    short_ma_col = f'SMA_{cfg["SHORT_MA_WINDOW"]}'
    long_ma_col = f'SMA_{cfg["LONG_MA_WINDOW"]}'
    macd_col = f'MACD_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
    macd_h_col = f'MACDh_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
    macd_s_col = f'MACDs_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, subplot_titles=(f'{ticker} Price', 'MACD'), 
                        row_width=[0.2, 0.7])

    fig.add_trace(go.Candlestick(x=hist_df.index,
                                open=hist_df['Open'],
                                high=hist_df['High'],
                                low=hist_df['Low'],
                                close=hist_df['Close'],
                                name='Price'),
                  row=1, col=1)
    
    fig.add_trace(go.Scatter(x=hist_df.index, y=hist_df[short_ma_col], 
                             line=dict(color='orange', width=1), name=f'SMA {cfg["SHORT_MA_WINDOW"]}'),
                  row=1, col=1)
    
    fig.add_trace(go.Scatter(x=hist_df.index, y=hist_df[long_ma_col], 
                             line=dict(color='blue', width=1), name=f'SMA {cfg["LONG_MA_WINDOW"]}'),
                  row=1, col=1)

    fig.add_trace(go.Bar(x=hist_df.index, y=hist_df[macd_h_col], 
                         name='Histogram',
                         marker_color=np.where(hist_df[macd_h_col] < 0, 'red', 'green')),
                  row=2, col=1)
    
    fig.add_trace(go.Scatter(x=hist_df.index, y=hist_df[macd_col], 
                             line=dict(color='blue', width=1), name='MACD'),
                  row=2, col=1)
    
    fig.add_trace(go.Scatter(x=hist_df.index, y=hist_df[macd_s_col], 
                             line=dict(color='orange', width=1), name='Signal'),
                  row=2, col=1)

    # --- ✅ NEW: Add Key Price Zones ---
    
    # Get data from the series
    b_ob_low = ticker_data.get('bullish_ob_low', np.nan)
    b_ob_high = ticker_data.get('bullish_ob_high', np.nan)
    be_ob_low = ticker_data.get('bearish_ob_low', np.nan)
    be_ob_high = ticker_data.get('bearish_ob_high', np.nan)
    last_sl = ticker_data.get('last_swing_low', np.nan)
    last_sh = ticker_data.get('last_swing_high', np.nan)
    
    # Get the date range of the chart
    chart_start_date = hist_df.index.min()
    chart_end_date = hist_df.index.max()

    # 1. Bullish OB (Demand Zone) - Green Rectangle
    if pd.notna(b_ob_low) and pd.notna(b_ob_high):
        fig.add_shape(
            type="rect",
            x0=chart_start_date, y0=b_ob_low,
            x1=chart_end_date, y1=b_ob_high,
            line=dict(width=0),
            fillcolor="rgba(0, 255, 0, 0.2)", # Green, 20% opacity
            layer="below",
            row=1, col=1
        )

    # 2. Bearish OB (Supply Zone) - Red Rectangle
    if pd.notna(be_ob_low) and pd.notna(be_ob_high):
        fig.add_shape(
            type="rect",
            x0=chart_start_date, y0=be_ob_low,
            x1=chart_end_date, y1=be_ob_high,
            line=dict(width=0),
            fillcolor="rgba(255, 0, 0, 0.2)", # Red, 20% opacity
            layer="below",
            row=1, col=1
        )

    # 3. Last Swing Low (Support) - Dotted Blue Line
    if pd.notna(last_sl):
        fig.add_hline(
            y=last_sl,
            line=dict(color="blue", width=1, dash="dot"),
            annotation_text="Swing Low",
            annotation_position="bottom right",
            row=1, col=1
        )

    # 4. Last Swing High (Resistance) - Dotted Red Line
    if pd.notna(last_sh):
        fig.add_hline(
            y=last_sh,
            line=dict(color="red", width=1, dash="dot"),
            annotation_text="Swing High",
            annotation_position="top right",
            row=1, col=1
        )
    # --- ✅ END OF NEW CODE ---
    
    fig.update_layout(
        title_text=f"{ticker} Technical Chart",
        xaxis_rangeslider_visible=False,
        height=500,
        legend_orientation="h",
        legend_yanchor="bottom",
        legend_y=1.02,
        legend_xanchor="right",
        legend_x=1
    )
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="MACD", row=2, col=1)
    
    return fig

def create_radar_chart(ticker_data, factor_cols):
    """Creates a Plotly Radar Chart for factor explainability."""
    
    values = ticker_data[factor_cols].values.flatten().tolist()
    theta = [col.replace('Z_', '') for col in factor_cols]
    
    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values + [values[0]], # Close the loop
        theta=theta + [theta[0]], # Close the loop
        fill='toself',
        name='Factor Z-Score'
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[min(-2, min(values)-0.5), max(2, max(values)+0.5)] 
            )
        ),
        title=f"Factor Profile for {ticker_data.name}",
        height=400
    )
    return fig

# --- ✅ MODIFIED FUNCTION ---
def display_buy_signal_checklist(ticker_data):
    """
    Displays a 5-step checklist on the Ticker Deep Dive tab,
    showing which buy criteria are met.
    """
    
    SCORE_THRESHOLD = 1.0
    FACTOR_Z_THRESHOLD = 0.5 
    RR_RATIO_THRESHOLD = 1.5

    # Step 1: Quant Score
    step1_met = False
    step1_text = f"**1. Quant Score > {SCORE_THRESHOLD}**"
    score = ticker_data.get('Final Quant Score', 0)
    if pd.notna(score) and score > SCORE_THRESHOLD:
        step1_met = True
    step1_details = f"Score is {score:.2f}"

    # Step 2: Factor Profile (Value & Quality)
    step2_met = False
    step2_text = f"**2. Value & Quality > {FACTOR_Z_THRESHOLD}**"
    z_value = ticker_data.get('Z_Value', -99)
    z_quality = ticker_data.get('Z_Quality', -99)
    if pd.notna(z_value) and pd.notna(z_quality) and (z_value > FACTOR_Z_THRESHOLD) and (z_quality > FACTOR_Z_THRESHOLD):
        step2_met = True
    step2_details = f"Value: {z_value:.2f}, Quality: {z_quality:.2f}"

    # --- NEW: Step 3: Favorable Technicals ---
    step3_met = False
    step3_text = "**3. Favorable Technicals**"
    
    RSI_OVERBOUGHT = 70.0
    TREND_GOOD = ["Confirmed Uptrend", "Uptrend (Correction)"]
    MACD_GOOD = ["Bullish Crossover", "Bullish"]

    rsi_val = ticker_data.get('RSI', 99)
    trend_val = ticker_data.get('Trend (50/200 Day MA)', 'N/A')
    macd_val = ticker_data.get('MACD_Signal', 'N/A')

    is_rsi_ok = pd.notna(rsi_val) and rsi_val < RSI_OVERBOUGHT
    is_trend_ok = trend_val in TREND_GOOD
    is_macd_ok = macd_val in MACD_GOOD

    if is_rsi_ok and is_trend_ok and is_macd_ok:
        step3_met = True
        
    rsi_icon = "✅" if is_rsi_ok else "❌"
    trend_icon = "✅" if is_trend_ok else "❌"
    macd_icon = "✅" if is_macd_ok else "❌"
    
    # Use <br> for line breaks, requires unsafe_allow_html=True later
    step3_details = f"{trend_icon} Trend: {trend_val}<br>{macd_icon} MACD: {macd_val}<br>{rsi_icon} RSI: {rsi_val:.1f}"

    # --- Step 4: SMC Entry Signal (Was Step 3) ---
    step4_met = False
    step4_text = "**4. SMC Entry Signal**"
    entry_signal = ticker_data.get('entry_signal', 'No Trade')
    has_fvg = ticker_data.get('bullish_ob_fvg', False)
    has_vol = ticker_data.get('bullish_ob_volume_ok', False)
    
    details = []
    if entry_signal == 'Buy near Bullish OB':
        step4_met = True
        details.append(f"Signal: {entry_signal}")
    elif entry_signal == 'Sell near Bearish OB':
        details.append(f"Signal: {entry_signal}")
    else:
        details.append("Signal: No Trade")

    details.append(f"FVG: {'✅' if has_fvg else '❌'}")
    details.append(f"Vol: {'✅' if has_vol else '❌'}")
    
    step4_details = ", ".join(details)
        
    # --- Step 5: Risk/Reward (Was Step 4) ---
    step5_met = False
    step5_text = f"**5. R/R Ratio > {RR_RATIO_THRESHOLD}**"
    rr_ratio = ticker_data.get('Risk/Reward Ratio', 0)
    if pd.notna(rr_ratio) and rr_ratio > RR_RATIO_THRESHOLD:
        step5_met = True
    step5_details = f"Ratio is {rr_ratio:.2f}"
    
    st.subheader("Buy Signal Checklist")
    cols = st.columns(5) # <-- MODIFIED to 5 columns
    
    criteria = [
        (step1_met, step1_text, step1_details),
        (step2_met, step2_text, step2_details),
        (step3_met, step3_text, step3_details), # <-- NEW STEP
        (step4_met, step4_text, step4_details), # <-- Was Step 3
        (step5_met, step5_text, step5_details)  # <-- Was Step 4
    ]
    
    for i, (met, text, details) in enumerate(criteria):
        with cols[i]:
            icon = "✅" if met else "❌"
            st.markdown(f"**{icon} {text}**")
            st.markdown(details, unsafe_allow_html=True) # <-- MODIFIED to allow HTML


# --- ⭐️ 5. Main Streamlit Application ⭐️ ---

def main():
    
    # --- Initialize Session State ---
    if 'selected_ticker' not in st.session_state:
        st.session_state.selected_ticker = None
    if 'run_timestamp' not in st.session_state:
        st.session_state.run_timestamp = time.time() 
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "🏆 Quant Rankings"
    # --- ✅ NEW: Initialize Portfolio ---
    if 'portfolio' not in st.session_state:
        st.session_state.portfolio = []
    
    load_css()
    
    global CONFIG 
    CONFIG = load_config('config.json')
    if CONFIG is None:
        st.error("FATAL: config.json not found. App cannot start.")
        st.stop()

    log_file_path = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('LOG_FILE_PATH', 'spus_analysis.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            logging.StreamHandler()
        ]
    )

    # --- Sidebar ---
    with st.sidebar:
        try:
            st.image("logo.jpg", width=200)
        except Exception as e:
            st.warning(f"Could not load logo.jpg: {e}")
        
        st.title("SPUS Quant Analyzer")
        st.markdown("Research-Grade Multi-Factor Analysis")
        st.divider()

        st.subheader("Controls")
        if st.button("🔄 Run Full Analysis", type="primary"):
            st.session_state.selected_ticker = None
            st.session_state.run_timestamp = time.time() 
            st.session_state.active_tab = "🏆 Quant Rankings"
            if 'raw_df' in st.session_state:
                del st.session_state['raw_df']
            st.rerun()
        
        # --- ⭐️ MODIFIED: Stock Analyzer Section ⭐️ ---
        st.divider()
        st.subheader("Stock Analyzer")
        new_ticker = st.text_input("Analyze Single Ticker:", placeholder="e.g., MSFT").upper().strip()
        
        if st.button("Analyze and Deep Dive"):
            if new_ticker:
                # Ensure data is loaded into session state first
                if 'raw_df' not in st.session_state:
                    # This check is needed in case the app just started
                    # We'll trigger a rerun, which will populate st.session_state.raw_df
                    st.warning("Priming data... please click 'Analyze' again.")
                    st.rerun() 
                elif new_ticker in st.session_state.raw_df.index:
                    st.success(f"'{new_ticker}' is already loaded.")
                    st.session_state.selected_ticker = new_ticker
                    st.session_state.active_tab = "🔬 Ticker Deep Dive"
                    st.rerun()
                else:
                    with st.spinner(f"Processing data for {new_ticker}..."):
                        try:
                            result = process_ticker(new_ticker)
                            
                            if result and result.get('success', False):
                                new_hist_df = result.pop('hist_df', None)
                                if new_hist_df is not None:
                                    st.session_state.all_histories[new_ticker] = new_hist_df
                                
                                new_ticker_df = pd.DataFrame([result])
                                new_ticker_df.set_index('ticker', inplace=True)
                                
                                st.session_state.raw_df = pd.concat([st.session_state.raw_df, new_ticker_df])
                                
                                st.info(f"Re-calculating Z-Scores for {len(st.session_state.raw_df)} stocks...")
                                # --- ⭐️ MODIFIED: Calls global function ⭐️ ---
                                st.session_state.raw_df = calculate_all_z_scores(st.session_state.raw_df, CONFIG)
                                
                                st.success(f"Successfully added '{new_ticker}'.")
                                st.session_state.selected_ticker = new_ticker
                                st.session_state.active_tab = "🔬 Ticker Deep Dive"
                                st.rerun()
                                
                            else:
                                st.error(f"Failed to fetch data for {new_ticker}. Error: {result.get('error', 'Unknown')}")
                        except Exception as e:
                            st.error(f"An exception occurred while processing {new_ticker}: {e}")
            else:
                st.warning("Please enter a ticker symbol.")
        
        st.divider()
        # --- ⭐️ END OF MODIFICATION ⭐️ ---

        default_weights = CONFIG.get('DEFAULT_FACTOR_WEIGHTS', {
            "Value": 0.20, "Momentum": 0.20, "Quality": 0.20,
            "Size": 0.10, "LowVolatility": 0.15, "Technical": 0.15
        })
        
        def callback_reset_weights():
            for factor in default_weights.keys():
                key_to_del = f"weight_{factor}" 
                if key_to_del in st.session_state:
                    del st.session_state[key_to_del]

        st.button("Reset Factor Weights", on_click=callback_reset_weights)
        
        st.subheader("Factor Weights")
        st.info("Adjust weights to re-rank stocks. Weights will be normalized.")
        
        weights = {}
        for factor, default in default_weights.items():
            weights[factor] = st.slider(factor, 0.0, 1.0, default, 0.05, key=f"weight_{factor}")
            
        total_weight = sum(weights.values())
        norm_weights = {f: (w / total_weight) if total_weight > 0 else 0 for f, w in weights.items()}
        
        with st.expander("Normalized Weights"):
            for factor, weight in norm_weights.items():
                st.write(f"{factor}: {weight*100:.1f}%")
        
        st.divider()

        st.subheader("Downloads")
        excel_path, pdf_path = get_latest_reports(os.path.join(BASE_DIR, CONFIG['LOGGING']['EXCEL_FILE_PATH']))
        
        if excel_path:
            with open(excel_path, "rb") as file:
                st.download_button(
                    label="📥 Download Excel Report",
                    data=file,
                    file_name=os.path.basename(excel_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        else:
            st.info("Run analysis to generate reports.")

        if pdf_path:
            with open(pdf_path, "rb") as file:
                st.download_button(
                    label="📄 Download PDF Report",
                    data=file,
                    file_name=os.path.basename(pdf_path),
                    mime="application/pdf",
                )
        
        st.divider()
        st.info("Analysis data is cached for 1 hour. Click 'Run' for fresh data.")


    # --- Main Page ---
    st.title("SPUS Quantitative Dashboard")
    
    # --- ⭐️ MODIFIED: Load Data into Session State ⭐️ ---
    
    base_raw_df, base_histories, base_sheets, base_last_run_time = load_analysis_data(CONFIG, st.session_state.run_timestamp)
    
    if 'raw_df' not in st.session_state or st.session_state.get('base_run_timestamp') != st.session_state.run_timestamp:
        if base_raw_df is None:
            st.error("Analysis failed to produce base data. App cannot continue.")
            st.stop()
        
        st.session_state.raw_df = base_raw_df.copy()
        st.session_state.all_histories = base_histories.copy()
        st.session_state.data_sheets = base_sheets
        st.session_state.last_run_time = base_last_run_time
        st.session_state.base_run_timestamp = st.session_state.run_timestamp
    
    raw_df = st.session_state.raw_df
    all_histories = st.session_state.all_histories
    last_run_time = st.session_state.last_run_time
    
    if raw_df is None or raw_df.empty:
        st.error("No data available in session state.")
        st.stop()
        
    st.success(f"Data loaded from analysis run at: {datetime.fromtimestamp(last_run_time, SAUDI_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # --- 7. UI: Dynamic Score Calculation & Filtering ---
    df = raw_df.copy()
    
    if df.empty:
        st.error("No stock data was successfully loaded. Check logs and data sources.")
        st.stop()

    df['Final Quant Score'] = 0.0
    factor_z_cols = []
    for factor, weight in norm_weights.items():
        z_col = f"Z_{factor}"
        factor_z_cols.append(z_col)
        if z_col in df.columns:
            df[f"Weighted_{z_col}"] = df[z_col] * weight
            df['Final Quant Score'] += df[f"Weighted_{z_col}"]
        else:
            logging.warning(f"Z-Score column {z_col} not found in dataframe.")

    st.subheader("Filters")
    
    # --- ⭐️ NEW: Filter section modified ⭐️ ---
    
    # Row 1: Sector & Market Cap
    filt_col1, filt_col2 = st.columns(2)
    
    all_sectors = sorted(df['Sector'].unique())
    selected_sectors = filt_col1.multiselect("Filter by Sector:", all_sectors, default=all_sectors)
    
    if df.empty or 'marketCap' not in df.columns or df['marketCap'].isnull().all():
        filt_col2.info("No Market Cap data to filter.")
        cap_range = (0.0, 0.0) 
    else:
        min_cap_val = float(df['marketCap'].min())
        max_cap_val = float(df['marketCap'].max())

        if min_cap_val == max_cap_val:
            min_cap = (min_cap_val / 1e9) * 0.9 
            max_cap = (max_cap_val / 1e9) * 1.1 
            if min_cap < 0: min_cap = 0.0
        else:
            min_cap = min_cap_val / 1e9
            max_cap = max_cap_val / 1e9
        
        if min_cap >= max_cap:
            min_cap = max_cap - 1.0
            if min_cap < 0: min_cap = 0.0

        cap_range = filt_col2.slider(
            "Filter by Market Cap (Billions):",
            min_value=min_cap,
            max_value=max_cap,
            value=(min_cap, max_cap),
            format="%.1f B"
        )
        
    # --- NEW: Row 2: Trend & Entry Signal ---
    filt_col3, filt_col4 = st.columns(2)

    all_trends = sorted(df['Trend (50/200 Day MA)'].unique())
    selected_trends = filt_col3.multiselect("Filter by MA Trend:", all_trends, default=all_trends)
    
    all_signals = sorted(df['entry_signal'].unique())
    selected_signals = filt_col4.multiselect("Filter by Entry Signal:", all_signals, default=all_signals)
    
    # --- ✅ MODIFIED: Row 3: Cut Loss % ---
    filt_col5, _ = st.columns(2) # Use first col, leave second blank

    if 'pct_above_cutloss' in df.columns and not df['pct_above_cutloss'].isnull().all():
        # Filter out NaNs before finding min/max
        valid_pct_data = df['pct_above_cutloss'].dropna()
        if not valid_pct_data.empty:
            min_pct = float(valid_pct_data.min())
            max_pct = float(valid_pct_data.max())
            default_min_pct = max(0.0, min_pct) # Start filter at 0% or higher
            if min_pct >= max_pct: max_pct = min_pct + 10.0
            
            support_range = filt_col5.slider(
                "Filter by % Above Cut Loss (Swing Low):", # <-- MODIFIED LABEL
                min_value=min_pct, 
                max_value=max_pct,
                value=(default_min_pct, max_pct),
                format="%.1f%%"
            )
        else:
            filt_col5.info("No Cut Loss % data to filter.")
            support_range = (0.0, 0.0)
    else:
        filt_col5.info("No Cut Loss % data to filter.")
        support_range = (0.0, 0.0)
    

    # --- NEW: Combined Filtering Logic ---
    
    # Base filter criteria
    base_filters = (
        (df['Sector'].isin(selected_sectors)) &
        (df['Trend (50/200 Day MA)'].isin(selected_trends)) &
        (df['entry_signal'].isin(selected_signals)) # <-- ADDED
    )

    # Market Cap filter (only if valid range)
    if cap_range != (0.0, 0.0) and 'marketCap' in df.columns and not df['marketCap'].isnull().all():
        cap_filter = (
            (df['marketCap'].ge(cap_range[0] * 1e9)) &
            (df['marketCap'].le(cap_range[1] * 1e9))
        )
        base_filters &= cap_filter

    # ✅ MODIFIED: Cut Loss % filter (only if valid range and data exists)
    if support_range != (0.0, 0.0) and 'pct_above_cutloss' in df.columns and not df['pct_above_cutloss'].isnull().all():
        support_filter = (
            (df['pct_above_cutloss'].ge(support_range[0])) &
            (df['pct_above_cutloss'].le(support_range[1]))
        )
        base_filters &= support_filter

    filtered_df = df[base_filters].copy()
    
    # --- END OF NEW FILTER LOGIC ---
    
    filtered_df.sort_values(by='Final Quant Score', ascending=False, inplace=True)
    
    st.markdown(f"Displaying **{len(filtered_df)}** of **{len(df)}** total stocks matching filters.")
    st.divider()

    # --- ⭐️ MODIFIED: Replaced st.tabs with st.radio ⭐️ ---
    
    # --- ✅ NEW: Added Portfolio Tab ---
    tab_list = ["🏆 Quant Rankings", "🔬 Ticker Deep Dive", "📈 Portfolio Analytics", "💼 My Portfolio"]
    
    # --- THIS IS THE FIX ---
    # We find the index of the tab we *want* to be active from our session state.
    try:
        default_idx = tab_list.index(st.session_state.active_tab)
    except ValueError:
        default_idx = 0 # Default to the first tab if not found

    # We create the radio button using index= instead of key=
    selected_tab = st.radio(
        "Navigation:",
        tab_list,
        index=default_idx, # Set the default selected tab
        horizontal=True
    )
    
    # After the widget is rendered, we update the session state
    # in case the *user* clicked a different tab.
    st.session_state.active_tab = selected_tab
    # --- END OF FIX ---
    
    # --- Tab 1: Quant Rankings ---
    if selected_tab == "🏆 Quant Rankings":
        st.header("🏆 Top Stocks by Final Quant Score")
        st.info("Click a ticker to select it and automatically move to the 'Ticker Deep Dive' tab.")
        
        # --- ✅ MODIFIED: Updated 5-step guide ---
        with st.expander("How to Find a Good Buy Signal (5-Step Guide)", expanded=False):
            st.markdown("""
                This 5-step method helps you use the app to find suitable buying opportunities.
                
                ### 1. Check the Final Quant Score (The "What")
                This is your primary signal. Look for stocks with a **high positive score** (e.g., > 1.0) 
                in the ranked list below. 
                
                ### 2. Check the Factor Profile (The "Why")
                Click a stock and go to the **"🔬 Ticker Deep Dive"** tab. Look at the 
                **"Factor Profile"** radar chart. This tells you *why* the score is high. 
                Is it high on `Value` (it's cheap) and `Quality` (it's a good company)? 
                
                ### 3. Check the Technicals (The "Momentum")
                On the **"Deep Dive"** tab, look at the **"Buy Signal Checklist"**.
                * **Technicals:** Are the `Trend`, `MACD`, and `RSI` all favorable (✅)?
                
                ### 4. Check the SMC Signal (The "When")
                This is your high-precision entry signal.
                * **Entry Signal:** Does it show **"Buy near Bullish OB"**?
                * **FVG / Vol:** Does the checklist show a `✅` for **FVG** (Fair Value Gap) and **Vol** (BOS Volume)? This confirms a high-quality signal.

                ### 5. Check the Risk & Sizing (The "How")
                In the **"Risk & Position Sizing"** section, check the:
                * **Risk/Reward Ratio:** Is it favorable (e.g., > 1.5)?
                * **Final Stop Loss:** Is this exit price (based on ATR or Cut-Loss) acceptable?
            """)
        
        rank_col1, rank_col2 = st.columns([1, 2])
        
        with rank_col1:
            st.subheader(f"Ranked List ({len(filtered_df)})")
            
            with st.container(height=800):
                if filtered_df.empty:
                    st.warning("No stocks match the current filters.")
                else:
                    for ticker in filtered_df.index:
                        score = filtered_df.loc[ticker, 'Final Quant Score']
                        label = f"{ticker} (Score: {score:.3f})"
                        
                        is_selected = (st.session_state.selected_ticker == ticker)
                        button_type = "primary" if is_selected else "secondary"
                        
                        # --- ⭐️ MODIFIED: Button click now changes state and reruns ⭐️ ---
                        if st.button(label, key=f"rank_{ticker}", use_container_width=True, type=button_type):
                            st.session_state.selected_ticker = ticker
                            st.session_state.active_tab = "🔬 Ticker Deep Dive"
                            st.rerun()
        
        with rank_col2:
            st.subheader("Top 20 Overview")
            
            display_cols = [
                'Last Price', 'Sector', 
                'entry_signal', # <-- NEW
                'Final Quant Score', 
                'Z_Value', 'Z_Momentum', 'Z_Quality', 
                'Z_Size', 'Z_LowVolatility', 'Z_Technical',
                'Risk/Reward Ratio',
                'Position Size (USD)',
                'pct_above_cutloss' # <-- MODIFIED
            ]
            display_cols = [c for c in display_cols if c in filtered_df.columns]
            
            filtered_df_display = filtered_df.copy()
            if 'marketCap' in filtered_df_display.columns:
                filtered_df_display['Market Cap'] = filtered_df_display['marketCap'] / 1e9
            
            st.dataframe(
                filtered_df_display.head(20)[display_cols],
                column_config={
                    "Last Price": st.column_config.NumberColumn(format="$%.2f"),
                    "Market Cap": st.column_config.NumberColumn(format="%.1f B", help="Market Cap in Billions"),
                    "entry_signal": st.column_config.TextColumn("SMC Signal"), # <-- NEW
                    "Final Quant Score": st.column_config.NumberColumn(format="%.3f"),
                    "Z_Value": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Momentum": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Quality": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Size": st.column_config.NumberColumn(format="%.2f"),
                    "Z_LowVolatility": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Technical": st.column_config.NumberColumn(format="%.2f"),
                    "Risk/Reward Ratio": st.column_config.NumberColumn(format="%.2f"),
                    "Position Size (USD)": st.column_config.NumberColumn(format="$%,.0f"),
                    "pct_above_cutloss": st.column_config.NumberColumn(format="%.1f%%", help="% Above Cut Loss (Swing Low)"),
                },
                use_container_width=True,
                height=700
            )

    # --- Tab 2: Ticker Deep Dive ---
    elif selected_tab == "🔬 Ticker Deep Dive":
        st.header("🔬 Ticker Deep Dive")
        
        selected_ticker = st.session_state.selected_ticker
        
        if selected_ticker is None:
            st.info("Go to the 'Quant Rankings' tab and click a ticker to see details.")
        elif filtered_df.empty:
            st.info("Go to the 'Quant Rankings' tab and click a ticker to see details.")
        elif selected_ticker not in filtered_df.index:
            try:
                ticker_data = st.session_state.raw_df.loc[selected_ticker]
                hist_data = all_histories.get(selected_ticker)
                st.warning(f"'{selected_ticker}' is not in the currently filtered list, but analysis is available.")
                # --- ⭐️ MODIFIED: Calls helper function ⭐️ ---
                display_deep_dive_details(ticker_data, hist_data, all_histories, factor_z_cols, norm_weights, filtered_df)
            except KeyError:
                st.error(f"Ticker '{selected_ticker}' not found in any data. Try the 'Stock Analyzer'.")
            
        else:
            ticker_data = filtered_df.loc[selected_ticker]
            hist_data = all_histories.get(selected_ticker)
            # --- ⭐️ MODIFIED: Calls helper function ⭐️ ---
            display_deep_dive_details(ticker_data, hist_data, all_histories, factor_z_cols, norm_weights, filtered_df)

    # --- Tab 3: Portfolio Analytics ---
    elif selected_tab == "📈 Portfolio Analytics":
        st.header("📈 Portfolio-Level Analytics")
        
        if filtered_df.empty:
            st.warning("No data to display. Adjust filters.")
        else:
            port_col1, port_col2 = st.columns(2)
            
            with port_col1:
                st.subheader("Factor Correlation Heatmap")
                st.info("This shows if factors are redundant (highly correlated). Aim for low values.")
                
                corr_matrix = filtered_df[factor_z_cols].corr()
                corr_heatmap = px.imshow(
                    corr_matrix,
                    text_auto=".2f",
                    aspect="auto",
                    color_continuous_scale='RdBu_r', 
                    zmin=-1, zmax=1,
                    title="Factor Z-Score Correlation Matrix"
                )
                st.plotly_chart(corr_heatmap, use_container_width=True)
                
            with port_col2:
                st.subheader("Sector Median Factor Strength")
                st.info("This shows which factors are strongest/weakest for each sector.")
                
                sector_median_factors = filtered_df.groupby('Sector')[factor_z_cols].median()
                sector_heatmap = px.imshow(
                    sector_median_factors,
                    text_auto=".2f",
                    aspect="auto",
                    color_continuous_scale='Viridis',
                    title="Median Factor Z-Score by Sector"
                )
                st.plotly_chart(sector_heatmap, use_container_width=True)
    
    # --- ✅ NEW: Tab 4: My Portfolio ---
    elif selected_tab == "💼 My Portfolio":
        display_portfolio_tab(st.session_state.raw_df)
            
# --- ⭐️ NEW HELPER FUNCTION ⭐️ ---
def display_deep_dive_details(ticker_data, hist_data, all_histories, factor_z_cols, norm_weights, filtered_df):
    """
    Helper function to display the full Ticker Deep Dive page.
    This avoids code duplication.
    """
    selected_ticker = ticker_data.name
    st.subheader(f"Analysis for: {selected_ticker}")

    # Add Previous/Next Buttons
    try:
        ticker_list = filtered_df.index.tolist() # Based on filtered list
        current_index = ticker_list.index(selected_ticker)
        prev_col, next_col = st.columns(2)
        
        is_first = (current_index == 0)
        if prev_col.button("⬅️ Previous", use_container_width=True, disabled=is_first, key="prev_ticker"):
            st.session_state.selected_ticker = ticker_list[current_index - 1]
            st.rerun()
            
        is_last = (current_index == len(ticker_list) - 1)
        if next_col.button("Next ➡️", use_container_width=True, disabled=is_last, key="next_ticker"):
            st.session_state.selected_ticker = ticker_list[current_index + 1]
            st.rerun()

    except ValueError:
        st.info("Previous/Next navigation is only available for stocks in the filtered list.")

    # Buy Signal Checklist
    display_buy_signal_checklist(ticker_data)
    st.divider()

    if pd.notna(ticker_data.get('data_warning')):
        st.warning(f"⚠️ **Data Warning:** {ticker_data['data_warning']}")
    
    st.markdown(f"**Sector:** {ticker_data['Sector']} | **Data Source:** `{ticker_data['source']}`")
    
    # --- MODIFIED: Key Metrics (Added Entry Signal) ---
    kpi_cols = st.columns(6) 
    
    # --- NEW: Entry Signal Metric ---
    entry_signal = ticker_data.get('entry_signal', 'No Trade')
    delta_text = "-"
    delta_color = "off"
    if entry_signal == 'Buy near Bullish OB':
        delta_text = "BUY"
        delta_color = "normal" 
    elif entry_signal == 'Sell near Bearish OB':
        delta_text = "SELL"
        delta_color = "inverse"
        
    kpi_cols[0].metric("SMC Entry Signal", entry_signal, delta=delta_text, delta_color=delta_color)
    # --- END OF NEW ---

    kpi_cols[1].metric("Final Quant Score", f"{ticker_data['Final Quant Score']:.3f}")
    kpi_cols[2].metric("Last Price", f"${ticker_data['last_price']:.2f}")
    kpi_cols[3].metric("Market Cap", f"${ticker_data['marketCap']/1e9:.1f} B")
    kpi_cols[4].metric("Trend (50/200 MA)", ticker_data['Trend (50/200 Day MA)'])
    
    last_div_val = ticker_data.get('last_dividend_value', np.nan)
    last_div_date = ticker_data.get('last_dividend_date', 'N/A')
    div_display = f"${last_div_val:.2f}" if pd.notna(last_div_val) else "N/A"
    div_help = f"Paid on: {last_div_date}"
    kpi_cols[5].metric("Last Dividend", div_display, help=div_help)
    
    st.divider()

    # --- NEW: Latest News Section ---
    st.subheader("Latest News")
    news_list_str = ticker_data.get('news_list', 'N/A')
    has_recent_news = ticker_data.get('recent_news', 'No') == 'Yes'
    
    if has_recent_news:
        st.markdown("🔥 **Recent News Detected (Last 48h)**")

    if news_list_str == "N/A" or not news_list_str:
        st.info("No news headlines found.")
    else:
        with st.expander("View Latest Headlines", expanded=False):
            # News is already a comma-separated string
            news_list = news_list_str.split(", ")
            for i, headline in enumerate(news_list):
                st.markdown(f"- {headline}")
    # --- END OF NEW ---

    st.divider() 
    
    # Charts
    chart_col1, chart_col2 = st.columns([2, 1])
    with chart_col1:
        st.subheader("Price Chart & Technicals")
        if hist_data is not None:
            # --- ✅ MODIFIED: Pass ticker_data to the chart function ---
            price_chart = create_price_chart(hist_data, ticker_data)
            st.plotly_chart(price_chart, use_container_width=True)
        else:
            st.error("Historical data not found for this ticker.")
            
    with chart_col2:
        st.subheader("Factor Profile")
        radar_chart = create_radar_chart(ticker_data, factor_z_cols)
        st.plotly_chart(radar_chart, use_container_width=True)
        
        with st.expander("Factor Contribution Breakdown", expanded=False):
            for factor in norm_weights.keys():
                z_col = f"Z_{factor}"
                w_z_col = f"Weighted_{z_col}"
                st.metric(
                    label=f"{factor} (Z-Score: {ticker_data[z_col]:.2f})",
                    value=f"Contrib: {ticker_data[w_z_col]:.3f}",
                    help=f"Weight: {norm_weights[factor]*100:.1f}%"
                )

    st.divider()
    
    # --- MODIFIED: Risk, Value & Position Sizing Metrics ---
    st.subheader("Risk & Position Sizing")
    risk_col1, risk_col2, risk_col3, risk_col4 = st.columns(4)

    # Column 1: Stop Losses
    with risk_col1:
        sl_atr = ticker_data.get('Stop Loss (ATR)', np.nan)
        sl_cut = ticker_data.get('Stop Loss (Cut Loss)', np.nan)
        sl_final = ticker_data.get('Final Stop Loss', np.nan)
        sl_method = ticker_data.get('SL_Method', 'N/A')
        risk_pct = ticker_data.get('Risk % (to Stop)', np.nan)
        risk_display = f"Risk %: {risk_pct:.1f}%" if pd.notna(risk_pct) else "N/A"

        st.metric("Stop Loss (ATR)", f"${sl_atr:.2f}" if pd.notna(sl_atr) else "N/A")
        st.metric("Stop Loss (Cut Loss)", f"${sl_cut:.2f}" if pd.notna(sl_cut) else "N/A", help="Based on last swing low")
        st.metric(f"Final Stop ({sl_method})", f"${sl_final:.2f}" if pd.notna(sl_final) else "N/A", help=risk_display)

    # Column 2: Profit & R/R
    with risk_col2:
        tp_price = ticker_data.get('Take Profit Price', np.nan)
        rr_ratio = ticker_data.get('Risk/Reward Ratio', np.nan)
        tp_display = f"${tp_price:.2f}" if pd.notna(tp_price) else "N/A"
        rr_display = f"{rr_ratio:.2f}" if pd.notna(rr_ratio) else "N/A"

        st.metric("Take Profit (Fib 1.618)", tp_display)
        st.metric("Risk/Reward Ratio", rr_display)

    # Column 3 & 4: Position Sizing
    with risk_col3:
        pos_shares = ticker_data.get('Position Size (Shares)', np.nan)
        pos_display = f"{pos_shares:.0f} Shares" if pd.notna(pos_shares) else "N/A"
        risk_usd = ticker_data.get('Risk Per Trade (USD)', 50)
        st.metric("Position Size (Shares)", pos_display, help=f"Based on ${risk_usd:,.0f} risk")
    
    with risk_col4:
        pos_usd = ticker_data.get('Position Size (USD)', np.nan)
        pos_usd_display = f"${pos_usd:,.0f}" if pd.notna(pos_usd) else "N/A"
        st.metric("Position Size (USD)", pos_usd_display, help="Shares * Last Price")

    st.divider() 

    # --- ⭐️ MODIFIED: Key Price Zones Section (Added Validation) ⭐️ ---
    st.subheader("Key Price Zones")
    zone_cols = st.columns(4)
    
    # 1. Bullish Order Block (NEW)
    b_ob_low = ticker_data.get('bullish_ob_low', np.nan)
    b_ob_high = ticker_data.get('bullish_ob_high', np.nan)
    b_ob_validated = ticker_data.get('bullish_ob_validated', False)
    b_ob_fvg = ticker_data.get('bullish_ob_fvg', False)
    b_ob_vol = ticker_data.get('bullish_ob_volume_ok', False)
    
    b_ob_label = f"{'✅ Mitigated' if b_ob_validated else 'Fresh'} Bullish OB"
    b_ob_display = f"${b_ob_low:.2f} - ${b_ob_high:.2f}" if pd.notna(b_ob_low) else "N/A"
    b_ob_help = f"FVG: {'Yes' if b_ob_fvg else 'No'} | BOS Vol: {'High' if b_ob_vol else 'Low'}"
    zone_cols[0].metric(b_ob_label, b_ob_display, help=b_ob_help)
    
    # 2. Bearish Order Block (NEW)
    be_ob_low = ticker_data.get('bearish_ob_low', np.nan)
    be_ob_high = ticker_data.get('bearish_ob_high', np.nan)
    be_ob_validated = ticker_data.get('bearish_ob_validated', False)
    be_ob_fvg = ticker_data.get('bearish_ob_fvg', False)
    be_ob_vol = ticker_data.get('bearish_ob_volume_ok', False)
    
    be_ob_label = f"{'✅ Mitigated' if be_ob_validated else 'Fresh'} Bearish OB"
    be_ob_display = f"${be_ob_high:.2f} - ${be_ob_low:.2f}" if pd.notna(be_ob_low) else "N/A"
    be_ob_help = f"FVG: {'Yes' if be_ob_fvg else 'No'} | BOS Vol: {'High' if be_ob_vol else 'Low'}"
    zone_cols[1].metric(be_ob_label, be_ob_display, help=be_ob_help)
    
    # 3. Support (Using new SMC data)
    support = ticker_data.get('last_swing_low', np.nan)
    support_display = f"${support:.2f}" if pd.notna(support) else "N/A"
    zone_cols[2].metric("Last Swing Low", support_display)

    # 4. Resistance (Using new SMC data)
    resistance = ticker_data.get('last_swing_high', np.nan)
    resistance_display = f"${resistance:.2f}" if pd.notna(resistance) else "N/A"
    zone_cols[3].metric("Last Swing High", resistance_display)
    # --- ⭐️ END OF MODIFIED Section ⭐️ ---
    
    st.divider() 
    
    # --- ✅ MODIFIED SECTION ---
    st.subheader("Valuation & Key Dates")
    val_col1, val_col2, val_col3 = st.columns(3)
    
    val_col1.metric("Valuation (Graham)", ticker_data['grahamValuation'])
    
    next_earnings = ticker_data.get('next_earnings_date', 'N/A')
    val_col2.metric("Next Earnings Date", next_earnings)
    
    next_dividend = ticker_data.get('next_ex_dividend_date', 'N/A')
    val_col3.metric("Next Ex-Dividend Date", next_dividend, help="تاريخ الأحقية القادم لتوزيع الأرباح")
    # --- ✅ END MODIFIED SECTION ---
    
    # --- ✅ FIX: Convert to string to prevent Arrow error ---
    with st.expander("View All Raw Data for " + selected_ticker):
        st.dataframe(ticker_data.astype(str))
# --- ⭐️ END OF NEW HELPER FUNCTION ⭐️ ---


# --- ✅ NEW PORTFOLIO TAB FUNCTION ---
def display_portfolio_tab(all_data_df):
    
    st.header("💼 My Portfolio")
    
    # --- 1. File Save/Load ---
    st.subheader("Portfolio Management")
    file_col1, file_col2 = st.columns([1, 3])
    
    with file_col1:
        # Save Portfolio
        portfolio_json = json.dumps(st.session_state.portfolio, indent=4)
        st.download_button(
            label="💾 Save Portfolio",
            data=portfolio_json,
            file_name="my_portfolio.json",
            mime="application/json",
            use_container_width=True
        )

    with file_col2:
        # Load Portfolio
        uploaded_file = st.file_uploader("📂 Load Portfolio (JSON)", type="json")
        if uploaded_file is not None:
            try:
                new_portfolio = json.load(uploaded_file)
                # Basic validation
                if isinstance(new_portfolio, list) and all('ticker' in item for item in new_portfolio):
                    st.session_state.portfolio = new_portfolio
                    st.success(f"Successfully loaded {len(new_portfolio)} positions!")
                else:
                    st.error("Invalid portfolio file format.")
            except Exception as e:
                st.error(f"Error loading file: {e}")

    # --- 2. Add New Position ---
    with st.expander("Add New Position"):
        with st.form("add_position_form"):
            form_col1, form_col2, form_col3 = st.columns(3)
            
            new_ticker = form_col1.text_input("Ticker Symbol").upper().strip()
            new_shares = form_col2.number_input("Number of Shares", min_value=0.0, step=1.0)
            new_cost_basis = form_col3.number_input("Cost Basis (Price per Share)", min_value=0.01, format="%.2f")
            
            submitted = st.form_submit_button("Add Position")
            
            if submitted:
                if not new_ticker or new_shares == 0 or new_cost_basis == 0:
                    st.error("Please fill out all fields.")
                elif new_ticker not in all_data_df.index:
                    st.error(f"Ticker '{new_ticker}' not found in the analyzer's database. Run a 'Deep Dive' for it first from the sidebar.")
                else:
                    new_position = {
                        "ticker": new_ticker,
                        "shares": new_shares,
                        "cost_basis": new_cost_basis
                    }
                    st.session_state.portfolio.append(new_position)
                    st.success(f"Added {new_shares} shares of {new_ticker}!")
                    st.rerun()

    st.divider()

    # --- 3. Display P/L Dashboard ---
    st.subheader("Open Positions")
    
    if not st.session_state.portfolio:
        st.info("Your portfolio is empty. Add a new position above.")
        return # Stop here if no portfolio

    portfolio_data = []
    total_market_value = 0
    total_cost = 0

    # Ticker list for removal
    tickers_in_portfolio = [pos['ticker'] for pos in st.session_state.portfolio]

    for position in st.session_state.portfolio:
        ticker = position['ticker']
        if ticker not in all_data_df.index:
            st.warning(f"Data for {ticker} is missing. Please run analysis for this ticker.")
            continue
            
        ticker_data = all_data_df.loc[ticker]
        current_price = ticker_data['last_price']
        
        market_value = position['shares'] * current_price
        total_cost_basis = position['shares'] * position['cost_basis']
        
        pl_dollars = market_value - total_cost_basis
        pl_percent = (pl_dollars / total_cost_basis) * 100 if total_cost_basis != 0 else 0
        
        total_market_value += market_value
        total_cost += total_cost_basis
        
        portfolio_data.append({
            "Ticker": ticker,
            "Shares": position['shares'],
            "Cost Basis": position['cost_basis'],
            "Current Price": current_price,
            "Market Value": market_value,
            "Total Cost": total_cost_basis,
            "P/L ($)": pl_dollars,
            "P/L (%)": pl_percent
        })

    if not portfolio_data:
        st.error("Could not calculate P/L for portfolio. Ensure tickers are valid.")
        return

    # Display P/L Dataframe
    pl_df = pd.DataFrame(portfolio_data)
    st.dataframe(pl_df.set_index('Ticker'), use_container_width=True,
        column_config={
            "Cost Basis": st.column_config.NumberColumn(format="$%.2f"),
            "Current Price": st.column_config.NumberColumn(format="$%.2f"),
            "Market Value": st.column_config.NumberColumn(format="$%.2f"),
            "Total Cost": st.column_config.NumberColumn(format="$%.2f"),
            "P/L ($)": st.column_config.NumberColumn(format="$%.2f"),
            "P/L (%)": st.column_config.NumberColumn(format="%.2f%%"),
        }
    )
    
    # Display Totals
    total_pl = total_market_value - total_cost
    total_pl_pct = (total_pl / total_cost) * 100 if total_cost != 0 else 0
    
    total_col1, total_col2, total_col3 = st.columns(3)
    total_col1.metric("Total Market Value", f"${total_market_value:,.2f}")
    total_col2.metric("Total Cost Basis", f"${total_cost:,.2f}")
    total_col3.metric("Total P/L", f"${total_pl:,.2f}", delta=f"{total_pl_pct:.2f}%")

    # --- 4. Position Analyzer ---
    st.divider()
    st.subheader("Position Analyzer")
    
    selected_ticker = st.selectbox("Select a position to analyze:", options=tickers_in_portfolio)
    
    if selected_ticker:
        # Find the full position and ticker data
        position_data = next(p for p in st.session_state.portfolio if p['ticker'] == selected_ticker)
        ticker_data = all_data_df.loc[selected_ticker]
        
        display_position_analysis(position_data, ticker_data)
    
    if st.button("Clear Entire Portfolio"):
        st.session_state.portfolio = []
        st.rerun()

# --- ✅ NEW POSITION ANALYSIS HELPER ---
def display_position_analysis(position_data, ticker_data):
    
    pa_col1, pa_col2, pa_col3 = st.columns(3)
    
    with pa_col1:
        st.subheader("Where to Buy More? (Averaging)")
        cost_basis = position_data['cost_basis']
        b_ob_low = ticker_data.get('bullish_ob_low', np.nan)
        b_ob_high = ticker_data.get('bullish_ob_high', np.nan)
        
        st.metric("Your Cost Basis", f"${cost_basis:,.2f}")

        if pd.notna(b_ob_low):
            st.metric("Bullish OB (Demand Zone)", f"${b_ob_low:,.2f} - ${b_ob_high:,.2f}")
            if cost_basis > b_ob_high:
                st.info("The current Demand Zone is *below* your cost basis. This could be a good area to average down if the signal is confirmed.")
            else:
                st.success("Your cost basis is already at or below the current Demand Zone. This is a strong position.")
        else:
            st.warning("No clear Demand Zone (Bullish OB) found.")

    with pa_col2:
        st.subheader("Should I Buy More Now?")
        st.markdown("Check the 5-step confirmation checklist:")
        
        # Re-use the same checklist from the Deep Dive tab
        display_buy_signal_checklist(ticker_data)
        
        entry_signal = ticker_data.get('entry_signal', 'No Trade')
        if entry_signal == 'Buy near Bullish OB':
            st.success("✅ **Confirmation:** The SMC Entry Signal is active now.")
        else:
            st.warning("❌ **Wait:** The SMC Entry Signal is NOT active. Consider waiting for a pullback to the Demand Zone.")

    with pa_col3:
        st.subheader("Exit / P&L Analysis")
        
        stop_loss = ticker_data.get('Final Stop Loss', np.nan)
        be_ob_high = ticker_data.get('bearish_ob_high', np.nan)
        be_ob_low = ticker_data.get('bearish_ob_low', np.nan)

        if pd.notna(stop_loss):
            st.metric("Suggested Stop Loss", f"${stop_loss:,.2f}")
        else:
            st.metric("Suggested Stop Loss", "N/A")
            
        if pd.notna(be_ob_high):
            st.metric("Supply Zone (Profit Target)", f"${be_ob_low:,.2f} - ${be_ob_high:,.2f}")
        else:
            st.metric("Supply Zone (Profit Target)", "N/A")
            
        if pd.notna(stop_loss) and cost_basis < stop_loss:
            st.error("⚠️ **Warning:** Your cost basis is *below* the suggested stop loss. This is a high-risk position.")


# --- ⭐️ 6. Scheduler Entry Point ---

def run_analysis_for_scheduler():
    """
    Function to be called by an external scheduler (e.g., cron).
    Does NOT use Streamlit.
    """
    print("--- [SPUS SCHEDULER] ---")
    print(f"Starting scheduled analysis at {datetime.now(SAUDI_TZ)}...")
    
    def print_progress_callback(percent, text):
        print(f"[{percent*100:.0f}%] {text}")
    
    CONFIG = load_config('config.json')
    if CONFIG is None:
        print("FATAL: Could not load config.json. Exiting.")
        return
        
    log_file_path = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('LOG_FILE_PATH', 'spus_analysis.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    try:
        # --- ⭐️ MODIFIED: Calls global function ⭐️ ---
        # Note: This is a change, but generate_quant_report ALSO calls it.
        # This is fine.
        df, _, _ = generate_quant_report(CONFIG, print_progress_callback)
        if df is not None:
            print(f"Successfully generated report for {len(df)} tickers.")
        else:
            print("Analysis failed to produce data.")
            
    except Exception as e:
        logging.error(f"[SPUS SCHEDULER] Fatal error during scheduled run: {e}", exc_info=True)
        print(f"Error: Analysis failed. Check log file for details: {log_file_path}")

# --- ⭐️ 7. Main App Entry Point ---

if __name__ == "__main__":
    if "--run-scheduler" in sys.argv:
        run_analysis_for_scheduler()
    else:
        main()
