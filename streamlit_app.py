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
import pytz # <-- IMPORT FOR TIMEZONE

# --- ⭐️ 1. Set Page Configuration FIRST ⭐️ ---
st.set_page_config(
    page_title="SPUS Quant Analyzer",
    page_icon="https://www.sp-funds.com/wp-content/uploads/2019/07/favicon-32x32.png", 
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
        # All other functions are now integrated or deprecated
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

# --- ⭐️ 2. Custom CSS (Unchanged from original) ⭐️ ---
def load_css():
    """Injects custom CSS for a modern, minimal, card-based theme."""
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
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
        [data-testid="stTabs"] button[role="tab"] {{
            border-radius: 8px 8px 0 0; padding: 10px 15px; font-weight: 500;
        }}
        [data-testid="stTabContent"] {{
            background-color: var(--secondary-background-color);
            border: 1px solid var(--gray-800); border-top: none;
            padding: 1.5rem; border-radius: 0 0 8px 8px;
        }}
        [data-testid="stVerticalBlock"]:nth-child(1) [data-testid="stButton"] button {{
            border: 1px solid var(--gray-800); font-weight: 500;
            text-align: left; padding: 0.5rem 0.75rem;
            transition: all 0.1s ease-in-out; border-radius: 8px;
            margin-bottom: 4px;
        }}
        [data-testid="stVerticalBlock"]:nth-child(1) [data-testid="stButton"] button[kind="secondary"]:hover {{
            border-color: var(--primary); color: var(--primary);
        }}
        [data-testid="stVerticalBlock"]:nth-child(1) [data-testid="stButton"] button[kind="primary"] {{
            border-color: #D30000; background-color: #D30000;
            color: white; font-weight: 600;
        }}
        [data-testid="stMetric"] {{
            background-color: var(--background-color);
            border: 1px solid var(--gray-800); border-radius: 8px;
            padding: 1rem 1.25rem;
        }}
    </style>
    """, unsafe_allow_html=True)


# --- ⭐️ 3. Core Analysis Logic (Modularized) ⭐️ ---

def calculate_robust_zscore_grouped(group_series):
    """Applies robust Z-score (MAD) to a pandas group."""
    series = pd.to_numeric(group_series, errors='coerce')
    median = series.median()
    mad = (series - median).abs().median()
    if mad == 0:
        # Fallback to standard Z-score if MAD is zero
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

    # Get sector counts and identify small sectors
    sector_counts = df_analysis['Sector'].value_counts()
    small_sectors = sector_counts[sector_counts < min_sector_size].index
    logging.info(f"Small sectors (<{min_sector_size} stocks) found: {list(small_sectors)}. Global medians will be used.")

    all_components = []
    for factor in factor_defs.keys():
        all_components.extend(factor_defs[factor]['components'])
    
    # 1. Pre-process all components (Winsorize, Handle Inversions, Calculate Ratios)
    for comp in all_components:
        col = comp['name']
        if col not in df_analysis.columns:
            logging.warning(f"Factor component '{col}' not found in data. Skipping.")
            continue
            
        # A. Winsorize/Clip outliers
        df_analysis[col] = pd.to_numeric(df_analysis[col], errors='coerce')
        # Use clip for robustness against extreme NaNs/Infs
        lower = df_analysis[col].quantile(win_limit)
        upper = df_analysis[col].quantile(1 - win_limit)
        if pd.notna(lower) and pd.notna(upper) and lower < upper:
            df_analysis[col] = df_analysis[col].clip(lower, upper)
        
        # B. Handle inversions (e.g., P/E -> E/P) or ratios
        # This implementation calculates Z-score on relative ratio (Stock/Sector)
        # which is simpler than inverting.
        
        # C. Calculate Sector/Global Medians
        global_median = df_analysis[col].median()
        if global_median == 0: global_median = 1e-6 # Avoid zero division
        
        sector_medians = df_analysis.groupby('Sector')[col].median()
        sector_medians.loc[small_sectors] = global_median # Replace small sectors
        sector_medians = sector_medians.fillna(global_median) # Fill any NaN sectors
        sector_medians[sector_medians == 0] = global_median # Avoid zero division
        
        df_analysis[f"{col}_Sector_Median"] = df_analysis['Sector'].map(sector_medians)
        
        # D. Calculate Relative Ratio (Stock / Sector Median)
        # We Z-score this ratio. A ratio > 1 is "better" if high_is_good=True
        df_analysis[f"{col}_Rel_Ratio"] = df_analysis[col] / df_analysis[f"{col}_Sector_Median"]
        
        # E. Calculate Z-Score for the relative ratio
        z_col_name = f"Z_{col}"
        df_analysis[z_col_name] = df_analysis.groupby('Sector')[f"{col}_Rel_Ratio"].transform(calculate_robust_zscore_grouped)
        
        # F. Adjust Z-Score based on 'high_is_good'
        if not comp['high_is_good']:
            df_analysis[z_col_name] = df_analysis[z_col_name] * -1.0
            
        df_analysis[z_col_name] = df_analysis[z_col_name].fillna(0) # Final fill

    # 2. Combine components into final factor Z-Scores
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
    
    # --- 2. Process Tickers Concurrently ---
    MAX_WORKERS = CONFIG.get('MAX_CONCURRENT_WORKERS', 10)
    report_progress(0.1, f"(2/7) Fetching data for {len(ticker_symbols)} tickers (Max Workers: {MAX_WORKERS})...")
    
    results_list = []
    all_histories = {}
    processed_count = 0
    total_tickers = len(ticker_symbols)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, ticker): ticker for ticker in ticker_symbols}
        
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result(timeout=60) # 60s timeout per ticker
                if result.get('success', False):
                    results_list.append(result)
                    if 'hist_df' in result:
                        all_histories[ticker] = result.pop('hist_df') # Store hist separately
                else:
                    logging.error(f"Failed to process {ticker}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                logging.error(f"Error processing {ticker} in main loop: {e}", exc_info=True)
            
            processed_count += 1
            percent_done = 0.1 + (0.6 * (processed_count / total_tickers))
            report_progress(percent_done, f"(2/7) Processing: {ticker} ({processed_count}/{total_tickers})")

    end_time = time.time()
    report_progress(0.7, f"(3/7) Data fetch complete. Time taken: {end_time - start_time:.2f}s")

    if not results_list:
        report_progress(1.0, "Error: No data successfully processed. Analysis cancelled.")
        return None, None, None
        
    results_df = pd.DataFrame(results_list)
    results_df.set_index('ticker', inplace=True)
    
    # --- 3. Risk Management Calcs ---
    # *** THIS SECTION IS NOW REMOVED, AS IT'S DONE IN SPUS.PY ***
    report_progress(0.75, "(4/7) Risk metrics calculated in spus.py.")
    
    # --- 4. Factor Z-Score Calculation ---
    report_progress(0.8, "(5/7) Calculating robust Z-Scores...")
    results_df = calculate_all_z_scores(results_df, CONFIG)
    
    # --- 5. Save Reports (Excel, PDF, CSV) ---
    report_progress(0.9, "(6/7) Generating reports...")
    
    # Note: Final Quant Score is NOT calculated here. It's done dynamically in the UI.
    # We sort by a default factor (e.g., Value) for the static reports.
    results_df.sort_values(by='Z_Value', ascending=False, inplace=True)

    # Column name mapping for display
    results_df_display = results_df.rename(columns={
        'last_price': 'Last Price', 'Sector': 'Sector', 'marketCap': 'Market Cap',
        'forwardPE': 'Forward P/E', 'priceToBook': 'P/B Ratio', 'grahamValuation': 'Valuation (Graham)',
        'momentum_12m': 'Momentum (12M %)', 'volatility_1y': 'Volatility (1Y)',
        'returnOnEquity': 'ROE (%)', 'debtToEquity': 'Debt/Equity', 'profitMargins': 'Profit Margin (%)',
        'beta': 'Beta', 'RSI': 'RSI (14)', 'ADX': 'ADX (14)',
        'Stop Loss Price': 'Stop Loss', 'Take Profit Price': 'Take Profit'
    })
    
    # Format percentages for display
    pct_cols = ['ROE (%)', 'Profit Margin (%)', 'Momentum (12M %)', 'Risk % (to Stop)']
    for col in pct_cols:
        if col in results_df_display.columns:
            results_df_display[col] = results_df_display[col] * 100

    # Create data sheets for Excel/PDF
    data_sheets = {
        'Top 20 (By Value)': results_df_display.sort_values(by='Z_Value', ascending=False).head(20),
        'Top 20 (By Momentum)': results_df_display.sort_values(by='Z_Momentum', ascending=False).head(20),
        'Top 20 (By Quality)': results_df_display.sort_values(by='Z_Quality', ascending=False).head(20),
        'Top Bullish Technicals': results_df_display.sort_values(by='Z_Technical', ascending=False).head(20),
        'Top Undervalued (Graham)': results_df_display[results_df_display['Valuation (Graham)'] == 'Undervalued (Graham)'].sort_values(by='Z_Value', ascending=False).head(20),
        'All Results (Raw)': results_df # Full raw data
    }

    # Save Excel
    excel_file_path = os.path.join(BASE_DIR, CONFIG['LOGGING']['EXCEL_FILE_PATH'])
    try:
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            for sheet_name, df_sheet in data_sheets.items():
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=True)
        report_progress(0.92, f"Excel report saved: {excel_file_path}")
    except Exception as e:
        logging.error(f"Failed to save Excel file: {e}")

    # Save PDF
    if REPORTLAB_AVAILABLE:
        try:
            # *** USE SAUDI TIMEZONE ***
            timestamp = datetime.now(SAUDI_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            base_pdf_path = os.path.splitext(excel_file_path)[0]
            pdf_file_path = f"{base_pdf_path}_{datetime.now(SAUDI_TZ).strftime('%Y%m%d_%H%M%S')}.pdf"
            
            doc = SimpleDocTemplate(pdf_file_path, pagesize=landscape(letter))
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(name='Left', alignment=TA_LEFT))
            
            elements = [Paragraph(f"SPUS Quant Report - {timestamp}", styles['h1'])]
            
            # *** PDF TABLE FIT FIX: Select fewer columns ***
            pdf_cols = ['Last Price', 'Z_Value', 'Z_Momentum', 'Z_Quality', 'Risk/Reward Ratio']
            
            for sheet_name, df_sheet in data_sheets.items():
                if sheet_name == 'All Results (Raw)': continue # Skip full report
                
                elements.append(Paragraph(sheet_name, styles['h2']))
                
                # Ensure selected columns exist
                cols_to_show = [col for col in pdf_cols if col in df_sheet.columns]
                df_pdf = df_sheet.head(15).reset_index()[['ticker'] + cols_to_show]
                
                # Format for PDF
                df_pdf = df_pdf.fillna('N/A')
                for col in cols_to_show:
                    if col in df_pdf.select_dtypes(include=[np.number]).columns:
                        df_pdf[col] = df_pdf[col].round(2)
                
                data = [df_pdf.columns.tolist()] + df_pdf.values.tolist()
                
                # Adjust column widths
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
    
    # Save timestamped CSV result
    try:
        results_dir = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('RESULTS_DIR', 'results_history'))
        os.makedirs(results_dir, exist_ok=True)
        # *** USE SAUDI TIMEZONE ***
        timestamp_csv = datetime.now(SAUDI_TZ).strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(results_dir, f"quant_results_{timestamp_csv}.csv")
        results_df.to_csv(csv_path)
        report_progress(0.98, f"Timestamped CSV saved: {csv_path}")
    except Exception as e:
        logging.error(f"Failed to save timestamped CSV: {e}")

    report_progress(1.0, "Analysis complete.")
    
    # Return the raw df, histories, and display sheets
    return results_df, all_histories, data_sheets

# --- ⭐️ 4. Streamlit UI Functions ⭐️ ---

@st.cache_data(show_spinner=False, ttl=3600) # Cache for 1 hour
def load_analysis_data(_config, run_timestamp):
    """
    Streamlit cache wrapper for the core analysis function.
    The run_timestamp parameter is used to bust the cache when
    the user clicks "Run Analysis".
    """
    
    # This context allows the core function to write to the Streamlit UI
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
        
    # *** USE SAUDI TIMEZONE ***
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

def create_price_chart(hist_df, ticker):
    """Creates an interactive Plotly Price Chart with SMAs and MACD."""
    
    cfg = CONFIG['TECHNICALS']
    short_ma_col = f'SMA_{cfg["SHORT_MA_WINDOW"]}'
    long_ma_col = f'SMA_{cfg["LONG_MA_WINDOW"]}'
    macd_col = f'MACD_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
    macd_h_col = f'MACDh_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'
    macd_s_col = f'MACDs_{cfg["MACD_SHORT_SPAN"]}_{cfg["MACD_LONG_SPAN"]}_{cfg["MACD_SIGNAL_SPAN"]}'

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, subplot_titles=(f'{ticker} Price', 'MACD'), 
                        row_width=[0.2, 0.7])

    # Price Chart (Row 1)
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

    # MACD Chart (Row 2)
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
                range=[min(-2, min(values)-0.5), max(2, max(values)+0.5)] # Dynamic range
            )
        ),
        title=f"Factor Profile for {ticker_data.name}",
        height=400
    )
    return fig

# --- ⭐️ NEW CODE START ⭐️ ---
def display_buy_signal_checklist(ticker_data):
    """
    Displays a 4-step checklist on the Ticker Deep Dive tab,
    showing which buy criteria are met.
    """
    
    # --- Define Thresholds (based on the guide's examples) ---
    SCORE_THRESHOLD = 1.0
    FACTOR_Z_THRESHOLD = 0.5 # "Good" factor (guide says "high", we'll use > 0.5)
    RSI_OVERBOUGHT = 70.0
    RR_RATIO_THRESHOLD = 1.5

    # --- Check Each Step ---
    
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

    # Step 3: Technicals
    step3_met = False
    step3_text = "**3. Favorable Technicals**"
    
    trend = ticker_data.get('Trend (50/200 Day MA)', 'N/A')
    rsi = ticker_data.get('RSI', 50)
    macd_sig = ticker_data.get('MACD_Signal', 'N/A')

    trend_ok = (trend == 'Confirmed Uptrend')
    rsi_ok = (pd.notna(rsi) and rsi < RSI_OVERBOUGHT)
    macd_ok = (macd_sig in ['Bullish', 'Bullish Crossover'])
    
    if trend_ok and rsi_ok and macd_ok:
        step3_met = True
    
    # Build details with emojis
    trend_icon = "✅" if trend_ok else "❌"
    rsi_icon = "✅" if rsi_ok else "❌"
    macd_icon = "✅" if macd_ok else "❌"
    
    step3_details = (
        f"{trend_icon} Trend: {trend}<br>"
        f"{rsi_icon} RSI: {rsi:.1f} (Not Overbought)<br>"
        f"{macd_icon} MACD: {macd_sig}"
    )

    # Step 4: Risk/Reward
    step4_met = False
    step4_text = f"**4. R/R Ratio > {RR_RATIO_THRESHOLD}**"
    rr_ratio = ticker_data.get('Risk/Reward Ratio', 0)
    if pd.notna(rr_ratio) and rr_ratio > RR_RATIO_THRESHOLD:
        step4_met = True
    step4_details = f"Ratio is {rr_ratio:.2f}"
    
    # --- Display in Columns ---
    st.subheader("Buy Signal Checklist")
    cols = st.columns(4)
    
    criteria = [
        (step1_met, step1_text, step1_details),
        (step2_met, step2_text, step2_details),
        (step3_met, step3_text, step3_details),
        (step4_met, step_4_text, step4_details)
    ]
    
    for i, (met, text, details) in enumerate(criteria):
        with cols[i]:
            icon = "✅" if met else "❌"
            # Use st.metric for a "card" look
            st.markdown(f"**{icon} {text}**")
            st.markdown(details, unsafe_allow_html=True) # For the <br> in step 3
# --- ⭐️ NEW CODE END ⭐️ ---


# --- ⭐️ 5. Main Streamlit Application ⭐️ ---

def main():
    
    # --- Initialize Session State ---
    if 'selected_ticker' not in st.session_state:
        st.session_state.selected_ticker = None
    if 'run_timestamp' not in st.session_state:
        # This key triggers the first run
        st.session_state.run_timestamp = time.time() 
    
    # --- Load CSS ---
    load_css()
    
    # --- Load Config ---
    global CONFIG # Make CONFIG globally available in main()
    CONFIG = load_config('config.json')
    if CONFIG is None:
        st.error("FATAL: config.json not found. App cannot start.")
        st.stop()

    # --- Setup Logging ---
    log_file_path = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('LOG_FILE_PATH', 'spus_analysis.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            logging.StreamHandler() # Also log to console
        ]
    )

    # --- Sidebar ---
    with st.sidebar:
        st.image("https://www.sp-funds.com/wp-content/uploads/2022/02/SP-Funds-Logo-Primary-Wht-1.svg", width=200)
        st.title("SPUS Quant Analyzer")
        st.markdown("Research-Grade Multi-Factor Analysis")
        st.divider()

        st.subheader("Controls")
        if st.button("🔄 Run Full Analysis", type="primary"):
            st.session_state.selected_ticker = None
            # Update timestamp to bust the cache
            st.session_state.run_timestamp = time.time() 
            st.rerun()
        
        default_weights = CONFIG.get('DEFAULT_FACTOR_WEIGHTS', {
            "Value": 0.20, "Momentum": 0.20, "Quality": 0.20,
            "Size": 0.10, "LowVolatility": 0.15, "Technical": 0.15
        })
        
        # --- ⭐️⭐️ CODE MODIFIED START ⭐️⭐️ ---
        # Define the callback function first
        def callback_reset_weights():
            """
            This function is called when the reset button is clicked.
            It deletes all factor weight keys from session state.
            """
            for factor in default_weights.keys():
                key_to_del = f"weight_{factor}" 
                if key_to_del in st.session_state:
                    del st.session_state[key_to_del]

        # Use the on_click callback in the button
        st.button("Reset Factor Weights", on_click=callback_reset_weights)
        
        # REMOVED the old `if st.button(...)` logic
        # --- ⭐️⭐️ CODE MODIFIED END ⭐️⭐️ ---

        # --- 7. UI: Factor Weight Sliders ---
        st.subheader("Factor Weights")
        st.info("Adjust weights to re-rank stocks. Weights will be normalized.")
        
        weights = {}
        for factor, default in default_weights.items():
            # *** ADDED KEY TO SLIDERS FOR RESET TO WORK ***
            weights[factor] = st.slider(factor, 0.0, 1.0, default, 0.05, key=f"weight_{factor}")
            
        # Normalize weights
        total_weight = sum(weights.values())
        norm_weights = {f: (w / total_weight) if total_weight > 0 else 0 for f, w in weights.items()}
        
        with st.expander("Normalized Weights"):
            for factor, weight in norm_weights.items():
                st.write(f"{factor}: {weight*100:.1f}%")
        
        st.divider()

        # --- Download Buttons ---
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

        # --- ⭐️ NEW CODE START ⭐️ ---
        # Add the author logo at the bottom of the sidebar
        st.divider()
        try:
            st.image("logo.jpg", width=120, use_column_width="auto")
        except Exception as e:
            st.warning(f"Could not load logo.jpg: {e}")
        # --- ⭐️ NEW CODE END ⭐️ ---


    # --- Main Page ---
    st.title("SPUS Quantitative Dashboard")
    
    # --- Load Data (from cache or new run) ---
    raw_df, all_histories, data_sheets, last_run_time = load_analysis_data(CONFIG, st.session_state.run_timestamp)
    
    if raw_df is None:
        st.error("Analysis failed to produce data.")
        st.stop()
        
    # *** USE SAUDI TIMEZONE ***
    st.success(f"Data loaded from analysis run at: {datetime.fromtimestamp(last_run_time, SAUDI_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # --- 7. UI: Dynamic Score Calculation & Filtering ---
    
    # 1. Calculate Final Quant Score dynamically
    df = raw_df.copy()
    
    # *** Handle case where data loading might have failed and df is empty ***
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

    # 2. Apply Filters
    st.subheader("Filters")
    filt_col1, filt_col2 = st.columns(2)
    
    # Sector Filter
    all_sectors = sorted(df['Sector'].unique())
    selected_sectors = filt_col1.multiselect("Filter by Sector:", all_sectors, default=all_sectors)
    
    # *** ROBUST SLIDER LOGIC TO PREVENT CRASH ***
    # Market Cap Filter
    if df.empty or 'marketCap' not in df.columns or df['marketCap'].isnull().all():
        filt_col2.info("No Market Cap data to filter.")
        cap_range = (0.0, 0.0) # Dummy value
    else:
        min_cap_val = float(df['marketCap'].min())
        max_cap_val = float(df['marketCap'].max())

        if min_cap_val == max_cap_val:
            # Only one stock, or all have same cap. Create a small range.
            min_cap = (min_cap_val / 1e9) * 0.9 # 10% below
            max_cap = (max_cap_val / 1e9) * 1.1 # 10% above
            if min_cap < 0: min_cap = 0.0
        else:
            min_cap = min_cap_val / 1e9
            max_cap = max_cap_val / 1e9
        
        # Ensure min is still less than max after calculations
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
    
    # *** ROBUST FILTERING LOGIC ***
    # Apply filters
    filtered_df = df[
        (df['Sector'].isin(selected_sectors))
    ].copy()

    # Conditionally apply market cap filter
    if not filtered_df.empty and 'marketCap' in filtered_df.columns and cap_range != (0.0, 0.0):
         filtered_df = filtered_df[
            (filtered_df['marketCap'].ge(cap_range[0] * 1e9)) &
            (filtered_df['marketCap'].le(cap_range[1] * 1e9))
         ]
    
    # 3. Sort by new dynamic score
    filtered_df.sort_values(by='Final Quant Score', ascending=False, inplace=True)
    
    st.markdown(f"Displaying **{len(filtered_df)}** of **{len(df)}** total stocks matching filters.")
    st.divider()

    # --- ⭐️ 6. UI: New Tab Structure ⭐️ ---
    
    tab_list = ["🏆 Quant Rankings", "🔬 Ticker Deep Dive", "📈 Portfolio Analytics"]
    tab_rank, tab_deep, tab_port = st.tabs(tab_list)
    
    # --- Tab 1: Quant Rankings ---
    with tab_rank:
        st.header("🏆 Top Stocks by Final Quant Score")
        st.info("Click a ticker to select it for the 'Ticker Deep Dive' tab.")
        
        # --- ⭐️ NEW CODE START ⭐️ ---
        with st.expander("How to Find a Good Buy Signal (4-Step Guide)", expanded=False):
            st.markdown("""
                This 4-step method helps you use the app to find suitable buying opportunities.
                
                ### 1. Check the Final Quant Score (The "What")
                This is your primary signal. Look for stocks with a **high positive score** (e.g., > 1.0) 
                in the ranked list below. This suggests the stock is "better than average" 
                across the factors you selected.
                
                ### 2. Check the Factor Profile (The "Why")
                Click a stock and go to the **"🔬 Ticker Deep Dive"** tab. Look at the 
                **"Factor Profile"** radar chart. This tells you *why* the score is high. 
                Is it high on `Value` (it's cheap) and `Quality` (it's a good company)? 
                This helps you buy stocks that match your strategy.
                
                ### 3. Check the Technicals (The "When")
                On the **"Deep Dive"** tab, look at the **"Price Chart"** and metrics.
                * **Trend (50/200 Day MA):** Is the trend "Confirmed Uptrend"?
                * **RSI/MACD:** Are the technical signals (`RSI`, `MACD_Signal`) favorable 
                    (e.g., not "overbought" or "bearish")?
                
                ### 4. Check the Risk & Sizing (The "How")
                In the **"Risk & Position Sizing"** section, check the:
                * **Risk/Reward Ratio:** Is it favorable (e.g., > 1.5)?
                * **Stop Loss Price:** Is this exit price acceptable to you?
                * **Position Size (USD):** This calculates how much to invest for 
                    your pre-defined risk amount (e.g., $50).
            """)
        # --- ⭐️ NEW CODE END ⭐️ ---
        
        # --- Column layout (List + Details) ---
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
                        
                        if st.button(label, key=f"rank_{ticker}", use_container_width=True, type=button_type):
                            st.session_state.selected_ticker = ticker
                            st.success(f"Selected {ticker}. See 'Ticker Deep Dive' tab.")
        
        with rank_col2:
            st.subheader("Top 20 Overview")
            
            # Display columns for the table
            display_cols = [
                'Last Price', 'Sector', 'Market Cap', 
                'Final Quant Score', 
                'Z_Value', 'Z_Momentum', 'Z_Quality', 
                'Z_Size', 'Z_LowVolatility', 'Z_Technical',
                'Risk/Reward Ratio',
                'Position Size (USD)' # Added new col
            ]
            # Ensure columns exist
            display_cols = [c for c in display_cols if c in filtered_df.columns]
            
            # Re-format Market Cap for display
            filtered_df_display = filtered_df.copy()
            if 'marketCap' in filtered_df_display.columns:
                filtered_df_display['Market Cap'] = filtered_df_display['marketCap'] / 1e9
            
            st.dataframe(
                filtered_df_display.head(20)[display_cols],
                column_config={
                    "Last Price": st.column_config.NumberColumn(format="$%.2f"),
                    "Market Cap": st.column_config.NumberColumn(format="%.1f B", help="Market Cap in Billions"),
                    "Final Quant Score": st.column_config.NumberColumn(format="%.3f"),
                    "Z_Value": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Momentum": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Quality": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Size": st.column_config.NumberColumn(format="%.2f"),
                    "Z_LowVolatility": st.column_config.NumberColumn(format="%.2f"),
                    "Z_Technical": st.column_config.NumberColumn(format="%.2f"),
                    "Risk/Reward Ratio": st.column_config.NumberColumn(format="%.2f"),
                    "Position Size (USD)": st.column_config.NumberColumn(format="$%,.0f"),
                },
                use_container_width=True,
                height=700
            )

    # --- Tab 2: Ticker Deep Dive ---
    with tab_deep:
        st.header("🔬 Ticker Deep Dive")
        
        selected_ticker = st.session_state.selected_ticker
        
        if selected_ticker is None:
            st.info("Go to the 'Quant Rankings' tab and click a ticker to see details.")
        elif filtered_df.empty:
            st.info("Go to the 'Quant Rankings' tab and click a ticker to see details.")
        elif selected_ticker not in filtered_df.index:
            st.warning(f"Ticker '{selected_ticker}' is not in the currently filtered list. Clear filters or select a new ticker.")
        else:
            ticker_data = filtered_df.loc[selected_ticker]
            hist_data = all_histories.get(selected_ticker)
            
            st.subheader(f"Analysis for: {selected_ticker}")

            # --- ⭐️ NEW CODE START ⭐️ ---
            # Add Previous/Next Buttons
            try:
                # Get the full list of tickers from the *filtered* dataframe
                ticker_list = filtered_df.index.tolist()
                current_index = ticker_list.index(selected_ticker)
                
                # Create columns for the buttons
                prev_col, next_col, _ = st.columns([1, 1, 4]) # Keep buttons compact
                
                # Previous Button
                is_first = (current_index == 0)
                if prev_col.button("⬅️ Previous", use_container_width=True, disabled=is_first, key="prev_ticker"):
                    st.session_state.selected_ticker = ticker_list[current_index - 1]
                    st.rerun()
                    
                # Next Button
                is_last = (current_index == len(ticker_list) - 1)
                if next_col.button("Next ➡️", use_container_width=True, disabled=is_last, key="next_ticker"):
                    st.session_state.selected_ticker = ticker_list[current_index + 1]
                    st.rerun()

            except ValueError:
                # This might happen if the ticker is somehow not in the list, though the parent 'if' should prevent it.
                st.error("Error finding ticker in the list for navigation.")
            # --- ⭐️ NEW CODE END ⭐️ ---
            
            # --- ⭐️⭐️ NEW CHECKLIST SECTION START ⭐️⭐️ ---
            # Call the new function here
            display_buy_signal_checklist(ticker_data)
            st.divider() # Add a divider after the checklist
            # --- ⭐️⭐️ NEW CHECKLIST SECTION END ⭐️⭐️ ---

            # *** ADD THIS WARNING DISPLAY ***
            if pd.notna(ticker_data.get('data_warning')):
                st.warning(f"⚠️ **Data Warning:** {ticker_data['data_warning']}")
            # *** END OF NEW DISPLAY ***
            
            st.markdown(f"**Sector:** {ticker_data['Sector']} | **Data Source:** `{ticker_data['source']}`")
            
            # --- Key Metrics ---
            kpi_cols = st.columns(4)
            kpi_cols[0].metric("Final Quant Score", f"{ticker_data['Final Quant Score']:.3f}")
            kpi_cols[1].metric("Last Price", f"${ticker_data['last_price']:.2f}")
            kpi_cols[2].metric("Market Cap", f"${ticker_data['marketCap']/1e9:.1f} B")
            kpi_cols[3].metric("Trend (50/200 MA)", ticker_data['Trend (50/200 Day MA)'])
            
            st.divider()
            
            # --- 6. Explainability & 7. UI Charts ---
            chart_col1, chart_col2 = st.columns([2, 1])
            
            with chart_col1:
                st.subheader("Price Chart & Technicals")
                if hist_data is not None:
                    price_chart = create_price_chart(hist_data, selected_ticker)
                    st.plotly_chart(price_chart, use_container_width=True)
                else:
                    st.error("Historical data not found for this ticker.")
                    
            with chart_col2:
                st.subheader("Factor Profile")
                radar_chart = create_radar_chart(ticker_data, factor_z_cols)
                st.plotly_chart(radar_chart, use_container_width=True)
                
                with st.expander("Factor Contribution Breakdown", expanded=True):
                    for factor in norm_weights.keys():
                        z_col = f"Z_{factor}"
                        w_z_col = f"Weighted_{z_col}"
                        st.metric(
                            label=f"{factor} (Z-Score: {ticker_data[z_col]:.2f})",
                            value=f"Contrib: {ticker_data[w_z_col]:.3f}",
                            help=f"Weight: {norm_weights[factor]*100:.1f}%"
                        )

            st.divider()
            
            # --- Risk, Value & Position Sizing Metrics ---
            st.subheader("Risk & Position Sizing")
            risk_val_cols = st.columns(5) # Changed to 5 columns
            
            # 1. Stop Loss
            sl_price = ticker_data['Stop Loss Price']
            risk_pct = ticker_data['Risk % (to Stop)']
            sl_display = f"${sl_price:.2f}" if pd.notna(sl_price) else "N/A"
            risk_display = f"Risk %: {risk_pct:.1f}%" if pd.notna(risk_pct) else "N/A"
            sl_method = ticker_data.get('SL_Method', 'N/A')
            risk_val_cols[0].metric(f"Stop Loss ({sl_method})", sl_display, help=risk_display)

            # 2. Take Profit (Fibonacci)
            tp_price = ticker_data['Take Profit Price']
            tp_display = f"${tp_price:.2f}" if pd.notna(tp_price) else "N/A"
            risk_val_cols[1].metric("Take Profit (Fib 1.618)", tp_display)

            # 3. Risk/Reward
            rr_ratio = ticker_data['Risk/Reward Ratio']
            rr_display = f"{rr_ratio:.2f}" if pd.notna(rr_ratio) else "N/A"
            risk_val_cols[2].metric("Risk/Reward Ratio", rr_display)

            # 4. Position Size (Shares)
            pos_shares = ticker_data['Position Size (Shares)']
            pos_display = f"{pos_shares:.0f} Shares" if pd.notna(pos_shares) else "N/A"
            risk_usd = ticker_data.get('Risk Per Trade (USD)', 500)
            risk_val_cols[3].metric("Position Size (Shares)", pos_display, help=f"Based on ${risk_usd:,.0f} risk")
            
            # 5. Position Size (USD)
            pos_usd = ticker_data['Position Size (USD)']
            pos_usd_display = f"${pos_usd:,.0f}" if pd.notna(pos_usd) else "N/A"
            risk_val_cols[4].metric("Position Size (USD)", pos_usd_display, help="Shares * Last Price")

            st.divider() # Add a divider before the next section
            
            # --- Graham Value Metric (moved to a new subheader) ---
            st.subheader("Valuation")
            val_col1, _, _ = st.columns(3) # Use columns to keep it compact
            val_col1.metric("Valuation (Graham)", ticker_data['grahamValuation'])
            
            # --- Raw Data Expander ---
            with st.expander("View All Raw Data for " + selected_ticker):
                st.dataframe(ticker_data)

    # --- Tab 3: Portfolio Analytics ---
    with tab_port:
        st.header("📈 Portfolio-Level Analytics")
        
        # Check if df is empty before grouping
        if filtered_df.empty:
            st.warning("No data to display. Adjust filters.")
        else:
            port_col1, port_col2 = st.columns(2)
            
            with port_col1:
                # --- 4. Statistical Robustness: Correlation Heatmap ---
                st.subheader("Factor Correlation Heatmap")
                st.info("This shows if factors are redundant (highly correlated). Aim for low values.")
                
                corr_matrix = filtered_df[factor_z_cols].corr()
                corr_heatmap = px.imshow(
                    corr_matrix,
                    text_auto=".2f",
                    aspect="auto",
                    color_continuous_scale='RdBu_r', # Red-Blue
                    zmin=-1, zmax=1,
                    title="Factor Z-Score Correlation Matrix"
                )
                st.plotly_chart(corr_heatmap, use_container_width=True)
                
            with port_col2:
                # --- 6. Explainability: Sector Heatmap ---
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
            

# --- ⭐️ 6. Scheduler Entry Point ---

def run_analysis_for_scheduler():
    """
    Function to be called by an external scheduler (e.g., cron).
    Does NOT use Streamlit.
    """
    print("--- [SPUS SCHEDULER] ---")
    # *** USE SAUDI TIMEZONE ---
    print(f"Starting scheduled analysis at {datetime.now(SAUDI_TZ)}...")
    
    # Setup basic print logging for the scheduler
    def print_progress_callback(percent, text):
        print(f"[{percent*100:.0f}%] {text}")
    
    CONFIG = load_config('config.json')
    if CONFIG is None:
        print("FATAL: Could not load config.json. Exiting.")
        return
        
    # Setup file logging for the scheduled run
    log_file_path = os.path.join(BASE_DIR, CONFIG.get('LOGGING', {}).get('LOG_FILE_PATH', 'spus_analysis.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='a'),
            logging.StreamHandler(sys.stdout) # Log to stdout for cron
        ]
    )
    
    try:
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
