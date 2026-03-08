import yfinance as yf
import pandas as pd
import pandas_ta as ta
import twstock
from datetime import datetime, timedelta
import concurrent.futures
import warnings
warnings.filterwarnings('ignore')

# Remove hardcoded list
# TOP_STOCKS = [...]

def get_all_tw_stocks():
    """Returns a list of all 4-digit TWSE and TPEX stock codes formatted for yfinance."""
    twse = [f"{c}.TW" for c, info in twstock.twse.items() if info.type == '股票' and len(c) == 4]
    tpex = [f"{c}.TWO" for c, info in twstock.tpex.items() if info.type == '股票' and len(c) == 4]
    return twse + tpex

def get_bulk_data(formatted_tickers):
    # Get history
    start_date = (datetime.now() - timedelta(days=200)).strftime('%Y-%m-%d')
    
    try:
        # Download in smaller batches if list is very large
        # Render free tier might choke on 1800+ tickers at once
        batch_size = 300 # Reduced batch size for stability
        all_data = []
        
        for i in range(0, len(formatted_tickers), batch_size):
            batch = formatted_tickers[i:i+batch_size]
            data = yf.download(batch, start=start_date, progress=False, group_by='ticker', threads=True)
            if not data.empty:
                all_data.append(data)
                
        if not all_data:
            return pd.DataFrame()
            
        return pd.concat(all_data, axis=1)
    except Exception as e:
        print(f"Error fetching bulk data: {e}")
        return pd.DataFrame()

def calculate_strategy_for_series(df):
    """
    Apply strategy logic for a single stock dataframe.
    Returns the latest status.
    """
    if df.empty or len(df) < 30: # Need enough data for indicators
        return None
        
    df = df.copy()
    
    # Indicators
    try:
        # Handle cases where columns might be multi-level or not
        close = df['Close']
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
            
        sma_short = ta.sma(close, length=5)
        sma_long = ta.sma(close, length=20)
        rsi = ta.rsi(close, length=14)
        macd_df = ta.macd(close)
        
        # Check if indicators are valid (not all NaN)
        if sma_short is None or macd_df is None:
            return None
            
        macd_val = macd_df.iloc[:, 0] # MACD
        macd_signal = macd_df.iloc[:, 2] # Signal
        
        # Latest values
        latest_close = close.iloc[-1]
        latest_sma_short = sma_short.iloc[-1]
        latest_sma_long = sma_long.iloc[-1]
        latest_rsi = rsi.iloc[-1]
        latest_macd = macd_val.iloc[-1]
        latest_signal = macd_signal.iloc[-1]
        
        # Strategy Logic
        score = 0
        reasons = []
        
        # MA
        if latest_sma_short > latest_sma_long:
            score += 1
            reasons.append("MA Bull")
        elif latest_sma_short < latest_sma_long:
            score -= 1
            reasons.append("MA Bear")
            
        # RSI
        if latest_rsi < 30:
            score += 1
            reasons.append("RSI Oversold")
        elif latest_rsi > 70:
            score -= 1
            reasons.append("RSI Overbought")
            
        # MACD
        if latest_macd > latest_signal:
            score += 1
            reasons.append("MACD Bull")
        elif latest_macd < latest_signal:
            score -= 1
            reasons.append("MACD Bear")
            
        final_signal = "HOLD"
        if score >= 2:
            final_signal = "BUY"
        elif score <= -2:
            final_signal = "SELL"
            
        return {
            "price": round(float(latest_close), 2),
            "signal": final_signal,
            "reasons": ", ".join(reasons),
            "rsi": round(float(latest_rsi), 2),
            "ma_gap": round(float(latest_sma_short - latest_sma_long), 2)
        }
            
    except Exception as e:
        # print(f"Calc error: {e}")
        return None

def scan_market(min_price=0, max_price=10000, strategy_filter="ALL"):
    """
    Scans the specific list of stocks.
    """
    tickers = get_all_tw_stocks()
    data = get_bulk_data(tickers)
    results = []
    
    if data.empty:
        return []
        
    tasks = []
    for ticker_formatted in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker_formatted not in data.columns.levels[0]:
                    continue
                df_ticker = data[ticker_formatted]
            else:
                df_ticker = data
            
            # Simple check if data exists
            if df_ticker['Close'].dropna().empty:
                continue
                
            tasks.append((ticker_formatted, df_ticker))
        except Exception as e:
            continue
            
    def process_ticker(args):
        ticker_formatted, df_ticker = args
        analysis = calculate_strategy_for_series(df_ticker)
        
        if analysis:
            price = analysis['price']
            signal = analysis['signal']
            
            # Filters
            if not (min_price <= price <= max_price):
                return None
                
            if strategy_filter != "ALL" and strategy_filter != signal:
                return None
                
            # Get stock name
            ticker_base = ticker_formatted.split('.')[0]
            stock_name = ""
            if ticker_base in twstock.codes:
                stock_name = twstock.codes[ticker_base].name

            return {
                "stock_code": ticker_base,
                "stock_name": stock_name,
                **analysis
            }
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for result in executor.map(process_ticker, tasks):
            if result:
                results.append(result)
            
    # Sort by signal strength (Buy -> Hold -> Sell) or just Code
    # Let's sort by Code for now
    return sorted(results, key=lambda x: x['stock_code'])

def scan_volume_spikes(threshold: int = 400):
    """
    Scans the market for 1-minute volume spikes exceeding the threshold.
    """
    tickers = get_all_tw_stocks()
    
    try:
        # For 1m data, batching is critical
        batch_size = 300
        all_data = []
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            data = yf.download(batch, period="1d", interval="1m", progress=False, group_by='ticker', threads=True)
            if not data.empty:
                all_data.append(data)
                
        if not all_data:
            return []
            
        data = pd.concat(all_data, axis=1)
    except Exception as e:
        print(f"Error fetching 1m data: {e}")
        return []

    results = []
    if data.empty:
        return results

    # Convert Timezone to Taipei Time to fix incorrect timestamps
    try:
        if data.index.tz is None:
            # Usually naive from yfinance is local time but might be interpreted wrongly, or it's UTC.
            # Yfinance for TW typically returns naive NY time or localized NY time.
            data.index = data.index.tz_localize('America/New_York').tz_convert('Asia/Taipei')
        else:
            data.index = data.index.tz_convert('Asia/Taipei')
    except Exception as e:
        print(f"Timezone conversion error: {e}")

    for ticker_formatted in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker_formatted not in data.columns.levels[0]:
                    continue
                df_ticker = data[ticker_formatted]
            else:
                df_ticker = data

            if 'Volume' not in df_ticker.columns or df_ticker['Volume'].dropna().empty:
                continue

            for timestamp, row in df_ticker.iterrows():
                vol = row['Volume']
                if pd.isna(vol) or vol == 0:
                    continue
                    
                if vol >= threshold:
                    ticker_base = ticker_formatted.split('.')[0]
                    stock_name = ""
                    if ticker_base in twstock.codes:
                        stock_name = twstock.codes[ticker_base].name
                        
                    # Estimate momentum (Taiwan convention: Red/Up=Buy, Green/Down=Sell)
                    try:
                        open_price = float(row['Open'])
                        close_price = float(row['Close'])
                        if close_price > open_price:
                            momentum = "BUY"
                        elif close_price < open_price:
                            momentum = "SELL"
                        else:
                            momentum = "NEUTRAL"
                    except:
                        momentum = "NEUTRAL"
                        
                    results.append({
                        "time": timestamp.strftime('%H:%M:%S'),
                        "stock_code": ticker_base,
                        "stock_name": stock_name,
                        "price": round(float(row['Close']), 2) if not pd.isna(row['Close']) else 0,
                        "volume": int(vol),
                        "momentum": momentum
                    })
        except Exception as e:
            # print(f"Error processing {ticker_code}: {e}")
            continue
            
    # Sort by time descending (newest first)
    return sorted(results, key=lambda x: x['time'], reverse=True)
