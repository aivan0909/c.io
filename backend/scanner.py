import yfinance as yf
import pandas as pd
import pandas_ta as ta
import twstock
from datetime import datetime, timedelta

# Top 10 Taiwan Stocks (Simpler list for connection test)
TOP_STOCKS = [
    "2330", "2317", "2454", "2308", "2303", "2881", "2882", "2891", "2002", "1301"
]

def get_bulk_data(tickers):
    # Formatted tickers
    formatted_tickers = [f"{t}.TW" for t in tickers]
    
    # Get history
    start_date = (datetime.now() - timedelta(days=200)).strftime('%Y-%m-%d')
    
    try:
        data = yf.download(formatted_tickers, start=start_date, progress=False, group_by='ticker')
        return data
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
    data = get_bulk_data(TOP_STOCKS)
    results = []
    
    if data.empty:
        return []
        
    # Iterate through tickers in the downloaded data
    # yfinance group_by='ticker' makes the top level columns the tickers
    available_tickers = data.columns.levels[0] if isinstance(data.columns, pd.MultiIndex) else TOP_STOCKS
    
    # If single ticker result (shouldn't happen with list but safety check)
    if len(TOP_STOCKS) == 1:
        # Logic for single
        pass 
        
    for ticker_code in TOP_STOCKS:
        try:
            ticker_formatted = f"{ticker_code}.TW"
            if ticker_formatted not in data.columns.levels[0]:
                continue
                
            df_ticker = data[ticker_formatted]
            
            # Simple check if data exists
            if df_ticker['Close'].dropna().empty:
                continue

            analysis = calculate_strategy_for_series(df_ticker)
            
            if analysis:
                price = analysis['price']
                signal = analysis['signal']
                
                # Filters
                if not (min_price <= price <= max_price):
                    continue
                    
                if strategy_filter != "ALL" and strategy_filter != signal:
                    continue
                    
                # Get stock name
                stock_name = ""
                if ticker_code in twstock.codes:
                    stock_name = twstock.codes[ticker_code].name

                results.append({
                    "stock_code": ticker_code,
                    "stock_name": stock_name,
                    **analysis
                })
                
        except Exception as e:
            continue
            
    # Sort by signal strength (Buy -> Hold -> Sell) or just Code
    # Let's sort by Code for now
    return sorted(results, key=lambda x: x['stock_code'])

def scan_volume_spikes(threshold: int = 400):
    """
    Scans the TOP_STOCKS for 1-minute volume spikes exceeding the threshold.
    """
    formatted_tickers = [f"{t}.TW" for t in TOP_STOCKS]
    try:
        data = yf.download(formatted_tickers, period="1d", interval="1m", progress=False, group_by='ticker')
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

    for ticker_code in TOP_STOCKS:
        try:
            ticker_formatted = f"{ticker_code}.TW"
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
                    stock_name = ""
                    if ticker_code in twstock.codes:
                        stock_name = twstock.codes[ticker_code].name
                        
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
                        "stock_code": ticker_code,
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
