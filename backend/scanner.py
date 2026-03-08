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

def scan_market(min_price=0, max_price=10000, strategy_filter="ALL"):
    """
    Scans the specific list of stocks using vectorized fast operations.
    """
    tickers = get_all_tw_stocks()
    data = get_bulk_data(tickers)
    results = []
    
    if data.empty:
        return []

    # Extract Close prices for all tickers simultaneously
    try:
        if isinstance(data.columns, pd.MultiIndex):
            # yfinance group_by='ticker' results in (Ticker, Price). So level=1 is 'Close'
            closes = data.xs('Close', axis=1, level=1)
        else:
            closes = data[['Close']]
    except Exception as e:
        print(f"Error extracting Close: {e}")
        return []

    # Fast Vectorized Computation
    closes = closes.ffill()
    sma_short = closes.rolling(window=5, min_periods=1).mean()
    sma_long = closes.rolling(window=20, min_periods=1).mean()

    delta = closes.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=13, adjust=False, min_periods=1).mean()
    ema_down = down.ewm(com=13, adjust=False, min_periods=1).mean()
    rs = ema_up / ema_down
    rsi = 100 - (100 / (1 + rs))

    ema12 = closes.ewm(span=12, adjust=False, min_periods=1).mean()
    ema26 = closes.ewm(span=26, adjust=False, min_periods=1).mean()
    macd_val = ema12 - ema26
    macd_signal = macd_val.ewm(span=9, adjust=False, min_periods=1).mean()

    # Get latest values across all 1800 tickers
    latest_close = closes.iloc[-1]
    latest_sma_short = sma_short.iloc[-1]
    latest_sma_long = sma_long.iloc[-1]
    latest_rsi = rsi.iloc[-1]
    latest_macd = macd_val.iloc[-1]
    latest_signal = macd_signal.iloc[-1]

    for ticker_formatted in latest_close.index:
        price = latest_close[ticker_formatted]
        if pd.isna(price) or price <= 0:
            continue
            
        price = float(price)
        
        # 1. Price Filter 
        if not (min_price <= price <= max_price):
            continue
            
        s_short = float(latest_sma_short[ticker_formatted])
        s_long = float(latest_sma_long[ticker_formatted])
        r = float(latest_rsi[ticker_formatted])
        m_val = float(latest_macd[ticker_formatted])
        m_sig = float(latest_signal[ticker_formatted])
        
        score = 0
        reasons = []
        
        if s_short > s_long:
            score += 1
            reasons.append("MA Bull")
        elif s_short < s_long:
            score -= 1
            reasons.append("MA Bear")
            
        if pd.notna(r):
            if r < 30:
                score += 1
                reasons.append("RSI Oversold")
            elif r > 70:
                score -= 1
                reasons.append("RSI Overbought")
                
        if pd.notna(m_val) and pd.notna(m_sig):
            if m_val > m_sig:
                score += 1
                reasons.append("MACD Bull")
            elif m_val < m_sig:
                score -= 1
                reasons.append("MACD Bear")
                
        final_signal = "HOLD"
        if score >= 2:
            final_signal = "BUY"
        elif score <= -2:
            final_signal = "SELL"
            
        if strategy_filter != "ALL" and strategy_filter != final_signal:
            continue
            
        ticker_base = str(ticker_formatted).split('.')[0]
        stock_name = ""
        if ticker_base in twstock.codes:
            stock_name = twstock.codes[ticker_base].name

        results.append({
            "stock_code": ticker_base,
            "stock_name": stock_name,
            "price": round(price, 2),
            "signal": final_signal,
            "reasons": ", ".join(reasons),
            "rsi": round(r, 2) if pd.notna(r) else 0,
            "ma_gap": round(s_short - s_long, 2) if pd.notna(s_short) and pd.notna(s_long) else 0
        })

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
