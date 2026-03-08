import yfinance as yf
import pandas as pd
import pandas_ta as ta
import twstock
from datetime import datetime, timedelta

def resolve_stock_code(input_str):
    """
    Resolve stock code from name if input is not numeric.
    """
    input_str = input_str.strip()
    if input_str.isdigit():
        return input_str
        
    # Create a reverse mapping for common names to codes
    # twstock.codes is {code: StockCodeInfo(...)}
    for code, info in twstock.codes.items():
        if info.name == input_str:
            return code
            
    return input_str

def get_stock_data(stock_code):
    # Resolve name to code if necessary
    stock_code = resolve_stock_code(stock_code)

    # Append .TW if not present (assuming TWSE)
    ticker = f"{stock_code}.TW" if not stock_code.endswith('.TW') else stock_code
    
    # Get 2 years of history + buffer for indicators
    start_date = (datetime.now() - timedelta(days=730 + 100)).strftime('%Y-%m-%d')
    df = yf.download(ticker, start=start_date, progress=False)
    
    if df.empty:
        # Try .TWO (TPEX) if .TW fails
        ticker = f"{stock_code}.TWO"
        df = yf.download(ticker, start=start_date, progress=False)
        
    if df.empty:
        raise ValueError(f"Stock data not found for '{stock_code}'")
        
    # Flat column names if multi-level (yfinance update)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
        
    return df, stock_code # Return resolved code as well

def analyze_stock(stock_code):
    try:
        df, resolved_code = get_stock_data(stock_code)
    except ValueError as e:
        return {"error": str(e)}

    # Calculate Indicators
    # 1. Moving Average (SMA 5 and SMA 20 for crossover strategy)
    df['SMA_Short'] = ta.sma(df['Close'], length=5)
    df['SMA_Long'] = ta.sma(df['Close'], length=20)
    
    # 2. RSI (14)
    df['RSI'] = ta.rsi(df['Close'], length=14)
    
    # 3. MACD
    macd = ta.macd(df['Close'])
    df = pd.concat([df, macd], axis=1)
    # MACD columns usually: MACD_12_26_9, MACDh_12_26_9 (hist), MACDs_12_26_9 (signal)
    macd_col = 'MACD_12_26_9'
    signal_col = 'MACDs_12_26_9'

    # Filter to last 2 years exactly for the result
    two_years_ago = datetime.now() - timedelta(days=730)
    df = df[df.index >= two_years_ago].copy()

    # Generate Signals
    df['Signal_MA'] = 0
    df['Signal_RSI'] = 0
    df['Signal_MACD'] = 0
    df['Strategy_Signal'] = 0 # 1 Buy, -1 Sell, 0 Hold
    df['Signal_Reason'] = ""

    # Logic Loop
    # We need to iterate to simulate 'real-time' signal generation effectively or use vectorized conditions
    # For simplicity and 'reason' generation, we'll iterate
    
    prev_row = None
    
    for index, row in df.iterrows():
        reasons = []
        score = 0
        
        # MA Crossover
        # Golden Cross: Short > Long (and was < previously) - Simplified: just current state
        if row['SMA_Short'] > row['SMA_Long']:
            score += 1
            reasons.append("MA Bullish")
        elif row['SMA_Short'] < row['SMA_Long']:
            score -= 1
            reasons.append("MA Bearish")
            
        # RSI
        if row['RSI'] < 30:
            score += 1
            reasons.append("RSI Oversold")
        elif row['RSI'] > 70:
            score -= 1
            reasons.append("RSI Overbought")
            
        # MACD
        if row[macd_col] > row[signal_col]:
             score += 1
             reasons.append("MACD Bullish")
        elif row[macd_col] < row[signal_col]:
             score -= 1
             reasons.append("MACD Bearish")
             
        # Comprehensive Strategy
        # If Score >= 2 -> Buy (1)
        # If Score <= -2 -> Sell (-1)
        
        final_signal = 0
        if score >= 2:
            final_signal = 1
        elif score <= -2:
            final_signal = -1
            
        df.at[index, 'Strategy_Signal'] = final_signal
        df.at[index, 'Signal_Reason'] = ", ".join(reasons)

    # Prepare visual data (latest)
    latest = df.iloc[-1]
    
    indicators = {
        "MA_Short": round(latest['SMA_Short'], 2),
        "MA_Long": round(latest['SMA_Long'], 2),
        "RSI": round(latest['RSI'], 2),
        "MACD": round(latest[macd_col], 2),
        "MACD_Signal": round(latest[signal_col], 2),
        "Close": round(latest['Close'], 2)
    }

    return {
        "stock_code": resolved_code,
        "current_price": indicators['Close'],
        "indicators": indicators,
        "data": df
    }
