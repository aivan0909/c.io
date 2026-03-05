import pandas as pd
import numpy as np

def run_backtest(df):
    """
    Backtest the strategy on the provided dataframe.
    Assumes 'Strategy_Signal' column exists (1 for Buy, -1 for Sell, 0 for Hold).
    """
    initial_capital = 100000 # Not strictly needed if we just calculate % return per trade
    
    trades = []
    position = None # None, 'LONG'
    entry_price = 0
    entry_date = None
    
    # Ensure signals are available
    if 'Strategy_Signal' not in df.columns:
        return {"trades": [], "summary": "No strategy signal found"}

    # Identify trades
    # We iterate through the dataframe. 
    # If Signal == 1 and Position is None -> Buy
    # If Signal == -1 and Position == 'LONG' -> Sell
    
    for index, row in df.iterrows():
        signal = row['Strategy_Signal']
        price = row['Close']
        date = index.strftime('%Y-%m-%d')
        
        if signal == 1 and position is None:
            # Buy
            position = 'LONG'
            entry_price = price
            entry_date = date
            trades.append({
                "date": date,
                "price": price,
                "action": "BUY",
                "reason": row.get('Signal_Reason', 'Strategy Signal'),
                "return_pct": 0,
                "cumulative_return_pct": 0.0 # To be calculated
            })
            
        elif signal == -1 and position == 'LONG':
            # Sell
            position = None
            exit_price = price
            trade_return = ((exit_price - entry_price) / entry_price) * 100
            trades.append({
                "date": date,
                "price": price,
                "action": "SELL",
                "reason": row.get('Signal_Reason', 'Strategy Signal'),
                "return_pct": round(trade_return, 1),
                "cumulative_return_pct": 0.0 # To be calculated
            })

    # Calculate cumulative returns
    cumulative_return = 0
    df_trades = pd.DataFrame(trades)
    
    if not df_trades.empty:
        # Simple cumulative sum of returns for demonstration logic (could be compounded)
        # Requirement: cumulative gain/loss %
        # We will sum the trade returns for simplicity as "Cumulative Return"
        
        running_total = 0
        for i, row in df_trades.iterrows():
            if row['action'] == 'SELL':
                running_total += row['return_pct']
            df_trades.at[i, 'cumulative_return_pct'] = round(running_total, 1)
            
        trades = df_trades.to_dict('records')
        
    return {
        "trades": trades[::-1], # Reverse order for display (latest first)
        "total_return": round(sum(t['return_pct'] for t in trades if t['action'] == 'SELL'), 1)
    }
