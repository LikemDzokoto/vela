import os
from binance.client import Client
from dotenv import load_dotenv

import pandas as pd



load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')


client = Client(API_KEY, API_SECRET)

def get_small_cap_coins(market_cap_threshold=100_000_000):
    """
    Fetch recently listed coins with market cap under the specified threshold.
    """
    # Get all tickers
    tickers = client.get_all_tickers()
    
    # Filter for small-cap coins (market cap under $100M)
    small_cap_coins = []
    for ticker in tickers:
        symbol = ticker['symbol']
        if symbol.endswith('USDT'):  # Focus on USDT pairs
            try:
                # Get 24hr ticker data for market cap calculation
                ticker_24hr = client.get_ticker(symbol=symbol)
                volume = float(ticker_24hr['quoteVolume'])
                price = float(ticker_24hr['lastPrice'])
                market_cap = volume * price
                
                if market_cap < market_cap_threshold:
                    small_cap_coins.append(symbol)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
    
    return small_cap_coins

def fetch_price_volume_data(symbol, interval='5m', limit=60):
    """
    Fetch OHLC and volume data for a given symbol.
    """
    try:
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        return [{
            'timestamp': kline[0],
            'open': float(kline[1]),
            'high': float(kline[2]),
            'low': float(kline[3]),
            'close': float(kline[4]),
            'volume': float(kline[5])
        } for kline in klines]
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

if __name__ == "__main__":
  
    small_cap_coins = get_small_cap_coins()
    print(f"Small-cap coins: {small_cap_coins}")

    small_cap_df = pd.DataFrame(small_cap_coins , columns=['Symbol'])
    small_cap_df.to_csv('small_cap_coins.csv', index = False)
    
    if small_cap_coins:
        coin_data = fetch_price_volume_data(small_cap_coins[0])
        f coin_data:
            coin_df = pd.DataFrame(coin_data)
            coin_df.to_csv(f'{small_cap_coins[0]}_price_volume_data.csv', index=False)
        print(f"Sample data for {small_cap_coins[0]}: {coin_data[:1]}")
