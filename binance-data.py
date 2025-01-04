import os
from binance.client import Client
from dotenv import load_dotenv

import pandas as pd

load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

client = Client(API_KEY, API_SECRET)



logging.basicConfig(level=logging.INFO)

from datetime import datetime, timedelta

def is_recently_listed(listing_date, months=3):
    """
    Check if a coin was listed within the specified number of months.
    """
    current_date = datetime.now()
    listing_date = datetime.strptime(listing_date, "%Y-%m-%d")
    return listing_date >= (current_date - timedelta(days=months*30))

def get_listing_dates():
    """
    Get listing dates for USDT trading pairs.
    """
    exchange_info = client.get_exchange_info()
    listing_dates = {}
    for symbol_info in exchange_info['symbols']:
        symbol = symbol_info['symbol']
        if symbol.endswith('USDT') and 'permissions' in symbol_info and 'SPOT' in symbol_info['permissions']:
            listing_dates[symbol] = symbol_info['onboardDate'][:10]
    return listing_dates

def get_small_cap_coins(market_cap_threshold=100_000_000, months_since_listing=3):
    """
    Fetch coins with market cap under the specified threshold that were listed within the specified months.
    """
    
    tickers = client.get_all_tickers()
    listing_dates = get_listing_dates()
    
    # small-cap coins (market cap under $100M) listed within last 3 months
    small_cap_coins = []
    for ticker in tickers:
        symbol = ticker['symbol']
        if symbol.endswith('USDT') and symbol in listing_dates:  # Only consider USDT pairs
            try:
                # Get 24hr ticker data for market cap calculation
                ticker_24hr = client.get_ticker(symbol=symbol)
                volume = float(ticker_24hr['quoteVolume'])
                price = float(ticker_24hr['lastPrice'])
                market_cap = volume * price
                
                # Debug logging
                print(f"Processing {symbol}:")
                print(f"  Market Cap: {market_cap}")
                print(f"  Listing Date: {listing_dates[symbol]}")
                print(f"  Recently Listed: {is_recently_listed(listing_dates[symbol], months_since_listing)}")
                
                # Check both market cap and listing date
                if market_cap < market_cap_threshold:
                    if is_recently_listed(listing_dates[symbol], months_since_listing):
                        small_cap_coins.append(symbol)
                        print(f"  Added {symbol} to small-cap coins")
                    else:
                        print(f"  {symbol} not recently listed")
                else:
                    print(f"  {symbol} market cap too high")
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
        if coin_data:
            coin_df = pd.DataFrame(coin_data)
            coin_df.to_csv(f'{small_cap_coins[0]}_price_volume_data.csv', index=False)
        print(f"Sample data for {small_cap_coins[0]}: {coin_data[:1]}")
