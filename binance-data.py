import os
from binance.client import Client
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

client = Client(API_KEY, API_SECRET)

def get_filtered_symbols_parallel(market_cap_threshold=100_000_000, min_volume=1_000_000, min_days_listed=90):
    """
    Fetch USDT trading pairs with specific conditions using parallel processing:
      - Market cap below the threshold.
      - Minimum 24-hour trading volume.
      - Listed for at least the specified number of days.

    Returns:
        list: Filtered symbols meeting the criteria.
    """
    print("Fetching exchange info...")
    exchange_info = client.get_exchange_info()
    symbols = [
        symbol_info['symbol'] for symbol_info in exchange_info['symbols']
        if symbol_info['symbol'].endswith('USDT') and symbol_info['status'] == 'TRADING'
    ]

    def process_symbol(symbol):
        try:
            ticker_24hr = client.get_ticker(symbol=symbol)
            volume = float(ticker_24hr['quoteVolume'])
            price = float(ticker_24hr['lastPrice'])
            market_cap = volume * price

            # Fetch listing age (minimum 90 daily klines required)
            klines = client.get_klines(symbol=symbol, interval='1d', limit=min_days_listed)

            if (
                market_cap < market_cap_threshold
                and volume >= min_volume
                and len(klines) >= min_days_listed
            ):
                return {
                    'symbol': symbol,
                    'market_cap': market_cap,
                    'volume': volume,
                    'days_listed': len(klines)
                }
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
        return None

    print("Processing symbols in parallel...")
    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_symbol, symbols), total=len(symbols), desc="Processing symbols"))

    filtered_symbols = [result for result in results if result is not None]
    return sorted(filtered_symbols, key=lambda x: x['market_cap'])

def fetch_historical_data(symbol, interval='15m', days=14):
    """
    Fetch historical OHLC and volume data for a given symbol.

    Args:
        symbol (str): The cryptocurrency trading pair symbol (e.g., BonkUSDT).
        interval (str): The time interval for the kline data (e.g., '15m').
        days (int): Number of days to fetch data for.

    Returns:
        pd.DataFrame: DataFrame containing OHLC and volume data.
    """
    try:
        print(f"Fetching historical data for {symbol} ({interval}, {days} days)...")
        since = int((pd.Timestamp.utcnow() - pd.Timedelta(days=days)).timestamp() * 1000)

        klines = client.get_historical_klines(symbol, interval, start_str=since)

        data = [{
            'timestamp': pd.to_datetime(kline[0], unit='ms'),
            'open': float(kline[1]),
            'high': float(kline[2]),
            'low': float(kline[3]),
            'close': float(kline[4]),
            'volume': float(kline[5])
        } for kline in klines]

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def main():
   
    filtered_symbols = get_filtered_symbols_parallel()

    if not filtered_symbols:
        print("No symbols met the criteria.")
    else:
        print(f"Filtered symbols found: {len(filtered_symbols)}")

        all_data = []
        for symbol_data in tqdm(filtered_symbols[:4], desc="Fetching historical data"):
            symbol = symbol_data['symbol']
            ohlc_data = fetch_historical_data(symbol, interval='15m', days=14)

            if not ohlc_data.empty:
                ohlc_data['symbol'] = symbol.replace('USDT', '')                                    
                ohlc_data['market_cap'] = symbol_data['market_cap']

                ohlc_data['market_cap'] = ohlc_data['market_cap'] * 1e6
                ohlc_data['volume'] = symbol_data['volume']

                


                ohlc_data[['open', 'high', 'low', 'close']] = ohlc_data[['open', 'high', 'low', 'close']].map(lambda x: f"{x:.8f}")
               
                all_data.append(ohlc_data[['timestamp','symbol', 'market_cap','open', 'high', 'low', 'close', 'volume']])

        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data.to_csv("small_capped_coins.csv", index=False)
            print("Historical data and market cap information saved to small_capped_coins.csv.")
        else:
            print("No historical data available for the filtered symbols.")

if __name__ == "__main__":
    main()
