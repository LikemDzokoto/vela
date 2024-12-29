import requests 
import pandas as  pd 
import os 
from dotenv  import load_dotenv 

load_dotenv()

api_key = os.getenv('BINANCE_API_KEY')
api_secrety = os.getenv('BINANCE_API_SECRET')




def fetch_data(symbol,  interval , start_time = None , end_time = None, limit = 500):
    url = 'https://api.binance.com/api/v3/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit 
        }
        
    headers= {
            'X-MBX-APIKEY': api_key,
            'X-MBX-SECRET-KEY': api_secrety
        }
    response = requests.get(url, params=params, headers=headers)
            

        
    if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data , columns = [
                'Open Time', 'Open', 'High', 'Low', 'Close', 
                                          'Volume', 'Close Time', 'Quote Asset Volume', 
                                          'Number of Trades', 'Taker Buy Base Asset Volume', 
                                          'Taker Buy Quote Asset Volume', 'Ignore'
            ])


          return df
    else:
        print(f"Error fetching data: {response.status_code} - {response.text}")
        return None
 

