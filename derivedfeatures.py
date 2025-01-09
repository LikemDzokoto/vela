import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler
import ta


class DerivedFeatures:
    def __init__(self, rsi_period=14, adx_period=14, ma_period=20, bb_period=20, bb_std=2, volatility_period=14):
        self.rsi_period = rsi_period
        self.adx_period = adx_period
        self.ma_period = ma_period
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.volatility_period = volatility_period

    def calculate_log_returns(self, close):
        return np.log(close / close.shift(1))

    def calculate_hl_ratio(self, high, low):
        return high / low

    def calculate_volume_ma_ratio(self, volume):
        ma = volume.rolling(window=self.ma_period).mean()
        return volume / ma

    def calculate_volume_std(self, volume):
        ma = volume.rolling(window=self.ma_period).mean()
        std = volume.rolling(window=self.ma_period).std()
        return std / ma

    def calculate_rsi(self, close):
        return ta.momentum.RSIIndicator(close, window=self.rsi_period).rsi()

    def calculate_adx(self, high, low, close):
        return ta.trend.ADXIndicator(high, low, close, window=self.adx_period).adx()

    def calculate_volatility(self, close):
        return close.pct_change().rolling(self.volatility_period).std()

    def calculate_bb_position(self, close):
        bb = ta.volatility.BollingerBands(close, window=self.bb_period, window_dev=self.bb_std)
        lower_band = bb.bollinger_lband()
        upper_band = bb.bollinger_hband()
        return (close - lower_band) / (upper_band - lower_band)

    def calculate_trend_strength(self, close):
        sma_50 = ta.trend.SMAIndicator(close, window=50).sma_indicator()
        sma_200 = ta.trend.SMAIndicator(close, window=200).sma_indicator()
        return sma_50 - sma_200

    def normalize_features(self, df):
        """
        Normalize selected features to a range of [0, 1].
        """
        scaler = MinMaxScaler()
        # Select columns for normalization
        features_to_normalize = [
            'log_returns', 'hl_ratio', 'volume_ma_ratio', 'volume_std',
            'rsi', 'adx', 'volatility', 'bb_position', 'trend_strength'
        ]
        normalized_features = scaler.fit_transform(df[features_to_normalize])
        # Create a DataFrame with normalized features
        normalized_features_df = pd.DataFrame(
            normalized_features, 
            columns=features_to_normalize, 
            index=df.index
        )
        # Combine normalized features with the original DataFrame
        return pd.concat([df[['open', 'high', 'low', 'close', 'volume']], normalized_features_df], axis=1)

    def derive_features(self, df):
        """
        Compute derived features with tqdm progress tracking.
        """
        features = pd.DataFrame(index=df.index)

        # Use tqdm for progress tracking
        with tqdm(total=9, desc="Calculating Derived Features") as pbar:
            features['log_returns'] = self.calculate_log_returns(df['close'])
            pbar.update(1)

            features['hl_ratio'] = self.calculate_hl_ratio(df['high'], df['low'])
            pbar.update(1)

            features['volume_ma_ratio'] = self.calculate_volume_ma_ratio(df['volume'])
            pbar.update(1)

            features['volume_std'] = self.calculate_volume_std(df['volume'])
            pbar.update(1)

            features['rsi'] = self.calculate_rsi(df['close'])
            pbar.update(1)

            features['adx'] = self.calculate_adx(df['high'], df['low'], df['close'])
            pbar.update(1)

            features['volatility'] = self.calculate_volatility(df['close'])
            pbar.update(1)

            features['bb_position'] = self.calculate_bb_position(df['close'])
            pbar.update(1)

            features['trend_strength'] = self.calculate_trend_strength(df['close'])
            pbar.update(1)

        # Combine features with the original DataFrame
        derived_df = pd.concat([df, features], axis=1)
        # Fill missing values
        return derived_df.ffill().bfill()


def main():

    df = pd.read_csv("small_capped_coins.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)


    derived_features = DerivedFeatures()

   
    print("Calculating derived features...")
    processed_data = derived_features.derive_features(df)

    
    print("Normalizing features...")
    normalized_data = derived_features.normalize_features(processed_data)

    
    normalized_data.to_csv("processed_normalized_features.csv")
    print("Normalized features saved to processed_normalized_features.csv")


if __name__ == "__main__":
    main()
