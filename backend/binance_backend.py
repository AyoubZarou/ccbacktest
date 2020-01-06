import ccxt
from backend.backend import Backend
from data.caching import cache_download
import pandas as pd
import time


class BinanceBackend(Backend):
    def __init__(self, exchange: ccxt.binance):
        self.exchange = exchange
        self._data_names = ['open_time', 'open', 'high', 'low', 'close', 'volume']

    def get_historical_data(self, ticker: str, freq: str, start: pd.datetime,
                            end: pd.datetime = None) -> pd.DataFrame:
        pass

    def historical_ohlcv(self, symbol, start, end, timeframe='1m'):
        since = start
        total_df = pd.DataFrame()
        while since <= end:
            print('rr ss')
            df = pd.DataFrame(self.exchange.fetch_ohlcv(symbol, since=since, timeframe=timeframe),
                              columns=self._data_names)
            total_df = pd.concat([total_df, df])
            if df.shape[0] == 0:
                return total_df[(total_df.open_time >= start) & (total_df.open_time <= end)]
            since = total_df['open_time'].max() + 1
        return total_df[(total_df.open_time >= start) & (total_df.open_time <= end)]

    def get_tick_data(self):
        pass

    @cache_download("binance")
    def download(self, ticker: str, timeframe: str,
                 start: pd.datetime, end: pd.datetime = None,
                 format: str = None) -> pd.DataFrame:
        data = self.historical_ohlcv(ticker, start, end, timeframe=timeframe)
        return data

    def _download(self):
        pass

    def get_account_status(self):
        account = self.client.get_account()
        balances = pd.DataFrame(account['balances'])
        balances[['free', 'locked']] = balances[['free', 'locked']].astype(float)
        balances['update_time'] = pd.to_datetime(int(account['updateTime']), unit='ms')
        return balances

    def get_symbol_info(self, symbol):
        info = self.client.get_symbol_info(symbol=symbol)
        cols = ['symbol', 'status', 'baseAsset', 'baseAssetPrecision', 'quoteAsset', 'quotePrecision',
                'baseCommissionPrecision', 'quoteCommissionPrecision', 'icebergAllowed', 'ocoAllowed',
                'quoteOrderQtyMarketAllowed', 'isSpotTradingAllowed', 'isMarginTradingAllowed']
        return pd.DataFrame([info])[cols]
