import pandas as pd 
import numpy as np
import unitest

from ccbacktest.factors.factors import LambdaFactor, MACD, MA, RSI, VWAP


# generating a test df
n = 100
volume = np.random.random(n) * 10 + 10
open_ =  100 + 10 * np.random.random(n)
close = 100 + 10 * np.random.random(n)
high = np.maximum(open_, close) + np.random.random() * 4
low = np.minimum(open_, close) - np.random.random() * 4
open_time = np.arange(n)
test_df = pd.DataFrame(dict(open_time=open_time, open=open_,
                      high=high, low=low, close=close, volume=volume))

