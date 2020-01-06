# ccbacktest
A python library to backtest cryptocurrencies trading

This repo is meant to provide a backend engine for cryptocurrencies trading on multiple exchanges, it's not a working version yet, 
it support so far data downloading and caching in an efficient way,  and it provides a high level interface to impliment different 
technical factors used in trading (MACD, MA, RSI, ...) 

# Goal and philosophy behind the project
the idea behind the project is to create a data loader classe that best simulates real life, having full access to the train data and an iterative access to test data, this way the strategy could be scaled easily to live trading,  the project is now nowhere near what it's intended to be and unusuable for now.
