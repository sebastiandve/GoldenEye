import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functions import generate_volumebars, read_trades, read_book
from book_features import get_features, wide_book, get_wb_cols

# import vectorbt as vbt

feed = "binance_futures"
symbol = "BTC-USD"
start = "2021-05-14 20:26:00"
end = "2021-05-14 23:59:59.999000"

data = read_book(feed, symbol, start, end)
df = wide_book(data)
trades = read_trades(feed, symbol, start, end)
trades = trades[(trades.index >= start) & (trades.index <= end)]
bars = generate_volumebars(trades, 1000)
levels = 20
bidSizeCols, bidPriceCols, askSizeCols, askPriceCols = get_wb_cols(levels)
features = get_features(df, levels)

df2 = pd.merge_asof(bars, features, left_index=True, right_index=True)
# df2 = pd.merge_asof(df2, df, left_index=True, right_index=True)
# df2['midprice'] =

fig, ax = plt.subplots(3, 1, sharex=True)
f1 = df2.reset_index().close
ax[0].plot(f1)
ax[0].plot(df2.reset_index()["midprice"], label="midprice")
ax[0].plot(df2.reset_index()["asks[0].price"], label="best ask")
ax[0].legend()
bai_cols = [c for c in features.columns if c[0:3] == "bai"]
# ax[1].plot(df2.reset_index()[bai_cols], label=bai_cols, alpha=0.25)
ax[1].plot(df2.reset_index()[bai_cols].mean(axis=1).rolling(1).mean(), label="mean", color="k")
# ax[1].plot(df2.reset_index()[bai_cols].mean(axis=1).ewm(span=64).mean(), label='ewmmean', color='r')
ax[1].plot(df2.reset_index()[bai_cols].median(axis=1), label="median", color="r")
ax[1].plot(df2.reset_index()["bai19"], label="bai0", color="g")
# ax[2].stackplot(df2.reset_index().index, df2[bidSizeCols[::-1]].T, labels=bidSizeCols[::-1])
# ax[2].stackplot(df2.reset_index().index, df2[askSizeCols[::-1]].T * -1, labels=askSizeCols[::-1])
ax[2].stackplot(
    df2.reset_index().index, df2[["bids[0].size", "bids[1].size"]].T, labels=["bids[0].size", "bids[1].size"]
)
ax[2].stackplot(
    df2.reset_index().index, df2[["asks[0].size", "asks[1].size"]].T * -1, labels=["asks[0].size", "asks[1].size"]
)
# f2 = df2.reset_index()['asks[0].size']
# f3 = df2.reset_index()['bids[0].size']
# ax[2].stackplot(df2.reset_index().index, f3 / (f2+f3))
ax[1].legend()
# ax[1].legend()


### TEST SIMPLE STRATEGY USING VECTORB JUST USING AGRESSIVE BID ASK IMBALANCE CHANGES AS AN INDICATOR
## IF THERE ARE ACTUALLY TOO MANY FALSE POSITIVES THEN WE CAN REFINE BUT NOT BEFORE!!!
# vbt.settings.portfolio['init_cash'] = 100.  # 100$
# vbt.settings.portfolio['fees'] = 0.0025  # 0.25%
# vbt.settings.portfolio['slippage'] = 0.0025  # 0.25%
# df3 = df2[['open', 'high', 'low', 'close', 'volume']]
# df3.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
# df3.vbt.ohlcv.plot()
