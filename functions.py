import pandas as pd
import numpy as np
import pandas as pd
from collections import OrderedDict as od
from arctic import Arctic
from arctic.date import DateRange
import matplotlib.pyplot as plt


def read_book(feed, symbol, start, end):
    store = Arctic("mongodb+srv://admin:6EXvy4T8n9UnaEMtsUAVfI@gg-cluster.ldnwj.mongodb.net")
    library = store[feed]
    it = library.iterator("l2_book-" + symbol, chunk_range=DateRange(start, end))
    book = {"bid": od(), "ask": od()}
    data = []
    for chunk in it:
        chunk["delta"] = chunk.delta.map({"False": False, "True": True})
        for row in chunk.iterrows():
            timestamp = row[0]
            receipt_timestamp, delta, side, price, size = row[1].values
            delta = bool(delta)
            if size == 0:
                book[side].pop(price, None)
            else:
                book[side][price] = size

            if delta and len(book["ask"]) == 20 and len(book["bid"]) == 20:
                data.append({"ask": book["ask"].copy(), "bid": book["bid"].copy(), "timestamp": timestamp})

    return data


def read_trades(feed, symbol, start, end):
    store = Arctic("mongodb://127.0.0.1:27017")
    library = store[feed]
    it = library.iterator("trades-" + symbol, chunk_range=DateRange(start, end))
    df = pd.DataFrame()
    for chunk in it:
        chunk.drop(["order_type", "id"], axis=1, inplace=True)
        df = pd.concat([df, chunk])
    return df


def generate_volumebars(df, frequency=10):
    times = df.index
    prices = df["price"]
    volumes = df["size"]
    buyVolume = np.where(df["side"] == "buy", df["size"], 0)
    ans = np.zeros(shape=(len(prices), 7))
    candle_counter = 0
    vol = 0
    lasti = 0
    for i in range(len(prices)):
        vol += volumes[i]
        if vol >= frequency:
            ans[candle_counter][0] = times[i].timestamp()  # time of sampling / last trade in bar
            ans[candle_counter][1] = prices[lasti]  # open
            ans[candle_counter][2] = np.max(prices[lasti : i + 1])  # high
            ans[candle_counter][3] = np.min(prices[lasti : i + 1])  # low
            ans[candle_counter][4] = prices[i]  # close
            ans[candle_counter][5] = np.sum(volumes[lasti : i + 1])  # volume
            ans[candle_counter][6] = np.sum(buyVolume[lasti : i + 1])  # buy volume
            candle_counter += 1
            lasti = i + 1
            vol = 0
    bars = pd.DataFrame(
        ans[:candle_counter], columns=["timestamp", "open", "high", "low", "close", "volume", "buyVolume"]
    )
    bars["sellVolume"] = bars.volume - bars.buyVolume
    bars.set_index(pd.to_datetime(bars.timestamp, unit="s"), inplace=True)
    bars.drop(columns=["timestamp"], inplace=True)
    return bars


def plot(df):
    fig, ax = plt.subplots(2, 1, sharex=True)
    ax[0].plot(df.index, df["bids[0].price"], label="bid")
    ax[0].plot(df.index, df["asks[0].price"], label="ask")

    levels = 5
    bid_size_cols = [c for c in df.columns if c[-4:] == "size" and c[0:4] == "bids" and int(c[5]) < levels]
    ask_size_cols = [c for c in df.columns if c[-4:] == "size" and c[0:4] == "asks" and int(c[5]) < levels]

    ax[1].plot(df.index, df[bid_size_cols].sum(axis=1), label="bidVolume")
    ax[1].plot(df.index, df[ask_size_cols].sum(axis=1), label="askVolume")

    # oib = (df[bid_size_cols].sum(axis=1) - df[ask_size_cols].sum(axis=1)) / (df[bid_size_cols].sum(axis=1) + df[ask_size_cols].sum(axis=1))
    # ax[1].plot(df.index, oib, label='imbalance')

    plt.legend(loc="upper left")
    plt.tight_layout()
    # plt.show()
