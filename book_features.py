import pandas as pd
from decimal import Decimal
from sortedcontainers import SortedDict as od


def wide_book_csv(df):
    odict_list = []
    odict = od({})

    for ix, row in df.iterrows():
        odict.update({"timestamp": row["timestamp"]})
        for index, i in enumerate(zip(eval(row["ask"]).items(), sorted(eval(row["bid"]).items(), reverse=True))):
            odict.update({f"asks[{index}].price": f"{i[0][0]}"})
            odict.update({f"asks[{index}].size": f"{i[0][1]}"})
            odict.update({f"bids[{index}].price": f"{i[1][0]}"})
            odict.update({f"bids[{index}].size": f"{i[1][1]}"})
        odict_list.append(odict.copy())

    df = pd.DataFrame(odict_list)
    df["date"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index(df["date"], inplace=True)
    df.drop(["timestamp"], axis=1, inplace=True)
    df = df.apply(pd.to_numeric)
    return df


def wide_book(data):
    """
    Function used to store l2_book data in an alternative way.
    Not default and must be enabled in the config.yaml under wide_tables: true
    data: list of dictionaries

    Example with book_depth = 3:
    data = [
    {
        "ask": {"11371.73": 0.80941112, "11371.74": 0.2, "11372.28": 4.23300088},
        "bid": {"11368.35": 1.59830075, "11368.37": 0.42152563, "11368.38": 0.60871406},
        "delta": False,
        "timestamp": 1565551972.7636518,
    },
    {
        "ask": {"11371.71": 1.55941112, "11371.74": 0.2, "11372.28": 4.23300088},
        "bid": {"11368.35": 1.59830072, "11368.37": 0.42152563, "11368.39": 0.60871406},
        "delta": False,
        "timestamp": 1565551972.764616,
    },
    ...]
    Notice that both [ask] and [bid] keys are stored in increasing value.
    We must reverse the order of the bids, so when zipped together we get the
    different book levels in the correct order.

    Example: highest_bid - lowest_ask,
             second_highest_bid - second_lowest_ask,
             ...
    """
    odict_list = []
    odict = od({})

    for dic in data:
        odict.update({"timestamp": dic["timestamp"]})
        for index, i in enumerate(zip(dic["ask"].items(), sorted(dic["bid"].items(), reverse=True))):
            odict.update({f"asks[{index}].price": f"{i[0][0]}"})
            odict.update({f"asks[{index}].size": f"{i[0][1]}"})
            odict.update({f"bids[{index}].price": f"{i[1][0]}"})
            odict.update({f"bids[{index}].size": f"{i[1][1]}"})
        odict_list.append(odict.copy())

    df = pd.DataFrame(odict_list)
    # df["date"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index(df["timestamp"], inplace=True)
    df.drop(["timestamp"], axis=1, inplace=True)
    df = df.apply(pd.to_numeric)
    return df


def get_wb_cols(levels=10):
    """Function to get the column names we need from our wide Book"""
    lc = ["[" + str(i) + "]" for i in range(0, levels)]
    askCols = ["asks" + l for l in lc]
    bidCols = ["bids" + l for l in lc]

    askSizeCols = [c + ".size" for c in askCols]
    askPriceCols = [c + ".price" for c in askCols]
    bidSizeCols = [c + ".size" for c in bidCols]
    bidPriceCols = [c + ".price" for c in bidCols]
    return bidSizeCols, bidPriceCols, askSizeCols, askPriceCols


def cum_bid_ask_imbalance(df, levels=10):
    """Calculate Cumulative Bid Ask Imbalance of the book"""
    bidSizeCols, _, askSizeCols, _ = get_wb_cols(levels)
    bidVolume = df[bidSizeCols].sum(axis=1)
    askVolume = df[askSizeCols].sum(axis=1)
    return (bidVolume - askVolume) / (bidVolume + askVolume)


def get_features(df, levels=10):
    features = df
    for i in range(1, levels + 1):
        features["bai" + str(i - 1)] = cum_bid_ask_imbalance(df, i)
    features["midprice"] = (features["asks[0].price"] + features["bids[0].price"]) / 2
    return features


# def weigh_book(df):
#     levels = int(df.shape[1] / 4)
#     newDF = pd.DataFrame()
#     for i in range(levels):
#         newDF['asks[' + str(i) + ']'] = df['asks[' + str(i) + '].price'] * df['asks[' + str(i) + '].size']
#         newDF['bids[' + str(i) + ']'] = df['bids[' + str(i) + '].price'] * df['bids[' + str(i) + '].size']
#     return newDF
