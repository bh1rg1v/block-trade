import os

import requests
import pandas as pd
import yfinance as yf
from tqdm import tqdm

LIMIT = 10
TOP_TICKERS_CACHE = "top_tickers.csv"


def get_sp500_tickers():
    """
    Returns the list of S&P 500 ticker symbols scraped from Wikipedia.
    Results are cached locally in sp500.csv to avoid repeated network calls.
    """
    cache_path = "sp500.csv"

    if os.path.exists(cache_path):
        return pd.read_csv(cache_path)["Symbol"].tolist()

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    table = pd.read_html(response.text)[0]
    table.to_csv(cache_path, index=False)

    return table["Symbol"].tolist()


def get_top_by_volume(tickers, limit=10):
    """
    Ranks tickers by average daily volume over the past 5 days and returns
    the top `limit` symbols.
    """
    volume_data = []

    for ticker in tqdm(tickers[:200]):
        try:
            hist = yf.Ticker(ticker).history(period="5d", interval="1d")
            if not hist.empty:
                volume_data.append((ticker, hist["Volume"].mean()))
        except Exception:
            continue

    volume_data.sort(key=lambda x: x[1], reverse=True)
    return [t[0] for t in volume_data[:limit]]


def get_company_names(symbols):
    """
    Fetches the full company name for each symbol using yfinance.
    Falls back to the symbol itself if the name cannot be retrieved.
    """
    names = {}
    for symbol in symbols:
        try:
            names[symbol] = yf.Ticker(symbol).info.get("longName", symbol)
        except Exception:
            names[symbol] = symbol
    return names


def get_top_stocks(limit=LIMIT):
    """
    Returns a dict of {symbol: company_name} for the top `limit` S&P 500
    stocks by average daily volume. Results are cached in top_tickers.csv
    and reused on subsequent calls. Delete the cache file to force a refresh.
    """
    if os.path.exists(TOP_TICKERS_CACHE):
        df = pd.read_csv(TOP_TICKERS_CACHE)
        return dict(zip(df["Symbol"], df["Name"]))

    top = get_top_by_volume(get_sp500_tickers(), limit)
    names = get_company_names(top)
    pd.DataFrame({"Symbol": top, "Name": [names[s] for s in top]}).to_csv(
        TOP_TICKERS_CACHE, index=False
    )
    return names


if __name__ == "__main__":
    stocks = get_top_stocks()
    print("Top Stocks:")
    for i, (symbol, name) in enumerate(stocks.items(), 1):
        print(f"  {i}. {symbol} - {name}")
