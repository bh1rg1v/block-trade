import yfinance as yf
import pandas as pd
import os
from tqdm import tqdm
import requests

# -------------------------------
# Step 1: Define limit
# -------------------------------
LIMIT = 10

# -------------------------------
# Step 2: Get US tickers (S&P 500)
# -------------------------------
def get_sp500_tickers():
    cache_path = "sp500.csv"

    if os.path.exists(cache_path):
        return pd.read_csv(cache_path)['Symbol'].tolist()

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    tables = pd.read_html(response.text)
    table = tables[0]

    table.to_csv(cache_path, index=False)  # cache it

    return table['Symbol'].tolist()

tickers = get_sp500_tickers()

# -------------------------------
# Step 3: Rank by volume
# -------------------------------
def get_top_by_volume(tickers, limit=10):
    volume_data = []

    for ticker in tqdm(tickers[:200]):  # limit initial universe (speed)
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d", interval="1d")

            if len(hist) > 0:
                avg_volume = hist["Volume"].mean()
                volume_data.append((ticker, avg_volume))
        except:
            continue

    # Sort by volume descending
    volume_data.sort(key=lambda x: x[1], reverse=True)

    top_tickers = [t[0] for t in volume_data[:limit]]
    return top_tickers

top_stocks = get_top_by_volume(tickers, LIMIT)

print("Top Stocks:", top_stocks)

# -------------------------------
# Step 4: Fetch minute data
# -------------------------------
def fetch_minute_data(tickers):
    data_dict = {}

    for ticker in tqdm(tickers):
        try:
            df = yf.download(
                ticker,
                period="5d",        # max allowed for 1m
                interval="1m",
                progress=False
            )

            if not df.empty:
                df.reset_index(inplace=True)
                data_dict[ticker] = df
        except:
            continue

    return data_dict

minute_data = fetch_minute_data(top_stocks)

# -------------------------------
# Step 5: Save to CSV
# -------------------------------
save_dir = "data/raw/stocks/"
os.makedirs(save_dir, exist_ok=True)

for ticker, df in minute_data.items():
    file_path = os.path.join(save_dir, f"{ticker}.csv")
    df.to_csv(file_path, index=False)

print("Data saved successfully!")