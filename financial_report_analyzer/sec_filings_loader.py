import json
import time
from tqdm import tqdm
from scraping import SECScraper
from utils import load_tickers


def main():
    tickers = load_tickers(file_name="modified_tickers.yaml")
    sec_scraper = SECScraper()

    filings = {}

    for ticker in tqdm(tickers, ncols=60):
        try:
            filings[ticker] = sec_scraper.get_10k_filings(ticker)
            time.sleep(0.5)
        except Exception:
            filings[ticker] = {}
            continue

    with open("modified_tickers_filings.json", "w") as f:
        json.dump(filings, f)


if __name__ == "__main__":
    main()
