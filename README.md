# Yahoo Finance Intraday Market Crawler

The script crawls intraday data from yahoo finance and save it as csv file in a directory 

The script takes in 3 parameters:
- number of thread
- directory path to store the scraped data
- csv path that specify the tickers to crawl

##### Execute Command:
```
python yahoofin_v1.py <number_thread> <directory_path> <ticker_csv_path>
```

##### Example:
```
python yahoofin_v1.py 10 ./data ./screener.csv
```

