import time
import requests
import json
from datetime import datetime, timezone, timedelta
import pandas as pd
import os
import math
import threading
import signal
import sys
def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    stop_threads = True

class myThread (threading.Thread):
    def __init__(self, threadID, name, tickers_lookup, directory):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.tickers_lookup = tickers_lookup
        self.directory = directory
        
    def run(self):
        print ("Starting " + self.name)
        for ticker in self.tickers_lookup:
            print(ticker)
            try:
                crawl(ticker, self.directory)
            except:
                print("An exception occurred when crawling: {0}".format(ticker))
            global stop_threads 
            if stop_threads: 
                break
        print ("Exiting " + self.name)
        
def crawl(ticker, directory):
    #directory =  str(sys.argv[2])
    #ticker="QQQ"
    #folder="./data"
    folder="{0}/{1}".format(directory, ticker)
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={1}&period1={2}&period2={3}&interval=1m&includePrePost=true'.format(ticker,ticker,int(time.time())-600000 , int(time.time()))
    if not os.path.exists(folder):
        os.makedirs(folder)
    resp = requests.get(url)
    meta = json.loads(resp.text)["chart"]["result"][0]["meta"]
    timestamps = json.loads(resp.text)["chart"]["result"][0]["timestamp"]
    indicators = json.loads(resp.text)["chart"]["result"][0]["indicators"]["quote"][0]
    index = 0
    cols = list(indicators.keys())
    data = []
    for timestamp in timestamps:
        record=[]
        if(indicators["close"][index] is not None):
            dt = datetime.fromtimestamp(timestamp, timezone.utc)
            market_period = "open"
            if(dt.hour<13):
                market_period = "pre"
                
            if(dt.hour==13):
                if(dt.minute<30):
                    market_period = "pre"
                    
            if(dt.hour>13):
                if(dt.hour>=20 or dt.hour == 0):
                    market_period = "post"
            
            record.append(int(timestamp))
            record.append(dt)      
            record.append(market_period)       
            for col in cols:
                record.append(indicators[col][index])    
        index = index + 1
        data.append(record)
    df = pd.DataFrame(data, columns = (['timestamp', 'datetime', 'period']+cols)).dropna()
    df["timestamp"] = df["timestamp"].astype(float).astype(int)
    
    # loop dates
    start_date = df.datetime.max() - timedelta(days=6)
    end_date = df.datetime.max()
    delta = timedelta(days=1)
    
    # write to csv, each date per file
    while start_date <= end_date:
        df_date = df[(df["datetime"] >= start_date.strftime("%Y-%m-%d")) & (df["datetime"] < (start_date+timedelta(days=1)).strftime("%Y-%m-%d"))]
        if(len(df_date)>0):
            save_location = "{0}/{1}_{2}.csv".format(folder, ticker, start_date.strftime("%Y-%m-%d"))
            df_date.to_csv(save_location, index=False)
            print(save_location)
        start_date += delta

            
if __name__ == "__main__":
    thread_count = int(sys.argv[1])
    directory =  str(sys.argv[2])
    if not os.path.exists(directory):
        os.mkdir(directory)
    
    stop_threads = False
    exitFlag = 0
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    
    threads_arr=[]
    tickers = pd.read_csv("D:\Workspace\stock_scraper\scraper\yahoofin\screener.csv")
    if(tickers is not None and len(tickers)>0):
        cnt = math.ceil(len(tickers)/thread_count)
        for x in range(0, thread_count):
            tickers_lookup = tickers[x*cnt:x*cnt+cnt].Ticker
            t = myThread(x, "Thread-{0}".format(x), tickers_lookup, directory)
            threads_arr.append(t)
    
    # Start new Threads
    for t in threads_arr:
        t.start()
    for t in threads_arr:
        t.join()
    print ("Exiting Main Thread")