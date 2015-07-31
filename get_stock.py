from yahoo_finance import Share
import datetime as dt
import pandas as pd
from pyspark import SparkContext
from os.path import join

def formatLine(line):
    line = line.values()
    line[0], line[5] = line[5], line[0]
    return ', '.join(line)

host = 'hdfs://localhost:9000'
ticker_path = host + '/user/hadoop/tickers.txt'
start = '2014-01-01'
end = dt.date.today().strftime('%Y-%m-%d')

sc = SparkContext(appName='stock_data')
stockData = sc.textFile(ticker_path).flatMap(lambda x: Share(x).get_historical(start, end)).map(formatLine)
stockData.saveAsTextFile(join(host, 'stock', end + '.csv'))
