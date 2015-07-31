from yahoo_finance import Share
import datetime as dt
import pandas as pd
from pyspark import SparkContext
from os.path import join
from pywebhdfs.webhdfs import PyWebHdfsClient

def formatLine(line):
    line = line.values()
    line[0], line[5] = line[5], line[0]
    return ', '.join(line)

if __name__ == '__main__':

    host = 'hdfs://localhost:9000'
    ticker_path = host + '/user/hadoop/tickers.txt'
    save_path = host + '/user/hadoop/stock'

    hdfs = PyWebHdfsClient(host='localhost', port='50070', user_name='hadoop')
    folder = hdfs.list_dir('user/hadoop/stock')['FileStatuses']['FileStatus']
    files = sorted([dt.datetime.strptime(f['pathSuffix'].split('.')[0], '%Y-%m-%d').date() for f in folder])
    end = dt.date.today().strftime('%Y-%m-%d')
    
    sc = SparkContext(appName='stock_data')
    
    if len(files) > 3:
        hdfs.delete_file_dir(join(save_path, files[0].strftime('%Y-%m-%d') + '.csv'), recursive=True)

    if len(files) == 0:
        start = '2014-01-01'
        stockData = sc.textFile(ticker_path).flatMap(lambda x: Share(x).get_historical(start, end)).map(formatLine)
        stockData.saveAsTextFile(join(save_path, end + '.csv'))
    else:
        start = (files[-1] + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        histStockData = sc.textFile(join(save_path, files[-1].strftime('%Y-%m-%d') + '.csv'))
        stockData = sc.textFile(ticker_path).flatMap(lambda x: Share(x).get_historical(start, end)).map(formatLine)
        histStockData.union(stockData).saveAsTextFile(join(save_path, end + '.csv'))

