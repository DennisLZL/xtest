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
    start = '2014-01-01'

    hdfs = PyWebHdfsClient(host='localhost', port='50070', user_name='hadoop')
    folder = hdfs.list_dir('user/hadoop/stock')['FileStatuses']['FileStatus']
    files = sorted([dt.datetime.strptime(f['pathSuffix'].split('.')[0], '%Y-%m-%d').date() for f in folder])
    
    lastdate = files[-1]



    #end = dt.date.today().strftime('%Y-%m-%d')

    #sc = SparkContext(appName='stock_data')
    #stockData = sc.textFile(ticker_path).flatMap(lambda x: Share(x).get_historical(start, end)).map(formatLine)
    #stockData.saveAsTextFile(join(save_path, end + '.csv'))
