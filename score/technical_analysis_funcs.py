import pandas as pd
from ta import *
import numpy as np
import pandas as pd
import math
#import plotly.plotly as py
#from .utils import *
#import plotly.graph_objs as go 
#import matplotlib.pyplot as plt
#import matplotlib as mpl
#mpl.style.use('seaborn')

from datetime import datetime

# Load data
#df = pd.read_csv('ta/data/datas.csv', sep=',')

#df = pd.read_csv('results/litecoin.csv', sep=',')
#df = utils.dropna(df)

#print(df.columns)

# Bollinger Bands
def ema(series, periods):
    sma = series.rolling(window=periods, min_periods=periods).mean()[:periods]
    rest = series[periods:]
    return pd.concat([sma, rest]).ewm(span=periods, adjust=False).mean()

def Bollinger_Bands(df,arguments):
	#If arguments are empty, use Default arguments
	period = 20
	ndev = 2
	if arguments:
		[period, ndev] = arguments
		
	# Add bollinger band high indicator filling nans values
	df['bb_high_indicator'] = bollinger_hband_indicator(df["Close"], period, ndev,fillna=True)
	# Add bollinger band low indicator filling nans values
	df['bb_low_indicator'] = bollinger_lband_indicator(df["Close"], period, ndev,fillna=True)
	# Add volatility
	df['volatility_bbh'] = bollinger_hband(df["Close"], period, ndev,fillna=True)
	df['volatility_bbl'] = bollinger_lband(df["Close"], period, ndev,fillna=True)
#	df['volatility_bbm'] = bollinger_mband(df["Close"], n=20, ndev=2,fillna=True)
	df['volatility_bbm'] = bollinger_mavg(df["Close"], period, fillna=False)

#def plot_BollingerBands(df):
	#dates=[datetime.datetime.fromtimestamp(ts) for ts in df.Timestamp]
#	d = []
#	for ts in df.Date:
#		d.append(datetime.strptime(ts, '%b %d, %Y'))
#	plt.plot(d[900:1000],df[900:1000].Close)
#	plt.plot(d[900:1000],df[900:1000].volatility_bbh, label='High BB')
#	plt.plot(d[900:1000],df[900:1000].volatility_bbl, label='Low BB')
#	plt.plot(d[900:1000],df[900:1000].volatility_bbm, label='MA BB')
	#plt.title('Bollinger Bands litecoin')
	#plt.legend()
	#plt.show()
	#py.iplot(d[900:1000],df[900:1000].Close)
	#py.iplot(d[900:1000],df[900:1000].volatility_bbh, label='High BB')
	#py.iplot(d[900:1000],df[900:1000].volatility_bbl, label='Low BB')
	#py.iplot(d[900:1000],df[900:1000].volatility_bbm, label='MA BB')
#	plt.title('Bollinger Bands litecoin')
#	plt.legend()
#	plt.show()

# Moving Average Convergence Divergence
'''Moving average convergence divergence (MACD) is a trend-following momentum indicator
that shows the relationship between two moving averages of prices. The MACD is calculated
by subtracting the 26-day exponential moving average (EMA) from the 12-day EMA.
A nine-day EMA of the MACD, called the "signal line", is then plotted on top of the MACD,
functioning as a trigger for buy and sell signals.'''

def MACD(df,arguments):
	#If arguments are empty, use Default arguments
	n_fast=12
	n_slow=26
	n_sign=9
	if arguments:
		[n_fast, n_slow, n_sign] = arguments		
	df['trend_macd'] = macd(df["Close"], n_fast, n_slow, fillna=False)
	df['trend_macd_signal'] =  macd_signal(df["Close"], n_fast, n_slow, n_sign, fillna=False)
	df['trend_macd_diff'] = macd_diff(df["Close"], n_fast, n_slow, n_sign, fillna=False)

'''def plot_MACD(df):
	#dates=[datetime.datetime.fromtimestamp(ts) for ts in df.Timestamp]
	dates = []
	for ts in df.Date:
                dates.append(datetime.strptime(ts, '%b %d, %Y'))
	plt.plot(dates, df.trend_macd, label='MACD')
	plt.plot(dates, df.trend_macd_signal, label='MACD Signal')
	plt.plot(dates, df.trend_macd_diff, label='MACD Difference')
	plt.title('MACD, MACD Signal and MACD Difference')
	plt.legend()
	plt.show()
'''	
#IchimokuKinkoHyo
"""Ichimoku Kinkō Hyō (Ichimoku)
It identifies the trend and look for potential signals within that trend."""
def IchimokuKinkoHyo(df,arguments):
	#If arguments are empty, use Default arguments
	n1=9
	n2=26
	n3=52
	if arguments:
		[n1, n2, n3] = arguments

	df['trend_ichimoku_a'] = ichimoku_a(df["High"], df["Low"], n1, n2, fillna=True)
	df['trend_ichimoku_b'] = ichimoku_b(df["High"], df["Low"], n2, n3, fillna=True)
'''def plot_IchimokuKinkoHyo(df):
	dates = []
	for ts in df.Date:
                dates.append(datetime.strptime(ts, '%b %d, %Y'))
	plt.plot(dates,df.Close)
	plt.plot(dates,df.trend_ichimoku_a, label='Ichimoku a')
	plt.plot(dates,df.trend_ichimoku_b, label='Ichimoku b')
	plt.title('Ichimoku Kinko Hyo')
	plt.legend()
	plt.show()
'''

#RSI
def RSI(df,period):
	#If arguments are empty, use Default arguments
	n=14
	if period:
		n = period
	df['momentum_rsi'] = rsi(df['Close'], n, fillna=False);

'''def plot_RSI(df):
	dates = []
	for ts in df.Date:
                dates.append(datetime.strptime(ts, '%b %d, %Y'))
	#plt.plot(dates,df.Close)
	plt.plot(dates,df.momentum_rsi, label='RSI')
	plt.title('RSI')
	plt.legend()
	plt.show()
'''
def Moving_average_crossover(df,arguments,fillna):
	#If arguments are empty, use Default arguments
	#nfast=42
	#nslow=252
	n1 = 9
	n2 = 21
	n3 = 30
	n4 = 50
	n5 = 100
	n6 = 200

	df['9band'] = 0.0
	df['21band'] = 0.0
	df['30band'] = 0.0
	df['50band'] = 0.0
	df['100band'] = 0.0
	df['200band'] = 0.0
	df['9-21band'] = 1
	df['9-30band'] = 1
	df['9-50band'] = 1
	df['9-100band'] = 1
	df['9-200band'] = 1

	if arguments:
		[nfast, nslow] = arguments

	#df['9band'] = pd.rolling_mean(df['Close'],n1,min_periods=1)
	#df['21band'] = pd.rolling_mean(df['Close'],n2,min_periods=1)
	#df['30band'] = pd.rolling_mean(df['Close'],n3,min_periods=1)
	#df['50band'] = pd.rolling_mean(df['Close'],n4,min_periods=1)
	#df['100band'] = pd.rolling_mean(df['Close'],n5,min_periods=1)
	#df['200band'] = pd.rolling_mean(df['Close'],n6,min_periods=1)
	df['9band'] = ema(df['Close'],n1)
	df['21band'] = ema(df['Close'],n2)
	df['30band'] = ema(df['Close'],n3)
	df['50band'] = ema(df['Close'],n4)
	df['100band'] = ema(df['Close'],n5)
	df['200band'] = ema(df['Close'],n6)

	df['9-21band'] = df['9band'] - df['21band']
	df['9-21band'] = np.where(df['9-21band'] > 0,1,0)
	df['9-30band'] = df['9band'] - df['30band']
	df['9-30band'] = np.where(df['9-30band'] > 0,1,0)
	df['9-50band'] = df['9band'] - df['50band']
	df['9-50band'] = np.where(df['9-50band'] > 0,1,0)
	df['9-100band'] = df['9band'] - df['100band']
	df['9-100band'] = np.where(df['9-100band'] > 0,1,0)
	df['9-200band'] = df['9band'] - df['200band']
	df['9-200band'] = np.where(df['9-200band'] > 0,1,0)
	
	
	#df['fastband'] = np.round(df['Close'].rolling(window=nfast).mean(),2)
	#df['slowband'] = np.round(df['Close'].rolling(window=nslow).mean(),2)
	#fband = df['fastband']
	#sband = df['slowband']
	#if fillna:
	#	fband = fband.replace([np.inf, -np.inf], np.nan).fillna(0)
	#	sband = sband.replace([np.inf, -np.inf], np.nan).fillna(0)
#	df['signal'][nfast:] = np.where(df['fastband'][nfast:] > df['slowband'][nslow:], 1.0, 0.0)
	#df['nfast-nslow'] = df['fastband'] - df['slowband']
	#X = 50
	#df['Stance'] = np.where(df['nfast-nslow'] > X, 1, 0)	
	#df['Stance'] = np.where(df['nfast-nslow'] < X, -1, df['Stance'])
#	Stance = df['Stance']
#	if fillna:
 #              Stance = Stance.replace([np.inf, -np.inf], np.nan).fillna(0)

	return df
'''def FibonacciRetracements(df):
	dates = []
	for ts in df.Date:
                dates.append(datetime.strptime(ts, '%b %d, %Y'))
	price_min = df.Close.min()

	price_max = df.Close.max()

	#The retracement levels for Fibonacci ratios of 23.6%, 38.2% and 61.8% are calculated as follows:
	# Fibonacci Levels considering original trend as upward move
	diff = price_max - price_min
	level1 = price_max - 0.236 * diff
	level2 = price_max - 0.382 * diff
	level3 = price_max - 0.618 * diff

	print ("Level Price")
	print ("0 ", price_max)
	print ("0.236", level1)
	print ("0.382", level2)
	print ("0.618", level3)
	print ("1 ", price_min)

	plt.plot(dates,df['Close'],)
	plt.axhspan(level1, price_min, alpha=0.4, color='lightsalmon')
	plt.axhspan(level2, level1, alpha=0.5, color='palegoldenrod')
	plt.axhspan(level3, level2, alpha=0.5, color='palegreen')
	plt.axhspan(price_max, level3, alpha=0.5, color='powderblue')

	plt.ylabel("Price")
	plt.xlabel("Date")
	plt.legend(loc=2)
	plt.show()
'''
#Bollinger_Bands( df, []);
#print(df);
#plot_BollingerBands(df);
#IchimokuKinkoHyo(df,[]);
#plot_IchimokuKinkoHyo(df);
#MACD(df, []);
#plot_MACD(df);
#print(df)
#RSI(df,[]);
#indicators = {}
#print(df[len(df)-20:len(df)])
#indicators['Date'] = df[len(df)-30:len(df)].Date
#indicators['macd'] = df[len(df)-30:len(df)].trend_macd
#indicators['macd_diff'] = df[len(df)-30:len(df)].trend_macd_diff
#indicators['rsi'] = df[len(df)-30:len(df)].momentum_rsi

#new = df[['Date','Close','volatility_bbm','trend_macd', 'trend_macd_diff', 'momentum_rsi']][len(df)-10:len(df)].copy()
#print(df[len(df)-30:len(df)].momentum_rsi < 50)
#print(new)
#plot_RSI(df);
#Moving_average_crossover(df, [],False);
#FibonacciRetracements(df);
