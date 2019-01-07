import json
import sys
import requests
from coinmetrics import CoinMetricsAPI
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date, timedelta
import pyodbc
import time
import math

import utils

# CryptoCompare daily history
from crycompare import history as h


def update_ta_db(df):
    #Database Connection
    config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()
    #for index, row in df.iterrows():
     #print("DB")
     #print(row['Id'])
     #print(str_to_utf16le(row['Id']))
    for index, row in df.iterrows():
        cursor.execute('INSERT INTO dbo.cryptoHistory (Id,Name,Symbol,Close_value,Date,High,Low,Market_Cap,Open_value,Volume) VALUES(?,?,?,?,?,?,?,?,?,?)',
         row['Id'],
         row['Name'],
         row['Symbol'],
         row['Close'],
         row['Date'],
         row['High'],
         row['Low'],
         row['Market Cap'],
         row['Open'],
         row['Volume']
        )
        cursor.commit()
    db.disconnect()
    #print(df)

def ta_features(i_from_date=None, i_to_date=None,i_coin_markets=[]):
    """
    :param str 'YYYY-MM-DD' i_from_date: pull data from this date [includes]
    :param str 'YYYY-MM-DD' i_to_date: pull data till this date [includes]
    writes to a csv file - historic data of coins
    """
    counter = 1
    from_date, to_date = get_from_to_dates(i_from_date, i_to_date)
    df_coins = pd.DataFrame([])
    coins_dict = get_coins_from_database()

    iterations = math.ceil(len(coins_dict.items())/25)
    for j in range(iterations):
        start = j*25
        end = (j+1)*25 -1
        counter = 0
        for id,coinNames in coins_dict.items():
            if(counter >=start and counter <= end):
                coin = coinNames[2]
                name = coinNames[1]
                symbol = coinNames[0]
                #print(coin)
                df_coins = df_coins.append(get_coins_historical_data(symbol,name,coin, from_date, to_date))
            counter += 1
        time.sleep(90)
    df_coins2 = df_coins[::-1]
    #print(df_coins2)
    return df_coins2


def get_coins_from_database():
    """
    :return dict: rank, coin name
    """
    #Database Connection
    config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    cursor.execute('Select myid,Symbol,Name,WebsiteSlug from dbo.Coins')
    R = cursor.fetchall()
    coins_dict = {}
    for r in R:
        coins_dict[r[0]] = [r[1],r[2],r[3]]
    return coins_dict

def get_coins_current_ranking(i_start, i_limit, i_min_volume):
    """
    :param int i_start: pull data from coin current ranking [includes]
    :param int i_limit: pull data till coin current ranking [includes]
    :param float i_min_volume: pull coins with 24 Hrs volume bigger [includes] than this value
    :return dict: rank, coin name
    """

    url_coin_list_json = 'https://api.coinmarketcap.com/v1/ticker/?start={}&limit={}'.format(i_start - 1, i_limit)
    #headers = {'Accept': 'application/json','Accept-Encoding': 'deflate, gzip','X-CMC_PRO_API_KEY': '98f46f7f-36c7-4a33-8c84-2fbb8430406e',}
    #url_coin_list_json = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map/?start={}&limit={}'.format(i_start - 1, i_limit)
    page = requests.get(url_coin_list_json)
    json_file = json.loads(page.text)
    coins_dict = {}
    for k in json_file:
        if float(k['24h_volume_usd']) >= i_min_volume:
            coins_dict[k['rank']] = [k['id'],k['name'],k['symbol']]
    #print (coins_dict)
    return coins_dict


def get_coins_historical_data(i_symbol, i_name, i_coin, i_from_date, i_to_date):
    """
    :param str i_coin: coin name
    :param date 'YYYYMMDD' i_from_date: pull data from this date [includes]
    :param date 'YYYYMMDD' i_to_date: pull data till this date [includes]
    return list: coin history data includes current ranking
    """
    df_coin = get_specific_coin_historical_data_coinmarketcap(i_coin, i_from_date, i_to_date)
    #df_coin = get_specific_coin_historical_data_cryptocompare(i_symbol, i_from_date, i_to_date)
    #if(df_coin):
    #print(len(df_coin))
    df_coin['Id'] = i_coin
    df_coin['Name'] = i_name
    df_coin['Symbol'] = i_symbol
    df_coin = pd.concat([df_coin.iloc[:,7:], df_coin.iloc[:,0:7]], axis=1, join_axes=[df_coin.index])
    return df_coin

def  get_specific_coin_historical_data_coinmarketcap(i_coin, i_from_date, i_to_date):
    """
    :param str i_coin: coin name
    :param date 'YYYYMMDD' i_from_date: pull data from this date [includes]
    :param date 'YYYYMMDD' i_to_date: pull data till this date [includes]
    return list: coin history data
    """
    print(i_coin)
    #print(i_from_date)
    #print(i_to_date)

    currencies = "https://coinmarketcap.com/currencies/"
    currencies_end = '/historical-data/'
    dates = '?start={}&end={}'.format(i_from_date, i_to_date)
    # collect and parse coin historical page
    url = currencies + i_coin + currencies_end + dates
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    # Pull and append historic data
    table = soup.find('table')
    data = {
     'Date': [],
     'Open': [],
     'High': [],
     'Low': [],
     'Close': [],
     'Volume': [],
     'Market Cap': []
    }
    try:
        rows = table.findAll('tr')[1:]
        #print(rows)
        for row in rows:
            cols = row.findAll('td')
            data['Date'].append(cols[0].string)
            data['Open'].append(cols[1].string)
            data['High'].append(cols[2].string)
            data['Low'].append(cols[3].string)
            data['Close'].append(cols[4].string)
            data['Volume'].append(cols[5].string)
            data['Market Cap'].append(cols[6].string)
        coin_data = pd.DataFrame(data)
    except AttributeError as e:
        print('input parameters not valid')
        sys.exit(13)
    return coin_data


def  get_specific_coin_historical_data_cryptocompare(i_symbol, i_from_date, i_to_date):
    """
    :param str i_symbol: coin name
    :param date 'YYYYMMDD' i_from_date: pull data from this date [includes]
    :param date 'YYYYMMDD' i_to_date: pull data till this date [includes]
    return list: coin history data
    """

    print(i_symbol)

    data = {
     'Date': [],
     'Open': [],
     'High': [],
     'Low': [],
     'Close': [],
     'Volume': [],
     'Market Cap': []
    }

    if i_symbol == 'CENNZ':
        data['Date'].append(None)
        data['Open'].append(None)
        data['High'].append(None)
        data['Low'].append(None)
        data['Close'].append(None)
        data['Volume'].append(None)
        data['Market Cap'].append(None)
        coin_data = pd.DataFrame(data)
        return coin_data

    # last day == yesterday
    d=h.histo_day(i_symbol,'usd',limit=1)[-1]

    # show the date of the history (not the start of the next day
    # ie in cryptocompare the OHLCV values for Wednesday, December 4
    # have timestamp Wednesday, December 5, 2018 12:00:00 AM
    cor_date=datetime.fromtimestamp(d['time'])-timedelta(1)

    data['Date'].append(cor_date)
    data['Open'].append(d['open'])
    data['High'].append(d['high'])
    data['Low'].append(d['low'])
    data['Close'].append(d['close'])
    data['Volume'].append(d['volumeto'])
    data['Market Cap'].append(None)
    coin_data = pd.DataFrame(data)

    return coin_data


def get_from_to_dates(i_from_date, i_to_date):
    """
    :param str 'YYYY-MM-DD' i_from_date: pull data from this date [includes]
    :param str 'YYYY-MM-DD' i_to_date: pull data till this date [includes]
    :exception ValueError: date format is not as asked
    :return tuple: dates in format 'YYYYMMDD' - dates ready to be scrapped
    """
    try:
        if i_from_date is None:
            from_date = str(date.today() + timedelta(days=-30))
        else:
            from_date = i_from_date
        from_date = datetime.strptime(from_date, '%Y-%m-%d').strftime('%Y%m%d')
        if i_to_date is None:
            to_date = str(date.today() + timedelta(days=-1))
        else:
            to_date = i_to_date
        to_date = datetime.strptime(to_date, '%Y-%m-%d').strftime('%Y%m%d')
        return from_date, to_date
    except ValueError as e:
        print(e)
        sys.exit(13)


def is_coin_in_markets(i_coin, i_coin_markets_to_search):
    '''
    :param str i_coin: see if this coin available in following markets
    :param set i_coin_markets_to_search: markets set to search in
    :param int i_min_market_volume: minimum trading volume to a market
    :return boolean : True - if coin traded in one of the markets to search or market set is empty
                      False - coin isn't traded at the markets
    '''
    coin_in_markets = False
    coin_markets_url = 'https://coinmarketcap.com/currencies/{}/#markets'.format(i_coin)
    if not i_coin_markets_to_search:
        coin_in_markets = True
    else:
        # collect and parse coin historical page
        page = requests.get(coin_markets_url)
        soup = BeautifulSoup(page.text, 'html.parser')
        table = soup.find('table')
        rows = table.findAll('tr')[1:]
        #getting markets of coin
        markets = set()
        for row in rows:
            cols = row.findAll('td')
            if cols[1].text is not None:
                markets.add(cols[1].text.upper())
            for market in i_coin_markets_to_search:
                if market.upper() in markets:
                    coin_in_markets = True
                    break
    return  coin_in_markets

def waitToTomorrow():
    """Wait to tommorow 00:00 am"""

    tomorrow = datetime.replace(datetime.now() + timedelta(days=1),
                         hour=0, minute=0, second=0)
    delta = tomorrow - datetime.now()
    print('Sleep for: ')
    print(delta.seconds)
    time.sleep(delta.seconds)

#ta_features('2018-11-10','2018-11-17')
def daily_update():
    yesterday = date.today() - timedelta(1)
    str_yesterday = yesterday.strftime('%Y-%m-%d')
    df_coins = ta_features(str_yesterday)
    update_ta_db(df_coins);


if __name__ == "__main__":
    df_coins = ta_features('2010-01-01')

