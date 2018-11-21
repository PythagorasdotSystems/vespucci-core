import tweepy
from tweepy import OAuthHandler

import json
import time
import csv
import yaml

import requests

import logging
import logging.handlers
import traceback
import threading
import os

# DB
import pyodbc

# CoinMarketCap API
from coinmarketcap import Market

import sys
sys.path.append('..')
from coinmetrics import CoinMetricsAPI

# VADER
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class Coin:

    def __init__(self, coin):

        self.name = coin['name']
        self.symbol = coin['symbol']
        #self.rank = coin['rank']
        self.website_slug = coin['website_slug']

        self.last_tweet_id = None
        self.last_tweet_created_at = None

        self.numTweets = []

    def last_tweet(self, tweet):
        self.last_tweet_id = tweet.id
        self.last_tweet_created_at = tweet.created_at

def searchCoinTweets(api, coin):

    txt_query = ''

    if coin.symbol.lower() not in ['omg', 'snt', 'bts', 'fun', 'ode', 'pay', 'wax', 'hot', 'mith', 'gas', 'ela', 'poly', 'drop', 'nas', 'elf', 'link', 'aoa', 'mkr']: # all crypto except the ones with issues
        txt_query += str2hashtag(coin.symbol)
        if coin.symbol.lower() != coin.name.lower():
            txt_query += " OR " + str2hashtag(coin.name)
    elif coin.symbol.lower() in ['omg', 'bts', 'fun', 'ode', 'pay', 'hot', 'mith', 'ela', 'poly', 'drop', 'nas', 'elf', 'link']: # all the coins where only name should be used
        txt_query += str2hashtag(coin.name)
    elif coin.symbol.lower() not in ['wax', 'gas', 'aoa', 'mkr']:
        txt_query += str2hashtag(coin.symbol) # all the coins where only symbol should be used

    # if it isn't empty
    if txt_query:
        txt_query += ' OR '
    #txt_query += ' OR ' + str2dollartag(coin.symbol) + ' OR ' + str2dollartag(coin.name)
    txt_query += str2dollartag(coin.symbol)
    if coin.symbol.lower() != coin.name.lower():
        txt_query += ' OR ' + str2dollartag(coin.name)

    if coin.symbol.lower() not in ['snt']:
        txt_query += " OR ((" + ((coin.symbol) + " OR " + (coin.name) + " OR " + (coin.website_slug) + ") AND (cryptocurrency OR crypto OR blockchain))")

    if coin.last_tweet_id is None:
        L = tweepy.Cursor(api.search, q=txt_query, tweet_mode='extended', lang='en').items(1)#100)
    else:

        L = api.search(q=txt_query, tweet_mode='extended', lang='en', since_id=coin.last_tweet_id, count=1000)
        # if there are any tweets, search if there are more (until since_id is reached)
        if len(L) > 0:
            Ltmp = api.search(q=txt_query, tweet_mode='extended', lang='en', since_id=coin.last_tweet_id, max_id=L[-1].id, count=1000, exclude_replies = True)
            Ltmp = Ltmp[1:]

            while len(Ltmp)>50:
                # remove the first which is the max_id
                L.extend(Ltmp)
                Ltmp = api.search(q=txt_query, tweet_mode='extended', lang='en', since_id=coin.last_tweet_id, max_id=L[-1].id, count=1000, exclude_replies = True)
                Ltmp = Ltmp[1:]

    T = []
    # tweets will be inserted ordered by latest
    for it,t in enumerate(L):
        if it == 0:
            coin.last_tweet(t)
        T.append(t)

    coin.numTweets.append(len(T))

    lock.acquire()

    # VADER
    analyser = SentimentIntensityAnalyzer()

    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+usrname+';PWD='+ password)
    for t in T[::-1]:
        t.full_text = t.full_text.replace('\n', ' ').replace('\r', '')
        snt = analyser.polarity_scores(t.full_text)['compound']
        # insert to DB (wiil be improved)
        try:
            if 'retweeted_status' in t._json:
                cnxn.execute('INSERT INTO TweetsSearched(Symbol,userName,TweetText,description,location,retweetCount,TweetedBy,SentimentScore,followers,createdAt) VALUES (?,?,?,?,?,?,?,?,?,?)',
                        coin.symbol,
                        str_to_utf16le(t.user.name),
                        str_to_utf16le(t.full_text),
                        str_to_utf16le(t.user.description),
                        str_to_utf16le(t.user.location),
                        t.retweet_count,
                        str_to_utf16le(t.retweeted_status.user.name),
                        snt,
                        t.user.followers_count,
                        (t.created_at)
                        )
            else:
                cnxn.execute('INSERT INTO TweetsSearched(Symbol,userName,TweetText,description,location,retweetCount,SentimentScore,followers,createdAt) VALUES (?,?,?,?,?,?,?,?,?)',
                        coin.symbol,
                        str_to_utf16le(t.user.name),
                        str_to_utf16le(t.full_text),
                        str_to_utf16le(t.user.description),
                        str_to_utf16le(t.user.location),
                        t.retweet_count,
                        snt,
                        t.user.followers_count,
                        t.created_at
                        )
        except Exception as e:
                print(txt_query)
                logger.error('Error at txt_query: ' + str(txt_query))
                logger.error(str(t))
                logger.error(traceback.format_exc())
                os._exit(1)
    cnxn.commit()
    cnxn.close()

    lock.release()

def str_to_utf16le(s):
    if s is None:
        s = ' '
    return s.encode('utf_16_le')


def coinMarketCapRank(numCoins):
    # default rank returned is the market cap ranking with an api limit of 100 coins/request
    # TODO  there is an issue when iterating to get more than 100 coins
    #       -> rank might change during the iterations!
    #       Only id sorting is guaranteed to return a consistent order
    coin_ranked={}
    if numCoins <= 100:
        coin_ranked.update(coinmarketcap.ticker(limit=numCoins)['data'])
        return coin_ranked

    for i in range(0,int(numCoins/100)):
        coin_ranked.update(coinmarketcap.ticker(start=((100*i)+1), limit=100)['data'])
    if numCoins % 100 > 0:
        coin_ranked.update(coinmarketcap.ticker(start=((100*(i+1))+1), limit=(numCoins-len(coin_ranked)))['data'])
    return coin_ranked



def str2hashtag(txt):
    if len(txt.split()) == 1:
        return ('#'+txt)
    else:
        return '\"' + txt.replace(' ','_') + '\"'

def str2dollartag(txt):
    if len(txt.split()) == 1:
        return ('$'+txt)
    else:
        return '\"' + txt.replace(' ','') + '\"'


# LOGGER
#----------------------------------------------------
logger = logging.getLogger('twitter_searcher')
logger.setLevel(logging.DEBUG)

# file handler logs debug msg
fh = logging.FileHandler('../../searcher.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# add the handlers to logger
logger.addHandler(fh)
#logger.addHandler(smtp_handler)
#-----------------------------------------------------


with open('../../config.yml', 'r') as f:
        config = yaml.load(f)


# DATABASE
#-----------------------------------------------------
server = config['db']['server']
database = config['db']['database']
usrname = config['db']['username']
password = config['db']['password']

driver = config['db']['driver']
#-----------------------------------------------------

# TWITTER
#-----------------------------------------------------
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']
consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']
#-----------------------------------------------------

# App auth
auth=tweepy.AppAuthHandler(consumer_key, consumer_secret)

#numCoins = 100
searchers_sleep_time = 15*60#15*60

thread_timeout = 5*60 # max time to wait for each thread to finish

coinmarketcap = Market()


listing = coinmarketcap.listings()
cm = CoinMetricsAPI()
cm_coins = cm.get_supported_assets()
cm_coins = [x.lower() for x in cm_coins]

listing_dict={}
for coin in listing['data']:
    listing_dict[coin['symbol'].lower()] = coin

coins = []
for c in cm_coins:
    # special case
    if c == 'ven': c = 'vet'
    if c in listing_dict:
        coins.append(Coin(listing_dict[c]))
        logger.info('Start searcher for ' + listing_dict[c]['name'] + ', ' + listing_dict[c]['symbol'] + ', ' + listing_dict[c]['website_slug'])
    else:
        logger.info('\t' + c + ':: Coin not in CoinMarket')
numCoins = len(coins)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

if (api.rate_limit_status()['resources']['search']['/search/tweets']['remaining'] < numCoins) and ((api.rate_limit_status()['resources']['search'])['/search/tweets']['reset'] - time.time() > 0):
        print('Sleep from previous rate limit for ' + str((((api.rate_limit_status()['resources']['search'])['/search/tweets']['reset'] - time.time()) + 5)/60) + ' minutes')
        time.sleep(((api.rate_limit_status()['resources']['search'])['/search/tweets']['reset'] - time.time()) + 5)

while(1):

    logger.info('Searchers started collecting tweets!')

    logger.info('Start search: ' + str(api.rate_limit_status()['resources']['search']))

    threads = []
    lock = threading.Lock()
    for c in coins:
        thread = threading.Thread(target=searchCoinTweets, args=[api, c], name=c.name)
        threads.append(thread)
        thread.start()

    # wait for all threads to finish
    for thrd in threads:
        thrd.join(thread_timeout)
        if thrd.is_alive():
            logger.error('Threads not finish yet! Name: ' + str(thrd.name))

    if threading.active_count() > 1:
        print('Threads not finished yet! Total threads:: ' + str(threading.active_count()))

    logger.info('End search: ' + str(api.rate_limit_status()['resources']['search']))

    # ping healthcheck
    requests.get(config['healthchecks']['sa']['twitter'])

    # sleep for some minutes
    searchers_sleep_time =(api.rate_limit_status()['resources']['search']['/search/tweets']['reset'] - time.time()) + 5
    if searchers_sleep_time > 0:
        logger.info('Searchers sleep time, for ' + str(searchers_sleep_time/60) + ' minutes!')
        time.sleep(searchers_sleep_time)

