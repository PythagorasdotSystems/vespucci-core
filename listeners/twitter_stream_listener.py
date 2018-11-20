import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#from tweepy import Stream, StreamListener, OAuthHandler
import json
import time
import logging
import csv
import yaml

import traceback

# VADER
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# DB
import pyodbc


class Listener(StreamListener):

    def __init__(self, symbol, api=None, time_limit=60):
        # start listening + connect to DB
        self.cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+usrname+';PWD='+ password)
        #self.cnxn.setencoding(encoding='utf-16le')

        self.symbol = symbol

        #self.time = time.time()
        #self.limit = time_limit

    def on_data(self, data):
        #while (time.time() - self.time) < self.limit:
        data = json.loads(data)

        if 'extended_tweet' in data:
            tweet_txt = data['extended_tweet']['full_text']
        else:
            tweet_txt = data['text']
        tweet_txt.replace('\n', ' ').replace('\r', '')

        snt = analyser.polarity_scores(tweet_txt)

        #row = [data['user']['name'], tweet_txt, data['retweet_count'],
        #        data["created_at"], data['user']['followers_count'],
        #        data['user']['friends_count'], snt['compound']]
        #print(tweet_txt)
        #with open(fname, 'a') as f:
        #    writer = csv.writer(f)
        #    writer.writerow(row)

        # insert to Test db
        self.cnxn.execute('INSERT INTO Test(Symbol,userName,TweetText,location,ScoreA) VALUES (?,?,?,?,?)',
                self.symbol,
                self._str_to_utf16le(data['user']['name']),
                self._str_to_utf16le(tweet_txt),
                self._str_to_utf16le(data['user']['location']),
                float(snt['compound']))
        self.cnxn.commit()

        return True

        # if time to sleep -> close DB
        #self.cnxn.commit()
        #self.cnxn.close()
        #return False

    def on_error(self, status):
        logger.error('Listener: on_error' + " " + str(status))
        #print('on_error::' + str(status))
        self.cnxn.commit()
        self.cnxn.close()
        return False

    def _str_to_utf16le(self, s):
        if s is None:
            s = ' '
        return s.encode('utf_16_le')

# LOGGER
#----------------------------------------------------
logger = logging.getLogger('twitter_listener')
logger.setLevel(logging.DEBUG)

# file handler logs debug msg
fh = logging.FileHandler('listener.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# add the handlers to logger
logger.addHandler(fh)
#-----------------------------------------------------


with open('config.yml', 'r') as f:
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

fname = 'out/tweets_crypto_' + time.strftime("%Y%m%d-%H%M") + '.csv'


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

logger.info('Start listener for bitcoin')

#with open(fname, 'w') as f:
#    writer = csv.writer(f)
#    writer.writerow(["User", "Text", "Retweets","Created_at","Followers_count","Friends_count","VADER"])

analyser = SentimentIntensityAnalyzer()

while(1):
    # example:  continuous stream for BTC tweets, compute sentiment score for each tweet using VADER and insert result to DB
    #           if any error sleep 15' and try again
    try:
        logger.info('Listening BTC...')
        l = Listener(symbol = 'BTC')
        stream = Stream(auth, l)
        stream.filter(track=['bitcoin'], languages=['en'])
    except Exception as e:
        logger.error('Listening error -> sleep, ' + traceback.format_exc())
    time.sleep(60*15)
