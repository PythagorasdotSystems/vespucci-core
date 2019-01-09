import utils

import datetime
import time

# Sentiment Analysis features
def sa_features(sel_date = datetime.date.today(), config = None):

    if not config: config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()   

    print('Sentiment Analysis for', sel_date, '(fetching tweets from:', sel_date - datetime.timedelta(1),')')
    if sel_date == datetime.date.today():
        # selected date is today!
        cursor.execute('select top 500000 Symbol, SentimentScore, Timestamp from TweetsSearched WITH (NOLOCK) ORDER BY myid  DESC')
    else:
        today = sel_date
        today = datetime.datetime(today.year,today.month,today.day)

        yesterday = today - datetime.timedelta(1)
        yesterday = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)

        print('SA:', today)
        print('SA:', yesterday)

        cursor.execute('select Symbol, SentimentScore, Timestamp from TweetsSearched WITH (NOLOCK) WHERE Timestamp >= ? AND Timestamp < ?', yesterday, today)

    tweets = cursor.fetchall()

    if sel_date == datetime.date.today():
        print('filtering')
        yesterday = datetime.date.today() - datetime.timedelta(1)
        yesterday = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)

        today = datetime.datetime.today()
        today = datetime.datetime(today.year, today.month, today.day)

        print('Tweets from ' + str(yesterday))
        print('Tweets until ' + str(today))
        # check that everything is OK!
        if not (tweets[0][-1] > today and tweets[-1][-1]  < yesterday):
            print('Fetch more tweets!!')
    else:
        yesterday = sel_date - datetime.timedelta(1)
        yesterday = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)

    filtered_tweets = {}
    for t in tweets:
        if t[-1] >= yesterday and t[-1] < today:# and (float(t[1]) < -0.5 or float(t[1]) > +0.5 ):
            if t[0].lower() not in filtered_tweets:
                filtered_tweets[t[0].lower()] = {}
                filtered_tweets[t[0].lower()]['twitter'] = {}
                filtered_tweets[t[0].lower()]['twitter']['total_score'] = 0
                filtered_tweets[t[0].lower()]['twitter']['num_of_tweets'] = 0
                filtered_tweets[t[0].lower()]['twitter']['num_of_total_tweets'] = 0

            # filter "neutral" tweets
            if (float(t[1]) < -0.5 or float(t[1]) > +0.5 ):
                # increase sum and number of tweets
                filtered_tweets[t[0].lower()]['twitter']['total_score'] += float(t[1])
                filtered_tweets[t[0].lower()]['twitter']['num_of_tweets'] += 1

            # count total number of tweets (with "neutrals")
            filtered_tweets[t[0].lower()]['twitter']['num_of_total_tweets'] += 1
    
    for coin in filtered_tweets:
        if filtered_tweets[coin]['twitter']['num_of_tweets'] > 0:
            filtered_tweets[coin]['twitter']['mean'] = filtered_tweets[coin]['twitter']['total_score'] / filtered_tweets[coin]['twitter']['num_of_tweets']
        else:
            filtered_tweets[coin]['twitter']['mean'] = 0
    #datetime.datetime(yesterday.year, yesterday.month, yesterday.day) <= res[i][-4] and (datetime.datetime(today.year, today.month, today.day) > res[i][-4])
    return filtered_tweets


# Sentiment Analysis (sa) scoring function
def sa_scoring_function(sa_feats):
    score = {}
    for coin in sa_feats:
        score[coin] = 0
        if 'twitter' in sa_feats[coin]:
            score[coin] += sa_feats[coin]['twitter']['mean'] * 100
    return score


def sa_scores(sel_date = datetime.date.today(), config = None):
    feats = sa_features(sel_date, config)
    scores = sa_scoring_function(feats)
    return scores, feats

if __name__ == "__main__":
    scores, feats = sa_scores()

