from pycoingecko.pycoingecko import CoinGeckoAPI
from coinmetrics.coinmetrics import CoinMetricsAPI

from fta.fta_coin import social_features
from fta.fta_coin import block_features

from db import DB
import utils

import logging
import datetime
import time


def fta_features(num_coins = 10):

    # LOGGER
    #----------------------------------------------------
    logger = logging.getLogger('fta_features_listener')
    logger.setLevel(logging.DEBUG)

    # file handler logs debug msg
    fh = logging.FileHandler('fta.log')
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    #smtp_handler = logging.handlers.SMTPHandler(
    #        mailhost=("smtp.gmail.com", 465),
    #        fromaddr="manolis@pythagoras.systems",
    #        toaddrs="manolis@pythagoras.systems",
    #        subject=u"AppName error!")

    # add the handlers to logger
    logger.addHandler(fh)
    #logger.addHandler(smtp_handler)
    #-----------------------------------------------------


    cg = CoinGeckoAPI()
    cm = CoinMetricsAPI()

    # returns top 50 by default
    coins_ranked = cg.get_coins(order='market_cap_desc')
    coins_ranked = coins_ranked[0:num_coins]

    #print('Features for top ' + str(len(coins_ranked)) + ' coins (ranked by market cap)')
    logger.info('Features for top ' + str(len(coins_ranked)) + ' coins (ranked by market cap)')
    coin_symbols_ranked = []
    for coin in coins_ranked:
        coin_symbols_ranked.append(coin['symbol'])
        #print(coin['name'])
        #print(coin['id'])
        #print(coin['developer_data'])
        ##print('\n')
        ##print(coin['community_data'])
        ##print(coin['public_interest_stats'])
        #print(coin['last_updated'])

    while(1):
        # COIN METRICS
        cm_supported_assets = cm.get_supported_assets()
        cm_coins_features = {}
        # get all features for selected coins (supported by coinmetrics)
        for c in coins_ranked:
            if c['symbol'] in cm_supported_assets:
                #print(c['symbol'] + ' in CoinMerics')
                cm_coins_features.update(cm.get_all_data_types_for_assets(c['symbol']))
            else:
                #print('! ' + c['symbol'] + ' not in CoinMerics')
                logger.info('! ' + c['symbol'] + ' not in CoinMerics')
        # get all features for all assets supported by coinmetrics
        #cm_coin_features = cm.get_all_data_types_for_all_assets()

        ## not supported by cryptocompare
        #cm_supported_assets.remove('cennz')
        #cm_supported_assets.remove('ethos')
        #cm_supported_assets.remove('ven')
        #coins_block_features = block_features(cm_supported_assets)
        #coins_social_features = social_features(cm_supported_assets)

        coins_block_features = block_features(coin_symbols_ranked)
        coins_social_features = social_features(coin_symbols_ranked)

        # Databases
        # update coin list from CoinGecko
        coins_ranked = coinGecko_list_update(coins_ranked)

        db_coinmetrics(cm_coins_features)
        db_cryptocompare(coins_block_features)
        db_developer(coins_ranked)

        # Sleep
        t=datetime.datetime.now()
        t0=datetime.datetime(t.year, t.month, t.day, 12)
        t1=t0 + datetime.timedelta(days=1)

        logger.info('Sleep until ' + str(t1) + ' (' + str((t1-t0).total_seconds()) + ' seconds)')
        #logger.info('Sleep for ' + str((t1-t0).total_seconds()) + ' seconds!')

        time.sleep((t1-t0).total_seconds())


    return cm_coins_features, coins_block_features, coins_social_features


def coinGecko_list_update(coin_list):
    cg = CoinGeckoAPI()
    response = []
    for coin in coin_list:
        #print(coin)
        #print(coin['id'])
        response.append(cg.get_coin_by_id(coin['id']))

        #print(response[-1]['name'])
        #print(response[-1]['id'])
        #print(response[-1]['developer_data'])
        ##print('\n')
        ##print(coin['community_data'])
        ##print(coin['public_interest_stats'])
        #print(response[-1]['last_updated'])

    return response


def db_developer(coin_features, db = None):
    if not db:
        db = DB()
    db.connect()

    #coin_features = coinGecko_list_update(coin_features)
    for coin in coin_features:
        #print(coin['developer_data'])
        #print(coin['id'])
        ## last updated form '2018-09-12T14:51:38.396Z'
        #print(datetime.datetime.strptime(coin['last_updated'],'%Y-%m-%dT%H:%M:%S.%fZ'))

        Values = []

        db_query = 'INSERT INTO FtaDeveloper ('

        db_query += ' Symbol'
        Values.append(coin['symbol'])

        db_query += ', last_updated'
        if not isinstance(coin['last_updated'], datetime.datetime):
            Values.append(datetime.datetime.strptime(coin['last_updated'],'%Y-%m-%dT%H:%M:%S.%fZ'))
        else:
            Values.append(coin['last_updated'])

        db_query += ',' + ','.join(list(map(( lambda x: '[' + x + ']'), (coin['developer_data'].keys()))))
        Values.extend(list(coin['developer_data'].values()))

        db_query += ') VALUES '

        db_query += '(' + ','.join(['?' for x in coin['developer_data'].keys()]) + ',?,? ' + ')'

        #print(db_query)
        #print(Values)
        db.cnxn.execute(db_query, tuple(Values))
        db.cnxn.commit()

    db.disconnect()


def db_cryptocompare(coin_features, db = None):
    if not db:
        db = DB()
    db.connect()

    for coin in coin_features:
        #print(coin_features[coin])
        Values = []

        db_query = 'INSERT INTO FtaCryptoCompare ('

        db_query += ' Symbol'
        Values.append(coin)

        #db_query += ', LastBlockExplorerUpdateTS'
        if not isinstance(coin_features[coin]['LastBlockExplorerUpdateTS'], datetime.datetime):
            coin_features[coin]['LastBlockExplorerUpdateTS'] = datetime.datetime.fromtimestamp(coin_features[coin]['LastBlockExplorerUpdateTS'])

        db_query += ',' + ','.join(list(map(( lambda x: '[' + x + ']'), (coin_features[coin].keys()))))
        Values.extend(list(coin_features[coin].values()))

        db_query += ') VALUES '

        db_query += '(' + ','.join(['?' for x in coin_features[coin].keys()]) + ',? ' + ')'

        #print(db_query)
        #print(Values)
        db.cnxn.execute(db_query, tuple(Values))
        db.cnxn.commit()


def db_coinmetrics(coin_features, db = None):
    if not db:
        db = DB()
    db.connect()

    r = {}
    # fix Dic to format Dic[coin][timestamp][feature] instead of Dic[coin][feature][[timestamp,feature_value],[timestamp2, feature_value]]
    for coin in coin_features:
        r[coin] = {}
        for feat in coin_features[coin]:
            for feat_ts in coin_features[coin][feat]:
                if feat_ts[0] not in r[coin].keys():
                    r[coin][feat_ts[0]] = {}
                r[coin][feat_ts[0]][feat] = feat_ts[1]
        #print(coin)
        #print(coin_features[coin])
        #print(coin_features[coin].keys())

    for coin in r:
        for ts in r[coin]:
            Values = []
            db_query = 'INSERT INTO FtaCoinmetrics ('

            db_query += ' Symbol'
            Values.append(coin)

            db_query += ', CM_Timestamp'
            Values.append(datetime.datetime.fromtimestamp(ts))

            #db_query += ',' + ','.join(r[coin][ts].keys())
            db_query += ',' + ','.join(list(map(( lambda x: '[' + x + ']'), (r[coin][ts].keys()))))
            Values.extend(list(r[coin][ts].values()))

            db_query += ') VALUES '
            #list(map(( lambda x: '[' + x + ']'), list(r[coin][ts].keys())))
            db_query += '(' + ','.join(['?' for x in r[coin][ts].keys()]) + ',?,? ' + ')'


            #print(db_query)
            #print(Values)
            db.cnxn.execute(db_query, tuple(Values))
            db.cnxn.commit()

    db.disconnect()

    return r


def coinGeckoHistoricalDeveloper(coin_list, from_date):
    cg = CoinGeckoAPI()

    date = datetime.datetime.strptime(from_date, '%d-%m-%Y').date()
    #taday = datetime.date.today()

    r = []
    while date!=datetime.date.today():
        print(date)
        for coin in coin_list:
            responce = cg.get_coin_history_by_id(coin['id'], date.strftime('%d-%m-%Y'))
            responce['last_updated'] = datetime.datetime.combine(date, datetime.datetime.min.time())
            r.append(responce)
            #r.append(cg.get_coin_history_by_id(coin['id'], date.strftime('%d-%m-%Y')))

        date = date + datetime.timedelta(days=1)

    return r


def max_from_list(l, pos = 0):
    return max([i[pos] for i in l if i[pos] is not None])


def divide_list_by_max(l, pos = 0):
    return list(map(lambda x: (0 if x is None else x) / max_from_list(l,pos), [i[pos] for i in l]))


def find_description_position(description, feature_name):
    return [i for i,x in enumerate([d[0] for d in description]) if x == feature_name][0]

def block_explorer_features(db = None):
    if not db:
        #db = DB()
        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
        db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -3, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -2, GETDATE())')
    t0 = cursor.fetchall()
    print(len(t0))
    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -2, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -1, GETDATE())')
    t1 = cursor.fetchall()
    print(len(t1))

    # get position of symbol
    pos = find_description_position(cursor.description, 'Symbol')
    descr = []

    block_feats = {}
    for i in range(len(t1)):
        descr.append(t1[i][pos])
        block_feats[t1[i][pos]] = {}
    #block_feats['symbol'] = (descr)
    
    # get position of feature
    pos = find_description_position(cursor.description, 'blockcount')
    #block_feats['blockcount'] = divide_list_by_max(t1, pos)
    for i, v in enumerate(divide_list_by_max(t1, pos)):
        block_feats[descr[i]]['blockcount'] = v
   
    # get position of feature
    pos = find_description_position(cursor.description, 'txcount')
    #block_feats['txcount'] = divide_list_by_max(t1, pos)
    for i, v in enumerate(divide_list_by_max(t1, pos)):
        block_feats[descr[i]]['txcount'] = v

    # get positions of features
    pos_medianfee = find_description_position(cursor.description, 'medianfee')
    pos_activeaddresses = find_description_position(cursor.description, 'activeaddresses')
    medianfee = []
    activeaddresses = []
    for i in range(len(t1)):
        if (t0[i][pos_medianfee] and t1[i][pos_medianfee]):
            if (t1[i][pos_medianfee] <= t0[i][pos_medianfee]):
                #medianfee.append(1)
                block_feats[descr[i]]['medianfee'] = 1
            else:
                #medianfee.append(0)
                block_feats[descr[i]]['medianfee'] = 0
        else:
            #medianfee.append(0)
            block_feats[descr[i]]['medianfee'] = 0

        if (t0[i][pos_activeaddresses] and t1[i][pos_activeaddresses]):
            if (t1[i][pos_activeaddresses] >= t0[i][pos_activeaddresses]):
                #activeaddresses.append(1)
                block_feats[descr[i]]['activeaddresses'] = 1
            else:
                #activeaddresses.append(0)
                block_feats[descr[i]]['activeaddresses'] = 0
        else:
            #activeaddresses.append(0)
            block_feats[descr[i]]['activeaddresses'] = 0

    #block_feats['medianfee'] = medianfee
    #block_feats['activeaddresses'] = activeaddresses

    cursor.execute('select * from FtaCryptoCompare where LastBlockExplorerUpdateTS >=  DATEADD(DAY, -2, GETDATE()) AND LastBlockExplorerUpdateTS <  DATEADD(DAY, -1, GETDATE())')
    t1 = cursor.fetchall()

    descr = []
    pos = find_description_position(cursor.description, 'Symbol')
    for i in range(len(t1)):
        descr.append(t1[i][pos])

    pos = find_description_position(cursor.description, 'NetHashesPerSecond')
    #block_feats['hashrate'] = divide_list_by_max(t1, pos)
    for i, v in enumerate(divide_list_by_max(t1, pos)):
        if descr[i] not in block_feats.keys():
            block_feats[descr[i]] = {}
        block_feats[descr[i]]['hashrate'] = v

    return block_feats


def sentiment_analysis_features(db = None):
    if not db:
        #db = DB()
        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
        db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()   
    print('cursor execute') 
    cursor.execute('select top 500000 Symbol, SentimentScore, Timestamp from TweetsSearched WITH (NOLOCK) ORDER BY myid  DESC')
    print('cursor fetchall') 
    tweets = cursor.fetchall()

    print('filtering') 
    yesterday=datetime.date.today() - datetime.timedelta(1)
    yesterday = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)

    today = datetime.datetime.today()
    today = datetime.datetime(today.year, today.month, today.day)

    print('Tweets from ' + str(yesterday))
    print('Tweets until ' + str(today))
    # check that everything is OK!
    if not (tweets[0][-1] > today and tweets[-1][-1]  < yesterday):
        print('Fetch more tweets!!')

    filtered_tweets = {}
    for t in tweets:
        if t[-1] >= yesterday and t[-1] < today and (float(t[1]) < -0.5 or float(t[1]) > +0.5 ):
            if t[0].lower() not in filtered_tweets:
                filtered_tweets[t[0].lower()] = {}
                filtered_tweets[t[0].lower()]['twitter'] = {}
                filtered_tweets[t[0].lower()]['twitter']['total_score'] = 0
                filtered_tweets[t[0].lower()]['twitter']['num_of_tweets'] = 0
            # increase sum and number of tweets
            filtered_tweets[t[0].lower()]['twitter']['total_score'] += float(t[1])
            filtered_tweets[t[0].lower()]['twitter']['num_of_tweets'] += 1
    
    for coin in filtered_tweets:
        if filtered_tweets[coin]['twitter']['num_of_tweets'] > 0:
            filtered_tweets[coin]['twitter']['mean'] = filtered_tweets[coin]['twitter']['total_score'] / filtered_tweets[coin]['twitter']['num_of_tweets']
        else:
            filtered_tweets[coin]['twitter']['mean'] = 0
    #datetime.datetime(yesterday.year, yesterday.month, yesterday.day) <= res[i][-4] and (datetime.datetime(today.year, today.month, today.day) > res[i][-4])
    return filtered_tweets


def expert_analysis_features():
    scores_expert = {}
    with open('fta_expert_scores.txt', 'r') as f:
        for line in f:
            line=line.strip().split(',')
            # normalize values from [1,10] to [0.1,1]
            scores_expert[line[1].lower()] = int(line[0])/10
    return scores_expert


def developer_analysis_features(db = None):
    if not db:
        #db = DB()
        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
        db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()    

    cursor.execute('select Symbol, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks from FtaDeveloper where last_updated >=  DATEADD(DAY, -3, GETDATE()) AND last_updated <  DATEADD(DAY, -2, GETDATE())')
    R = cursor.fetchall()
    t0 = {}
    for r in R:
        if r[0] not in t0:
            t0[r[0]] = {}
        t0[r[0]]['forks'] = r[1]
        t0[r[0]]['stars'] = r[2]
        t0[r[0]]['subscribers'] = r[3]
        t0[r[0]]['total_issues'] = r[4]
        t0[r[0]]['closed_issues'] = r[5]
        t0[r[0]]['pull_requests_merged'] = r[6]
        t0[r[0]]['pull_request_contributors'] = r[7]
        t0[r[0]]['commit_count_4_weeks'] = r[8]

    cursor.execute('select Symbol, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks from FtaDeveloper where last_updated >=  DATEADD(DAY, -2, GETDATE()) AND last_updated <  DATEADD(DAY, -1, GETDATE())')
    R = cursor.fetchall()
    t1 = {}
    for r in R:
        if r[0] not in t1:
            t1[r[0]] = {}
        t1[r[0]]['forks'] = r[1]
        t1[r[0]]['stars'] = r[2]
        t1[r[0]]['subscribers'] = r[3]
        t1[r[0]]['total_issues'] = r[4]
        t1[r[0]]['closed_issues'] = r[5]
        t1[r[0]]['pull_requests_merged'] = r[6]
        t1[r[0]]['pull_request_contributors'] = r[7]
        t1[r[0]]['commit_count_4_weeks'] = r[8]   
    
    da_feats = {}
    for coin in t1:
        da_feats[coin] = {}

        da_feats[coin]['open_source'] = (1 if sum(t1[coin].values()) > 0 else -1)

        da_feats[coin]['commit_count_4_weeks'] = (1 if t1[coin]['commit_count_4_weeks'] > 0 else 0)

        if t1[coin]['total_issues'] > 0 and t0[coin]['total_issues'] > 0 and ((t1[coin]['closed_issues'] / ( t1[coin]['total_issues'] - t1[coin]['closed_issues'] )) >= (t0[coin]['closed_issues'] / ( t0[coin]['total_issues'] - t0[coin]['closed_issues'] ))):
            da_feats[coin]['issues_rate'] = 1
        else:
            da_feats[coin]['issues_rate'] = 0
        
        t1_fss = t1[coin]['forks'] + t1[coin]['stars'] + t1[coin]['subscribers']
        t0_fss = t0[coin]['forks'] + t0[coin]['stars'] + t0[coin]['subscribers']
        if t1_fss > 0 and t1_fss >= t0_fss:
            da_feats[coin]['forks_stars_subs'] = 1
        else:
            da_feats[coin]['forks_stars_subs'] = 0
        
        t1_pr = t1[coin]['pull_requests_merged'] + t1[coin]['pull_request_contributors']
        t0_pr = t0[coin]['pull_requests_merged'] + t0[coin]['pull_request_contributors']
        if t1_pr > 0 and t1_pr > t0_pr:
            da_feats[coin]['pull_requests'] = 1
        else:
            da_feats[coin]['pull_requests'] = 0

        #t1[5]/(t1[4] - t1[5])

    return da_feats


# Block Explorer Analysis (bea) scoring function
def bea_scoring_function(block_feats):
    score = {}
    for coin in block_feats:
        score[coin] = 0
        if 'blockcount' in block_feats[coin]:
            score[coin] += block_feats[coin]['blockcount'] * 20
        if 'txcount' in block_feats[coin]:
            score[coin] += block_feats[coin]['txcount'] * 20
        if 'medianfee' in block_feats[coin]:
            score[coin] += block_feats[coin]['medianfee'] * 20
        if 'activeaddresses' in block_feats[coin]:
            score[coin] += block_feats[coin]['activeaddresses'] * 20
        if 'hashrate' in block_feats[coin]:
            score[coin] += float(block_feats[coin]['hashrate']) * 20
    return score


# Sentiment Analysis (sa) scoring function
def sa_scoring_function(sa_feats):
    score = {}
    for coin in sa_feats:
        score[coin] = 0
        if 'twitter' in sa_feats[coin]:
            score[coin] += sa_feats[coin]['twitter']['mean'] * 100
    return score


# Expert Analysis (ea) scoring function
def ea_scoring_function(ea_feats):
    score = {}
    for coin in ea_feats:
        score[coin] = ea_feats[coin] * 100
    return score


def da_scoring_function(da_feats):
    score = {}
    for coin in da_feats:
        score[coin] = 0
        if 'open_source' in da_feats[coin]:
            #score[coin] += da_feats[coin]['open_source'] * 50
            #if da_feats[coin]['open_source'] == -1:
            #    score[coin] += -100
            #else:
            #    score[coin] += 50
            score[coin] += (-100 if da_feats[coin]['open_source'] == -1 else 50)
        if 'commit_count_4_weeks' in da_feats[coin]:
            score[coin] += da_feats[coin]['commit_count_4_weeks'] * 12.5
        if 'issues_rate' in da_feats[coin]:
            score[coin] += da_feats[coin]['issues_rate'] * 12.5
        if 'forks_stars_subs' in da_feats[coin]:
            score[coin] += da_feats[coin]['forks_stars_subs'] * 12.5
        if 'pull_requests' in da_feats[coin]:
            score[coin] += da_feats[coin]['pull_requests'] * 12.5
    return score


def scoring_function():

    total_analysis = {}

    bea_feats = block_explorer_features()
    bea_scores = bea_scoring_function(bea_feats)

    total_analysis['bea_feats'] = bea_feats
    total_analysis['bea_scores'] = bea_scores

    #for coin in bea_scores:
    #    print(coin + ', ' + str(bea_scores[coin]))

    ea_feats = expert_analysis_features()
    ea_scores = ea_scoring_function(ea_feats)


    total_analysis['ea_feats'] = ea_feats
    total_analysis['ea_scores'] = ea_scores

    #for coin in ea_scores:
    #    print(coin + ', ' + str(ea_scores[coin]))

    sa_feats = sentiment_analysis_features()
    sa_scores = sa_scoring_function(sa_feats)

    total_analysis['sa_feats'] = sa_feats
    total_analysis['sa_scores'] = sa_scores

    #for coin in sa_scores:
    #    print(coin + ', ' + str(sa_scores[coin]))
    
    da_feats = developer_analysis_features()
    da_scores = da_scoring_function(da_feats)

    total_analysis['da_feats'] = da_feats
    total_analysis['da_scores'] = da_scores

    # look only for the ones we have block explorer feats
    print('coin, bea, ea, sa, da')
    for coin in bea_scores:
        print(coin + ': ' + str(bea_scores[coin]) + ', ' + str(ea_scores[coin]) + ', ' + str(sa_scores[coin]) + ', ' + str(da_scores[coin]))
    
    total_score = {}
    for coin in bea_scores:
        total_score[coin] = 0.4 * ( 0.3 * bea_scores[coin] + 0.1 * ea_scores[coin] + 0.25 * da_scores[coin] ) + 0.2 * ( 1 * sa_scores[coin] )
    
    return total_score, total_analysis


if __name__ == "__main__":
#    while(1):
    cm_coins_features, coins_block_features, coins_social_features = fta_features()
#    t0 = datetime.datetime.fromtimestamp(coins_block_features['btc']['LastBlockExplorerUpdateTS'])
#    print(t0)
#    t1 = t0 + datetime.timedelta(0,(30*60)+5)
#    print('sleep for ' + str((t1-t0).seconds) + ' seconds')
#    time.sleep((t1-t0).seconds)

