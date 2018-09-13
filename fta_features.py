from pycoingecko.pycoingecko import CoinGeckoAPI
from coinmetrics.coinmetrics import CoinMetricsAPI

from fta.fta_coin import social_features
from fta.fta_coin import block_features

from db import DB

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

    print('Features for top ' + str(len(coins_ranked)) + ' coins (ranked by market cap)')
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
                print('! ' + c['symbol'] + ' not in CoinMerics')
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
1

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

    coin_features = coinGecko_list_update(coin_features)

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
        Values.append(datetime.datetime.strptime(coin['last_updated'],'%Y-%m-%dT%H:%M:%S.%fZ'))

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
        print(coin_features[coin])
        Values = []

        db_query = 'INSERT INTO FtaCryptoCompare ('

        db_query += ' Symbol'
        Values.append(coin)

        #db_query += ', LastBlockExplorerUpdateTS'
        if not isinstance(coins_block_features[coin]['LastBlockExplorerUpdateTS'], datetime.datetime):
            coin_features[coin]['LastBlockExplorerUpdateTS'] = datetime.datetime.fromtimestamp(coin_features[coin]['LastBlockExplorerUpdateTS'])

        db_query += ',' + ','.join(list(map(( lambda x: '[' + x + ']'), (coins_block_features[coin].keys()))))
        Values.extend(list(coins_block_features[coin].values()))

        db_query += ') VALUES '

        db_query += '(' + ','.join(['?' for x in coins_block_features[coin].keys()]) + ',? ' + ')'

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
    taday = datetime.date.today()

    r = []
    while date!=datetime.date.today():
        print(date)
        for coin in coin_list:
            r.append(cg.get_coin_history_by_id(coin['id'], date.strftime('%d-%m-%Y')))

        date = date + datetime.timedelta(days=1)

    return r


if __name__ == "__main__":
#    while(1):
    cm_coins_features, coins_block_features, coins_social_features = fta_features()
#    t0 = datetime.datetime.fromtimestamp(coins_block_features['btc']['LastBlockExplorerUpdateTS'])
#    print(t0)
#    t1 = t0 + datetime.timedelta(0,(30*60)+5)
#    print('sleep for ' + str((t1-t0).seconds) + ' seconds')
#    time.sleep((t1-t0).seconds)

