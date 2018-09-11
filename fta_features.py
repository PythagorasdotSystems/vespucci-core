from pycoingecko.pycoingecko import CoinGeckoAPI
from coinmetrics.coinmetrics import CoinMetricsAPI

from fta.fta_coin import social_features
from fta.fta_coin import block_features

from db import DB

import datetime
import time

def fta_features(num_coins = 10):
    cg = CoinGeckoAPI()
    cm = CoinMetricsAPI()

    db = DB()

    # returns top 50 by default
    coins_ranked = cg.get_coins(order='market_cap_desc')
    coins_ranked = coins_ranked[0:num_coins]

    print('Features for top ' + str(len(coins_ranked)) + ' coins (ranked by market cap)')
    coin_symbols_ranked = []
    for coin in coins_ranked:
        coin_symbols_ranked.append(coin['symbol'])
        #print(coin['name'])
        #print(coin['developer_data'])
        #print('\n')
        #print(coin['community_data'])
        #print(coin['public_interest_stats'])
        #print(coin['last_updated'])


    # COIN METRICS
    cm_supported_assets = cm.get_supported_assets()
    cm_coins_features = {}
    # get all features for selected coins (supported by coinmetrics)
    for c in coins_ranked:
        if c['symbol'] in cm_supported_assets:
            print(c['symbol'] + ' in CoinMerics')
            cm_coins_features.update(cm.get_all_data_types_for_assets(c['symbol']))
        else:
            print('! ' + c['symbol'] + ' not in CoinMerics')
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

    return cm_coins_features, coins_block_features, coins_social_features

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


if __name__ == "__main__":
#    while(1):
    cm_coins_features, coins_block_features, coins_social_features = fta_features()
#    t0 = datetime.datetime.fromtimestamp(coins_block_features['btc']['LastBlockExplorerUpdateTS'])
#    print(t0)
#    t1 = t0 + datetime.timedelta(0,(30*60)+5)
#    print('sleep for ' + str((t1-t0).seconds) + ' seconds')
#    time.sleep((t1-t0).seconds)

