import pycoingecko
import coinmetrics

from .fta_coin import social_features
from .fta_coin import block_features

from db import DB
import utils

import logging
import datetime
import time

#import score

def fta_features():

    logger = utils.logger_default('fta_features_listener', '../../fta.log')

    logger.info('Starting (Update FTA feats and compute score')

    # Vespucci coin list
    coin_list = utils.tools.vespucci_coin_list()
    coin_list =[coin['Symbol'].lower() for coin in coin_list]

    # init APIs
    cg = pycoingecko.CoinGeckoAPI()
    cm = coinmetrics.CoinMetricsAPI()

    logger.info('Get FTA feats')


    # CoinMetrics API
    ##cm_supported_assets = cm.get_supported_assets()
    #cm_supported_assets = cm.get_supported_assets()
    #cm_supported_assets = ['vet' if coin=='ven' else coin for coin in cm_supported_assets]

    #cm_coins_features = cm.get_all_data_types_for_all_assets()
    #cm_coins_features['vet'] = cm_coins_features.pop('ven')

    cm_supported_assets = cm.get_supported_assets()
    cm_supported_assets = ['vet' if coin=='ven' else coin for coin in cm_supported_assets]
    #cm_coins_features = cm.get_all_data_types_for_assets( ['ven' if coin['Symbol'].lower() == 'vet' else coin['Symbol'].lower() for coin in coin_list if coin['Symbol'].lower() in cm_supported_assets] )
    cm_coins_features = cm.get_all_data_types_for_assets(['ven' if coin.lower() == 'vet' else coin.lower() for coin in coin_list if coin.lower() in cm_supported_assets])
    cm_coins_features['vet'] = cm_coins_features.pop('ven')


    # CryptoCompare API
    #coins_block_features = block_features(cm_supported_assets)
    #coins_social_features = social_features(cm_supported_assets)

    coins_block_features = block_features(coin_list)
    coins_social_features = social_features(coin_list)


    # CoinGecko API
    #coins_dev_features = coinGecko_developer_update(cg, cm_supported_assets, cg.get_coins_list())
    #coins_dev_features = coinGecko_list_update(coins_dev_features)

    coins_dev_features = coinGecko_developer_update(cg, coin_list, cg.get_coins_list())
    coins_dev_features = coinGecko_list_update(coins_dev_features)

    return cm_coins_features, coins_block_features, coins_dev_features, coins_social_features


def update_fta_db(cm_coins_features, coins_block_features, coins_dev_features):
    #logger.info('Update DB with FTA feats')
    db_coinmetrics(cm_coins_features)
    db_cryptocompare(coins_block_features)
    db_developer(coins_dev_features)


def update():
    cm_coins_features, coins_block_features, coins_dev_features, coins_social_features = fta_features()
    update_fta_db(cm_coins_features, coins_block_features, coins_dev_features)


def coinGecko_developer_update(cg, cm_assets, cg_list):
    #cg_list = cg.get_coins_list()
    #cm_assets = cm.get_supported_assets()

    response = []
    
    # there are some duplicates also !! (keep them here to check)
    coin_seen  = {}

    for coin in cg_list:
        if coin['symbol'].lower() in cm_assets:

            if coin['symbol'].lower() == 'btg' and coin['id'].lower() != 'bitcoin-gold':
                continue
            if coin['symbol'].lower() == 'bat' and coin['id'].lower() != 'basic-attention-token':
                continue
            if coin['symbol'].lower() == 'cvc' and coin['id'].lower() != 'civic':
                continue
            if coin['symbol'].lower() == 'pax' and coin['id'].lower() != 'paxos-standard':
                continue
            if coin['symbol'].lower() == 'fun' and coin['id'].lower() != 'funfair':
                continue

            if coin['symbol'] in coin_seen:
                print('---Dupicates (same symbol)---')
                print(coin['symbol'])
                print(coin['id'])
                print('---was---')
                print(coin_seen[coin['symbol']])
                print('------------------------\n')
                continue
            else:
                print(coin['symbol'], ' ', coin['id'])
                coin_seen[coin['symbol']] = coin['id']

            response.append(cg.get_coin_by_id(coin['id']))

    return response


def coinGecko_list_update(coin_list):
    cg = pycoingecko.CoinGeckoAPI()
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


def db_developer(coin_features):
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


def db_cryptocompare(coin_features):
    db = DB()
    db.connect()

    for coin in coin_features:
        #print(coin_features[coin])
        Values = []

        coin_features[coin]['TotalCoinSupply'] = (0.0 if coin_features[coin]['TotalCoinSupply'] is '' else coin_features[coin]['TotalCoinSupply'])
        coin_features[coin]['TotalCoinsMined'] = (0.0 if coin_features[coin]['TotalCoinsMined'] is '' else coin_features[coin]['TotalCoinsMined'])
        coin_features[coin]['BlockTime'] = (0.0 if coin_features[coin]['BlockTime'] is '' else coin_features[coin]['BlockTime'])
        coin_features[coin]['BlockNumber'] = (0.0 if coin_features[coin]['BlockNumber'] is '' else coin_features[coin]['BlockNumber'])
        coin_features[coin]['BlockReward'] = (0.0 if coin_features[coin]['BlockReward'] is '' else coin_features[coin]['BlockReward'])
        coin_features[coin]['BlockRewardReduction'] = (0.0 if coin_features[coin]['BlockRewardReduction'] is '' else coin_features[coin]['BlockRewardReduction'])
        coin_features[coin]['NetHashesPerSecond'] = (0.0 if coin_features[coin]['NetHashesPerSecond'] is '' else coin_features[coin]['NetHashesPerSecond'])
        coin_features[coin]['PreviousTotalCoinsMined'] = (0.0 if coin_features[coin]['PreviousTotalCoinsMined'] is '' else coin_features[coin]['PreviousTotalCoinsMined'])
        coin_features[coin]['LastBlockExplorerUpdateTS'] = (int(datetime.datetime.now().timestamp()) if coin_features[coin]['LastBlockExplorerUpdateTS'] is '' else coin_features[coin]['LastBlockExplorerUpdateTS'])


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


def db_coinmetrics(coin_features):
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


def coinGeckoHistoricalDeveloper(coin_list, from_date, until_date = datetime.date.today().strftime('%d-%m-%Y')):
    cg = CoinGeckoAPI()

    date = datetime.datetime.strptime(from_date, '%d-%m-%Y').date()
    until_date = datetime.datetime.strptime(until_date, '%d-%m-%Y').date()
    #taday = datetime.date.today()

    r = []
    while date <= until_date:
        print(date)
        for coin in coin_list:
            responce = cg.get_coin_history_by_id(coin['id'], date.strftime('%d-%m-%Y'))
            responce['last_updated'] = datetime.datetime.combine(date, datetime.datetime.min.time())
            r.append(responce)
            #r.append(cg.get_coin_history_by_id(coin['id'], date.strftime('%d-%m-%Y')))

        date = date + datetime.timedelta(days=1)

    return r


if __name__ == "__main__":

    cm_coins_features, coins_block_features, coins_dev_features, coins_social_features = fta_features()
