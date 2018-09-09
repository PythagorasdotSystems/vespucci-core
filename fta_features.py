from pycoingecko.pycoingecko import CoinGeckoAPI
from coinmetrics.coinmetrics import CoinMetricsAPI

from fta.fta_coin import social_features
from fta.fta_coin import block_features

import datetime
import time

def fta_features(num_coins = 10):
    cg = CoinGeckoAPI()
    cm = CoinMetricsAPI()

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

    return coins_block_features, coins_social_features

def db_coinmetrics(coin_features):
    pass

if __name__ == "__main__":
    while(1):
        coins_block_features, coins_social_features = fta_features()
        t0 = datetime.datetime.fromtimestamp(coins_block_features['btc']['LastBlockExplorerUpdateTS'])
        print(t0)
        t1 = t0 + datetime.timedelta(0,(30*60)+5)
        print('sleep for ' + str((t1-t0).seconds) + ' seconds')
        time.sleep((t1-t0).seconds)

