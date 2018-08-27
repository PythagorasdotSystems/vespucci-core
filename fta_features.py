from pycoingecko.pycoingecko import CoinGeckoAPI
from coinmetrics.coinmetrics import CoinMetricsAPI

from fta.fta_coin import social_features
from fta.fta_coin import block_features


cg = CoinGeckoAPI()
cm = CoinMetricsAPI()

# returns top 50 by default
coins_ranked = cg.get_coins(order='market_cap_desc')
coins_ranked = coins_ranked[0:10]

print(len(coins_ranked))
coin_symbols_ranked = []
for coin in coins_ranked:
    coin_symbols_ranked.append(coin['symbol'])
    print(coin['name'])
    print(coin['developer_data'])
    print(coin['community_data'])
    print(coin['public_interest_stats'])
    print(coin['last_updated'])

coins_social_stats = {}
coins_blockexplorer_stats = {}
#for coin in coin_list:
#    #coins_social_stats[coin['symbol']] = coin_social_stats(coin['symbol'])
#    coins_blockexplorer_stats[coin['symbol']] = coin_blockexplorer_stats(coin['symbol'])

## COIN METRICS
##coins_blockexplorer_stats = coin_blockexplorer_stats(coin_symbols_ranked)
#cm_supported_assets = cm.get_supported_assets()
#
#for c in coins_ranked:
#    if c['symbol'] in cm_supported_assets:
#        print(c['symbol'] + ' in CM.')
#    else:
#        print('! ' + c['symbol'] + ' not in CM.')
#
## not supported by cryptocompare
#cm_supported_assets.remove('cennz')
#cm_supported_assets.remove('ethos')
#cm_supported_assets.remove('ven')
#coins_block_features = block_features(cm_supported_assets)
#coins_social_features = social_features(cm_supported_assets)

coins_block_features = block_features(coin_symbols_ranked)
coins_social_features = social_features(coin_symbols_ranked)

