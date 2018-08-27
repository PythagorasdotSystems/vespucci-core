from crycompare import social as s
from crycompare import price as p
#from crycompare import history as h


def coin_full_snapshot(coin_names):
    if not isinstance(coin_names, list):
        # when only one coin
        coin_names = [coin_names]
    
    L = p.coin_list()

    response = {}
    for coin in coin_names:
        if coin.upper() in L['Data'].keys():
            response[coin] = p.coin_snapshot_id(L['Data'][coin.upper()]['Id'])['Data']
        else:
            print(coin + ' not in cryptocompare!')
    return response

def coin_features_from_snapshot(coins_full_snapshots):
    coins_features = {}
    for coin_snapshot in coins_full_snapshots:
        #print(coin_snapshot)
        features = {}
        features['BlockTime'] = coins_full_snapshots[coin_snapshot]['General']['BlockTime']
        features['BlockNumber'] = coins_full_snapshots[coin_snapshot]['General']['BlockNumber']
        features['DifficultyAdjustment'] = coins_full_snapshots[coin_snapshot]['General']['DifficultyAdjustment']
        features['BlockReward'] = coins_full_snapshots[coin_snapshot]['General']['BlockReward']
        features['BlockRewardReduction'] = coins_full_snapshots[coin_snapshot]['General']['BlockRewardReduction']
        features['TotalCoinSupply'] = coins_full_snapshots[coin_snapshot]['General']['TotalCoinSupply']
        features['NetHashesPerSecond'] = coins_full_snapshots[coin_snapshot]['General']['NetHashesPerSecond']
        features['TotalCoinsMined'] = coins_full_snapshots[coin_snapshot]['General']['TotalCoinsMined']
        features['PreviousTotalCoinsMined'] = coins_full_snapshots[coin_snapshot]['General']['PreviousTotalCoinsMined']
        features['LastBlockExplorerUpdateTS'] = coins_full_snapshots[coin_snapshot]['General']['LastBlockExplorerUpdateTS']
        coins_features[coin_snapshot] = features
    return coins_features


def block_features(coin_names):
    if not isinstance(coin_names, list):
        # when only one coin
        coin_names = [coin_names]
    
    coins_snapshots = coin_full_snapshot(coin_names)

    response = coin_features_from_snapshot(coins_snapshots)
    #for coin in coin_names:
    #    response[coin] = coin_full_snapshot 
        
    return response


def social_features(coin_names):
    if not isinstance(coin_names, list):
        # when only one coin
        coin_names = [coin_names]
    
    L = p.coin_list()

    response = {}
    for coin in coin_names:
        if coin.upper() in L['Data'].keys():
            #print(coin)
            response[coin] = s.social_stats(L['Data'][coin.upper()]['Id'])
        else:        
            print(coin + ' not in cryptocompare!')
        #print(response[coin]['CodeRepository']['List'][-1]['url'])
    return response

