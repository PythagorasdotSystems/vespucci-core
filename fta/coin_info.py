# not FTA (but example of other info we could use from cryptocompare)
from fta_coin import coin_full_snapshot

def coin_info(coin):
    info = coin_full_snapshot(coin)
    
    responce = {}
    responce['Technology'] = info[coin]['General']['Technology']
    responce['Features'] = info[coin]['General']['Features']
    responce['Twitter'] = info[coin]['General']['Twitter']
    #responce['Url'] = info[coin]['General']['Url']
    responce['Website'] = info[coin]['General']['Website']
    responce['WebsiteUrl'] = info[coin]['General']['WebsiteUrl']

    return responce


if __name__ == "__main__":
    info = coin_info('BTC')
    print(info)

