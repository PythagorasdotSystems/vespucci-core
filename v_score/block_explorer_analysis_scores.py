import utils

import datetime


#def max_from_list_of_lists(l, pos = 0):
#    return max([i[pos] for i in l if i[pos] is not None])

#def max_from_list(l):
#    return max([i for i in l if i is not None])

#def divide_list_by_max(l, pos = 0):
#    return list(map(lambda x: (0 if x is None else x) / max_from_list_of_lists(l,pos), [i[pos] for i in l]))

#def divide_dict_by_max(d, pos):
#    print(pos)
#    max_value = max_from_list([v[pos] for v in d.values()  if (len(v) >=5)  ])
#    print(max_value)
#    results = {}
#    for k, v in d.items():
#        #print(v[pos])
#        if max_value != 0 and len(v) >=5 and v[pos] != None:
#            results[k] = v[pos] / max_value
#        else:
#            results[k] = 0
#    return results


def divide_dict_by_max(d, feat_name):
    #print(d)
    L=[x[feat_name] for x in d.values() if feat_name in x and x[feat_name] is not None]
    max_value=max(L) if L else 0

    results = {}
    for k, v in d.items():
        if max_value != 0 and feat_name in v and v[feat_name] is not None:
            results[k] = v[feat_name] / max_value
        else:
            results[k] = 0
    return results


#def find_description_position(description, feature_name):
#    return [i for i,x in enumerate([d[0] for d in description]) if x == feature_name][0]


def select_coin_bea_by_date(coin, sel_date = datetime.date.today(), config = None):
    from_date = datetime.datetime(sel_date.year, sel_date.month, sel_date.day)
    until_date = sel_date - datetime.timedelta(10)

    if not config: config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    # fetch the two newest for the specified date range
    cursor.execute('SELECT TOP 2 Symbol, blockcount, txcount, medianfee, activeaddresses, CM_Timestamp FROM FtaCoinmetrics WHERE Symbol = ? AND CM_Timestamp >= ?  AND CM_Timestamp < ? ORDER BY CM_Timestamp desc', coin, until_date, from_date)
    R = cursor.fetchall()

    T = []
    if len(R) != 2:
        print('Only', len(R), 'BEA features for', coin, 'at', sel_date,'.')
        #return None
    else:
        for r in R:
            d = {}
            d[r[0]] = {}
            d[r[0]]['blockcount'] = r[1]
            d[r[0]]['txcount'] = r[2]
            d[r[0]]['medianfee'] = r[3]
            d[r[0]]['activeaddresses'] = r[4]
            d[r[0]]['CM_Timestamp'] = r[5]
            T.append(d)

    # reverse to have older date first
    T = T[::-1]

    cursor.execute('SELECT TOP 1 Symbol, NetHashesPerSecond, LastBlockExplorerUpdateTS FROM FtaCryptoCompare WHERE Symbol = ? AND LastBlockExplorerUpdateTS >= ? AND LastBlockExplorerUpdateTS <  ? ORDER BY LastBlockExplorerUpdateTS desc', coin, until_date, from_date)
    R = cursor.fetchall()
    if R:
        r = R[0]

        if not len(T):
            d = {}
            d[r[0]] = {}
            d[r[0]]['NetHashesPerSecond'] = r[1]
            T = [d]
            #print(T)
            #return T
        else:
            T[1][r[0]]['NetHashesPerSecond'] = r[1]

    #return T[0], T[1]
    return T

def select_bea_by_date(sel_date, config = None):
    sel_date = datetime.datetime(sel_date.year, sel_date.month, sel_date.day)
    prev_sel_date = sel_date - datetime.timedelta(1)
    #print(sel_date)
    #print(prev_sel_date)

    if not config: config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    cursor.execute('SELECT Symbol, blockcount, txcount, medianfee, activeaddresses FROM FtaCoinmetrics WHERE CM_Timestamp >= ?  AND CM_Timestamp < ? ORDER BY CM_Timestamp', prev_sel_date, sel_date)
    R = cursor.fetchall()
    #pos = find_description_position(cursor.description, 'Symbol')
    d = {}
    for r in R:
        #d[r[pos]] = list(r)
        if r[0] not in d:
            d[r[0]] = {}
        d[r[0]]['blockcount'] = r[1]
        d[r[0]]['txcount'] = r[2]
        d[r[0]]['medianfee'] = r[3]
        d[r[0]]['activeaddresses'] = r[4]

    print('BEA feats:: length ', len(R))

    cursor.execute('SELECT Symbol, NetHashesPerSecond FROM FtaCryptoCompare WHERE LastBlockExplorerUpdateTS >= ? AND LastBlockExplorerUpdateTS <  ?', prev_sel_date, sel_date)
    R = cursor.fetchall()
    for r in R:
        if r[0] not in d:
            d[r[0]] = {}
        d[r[0]]['NetHashesPerSecond'] = r[1]

    return d

# Block Explorer Analysis features
def bea_features(sel_date = datetime.date.today(), coin_list = None, config = None):

    #prev_sel_date = sel_date - datetime.timedelta(1)
    #t0 = select_bea_by_date(prev_sel_date)
    #t1 = select_bea_by_date(sel_date)

    if not coin_list:
        coin_list = utils.tools.vespucci_coin_list(config)
        coin_list =[coin['Symbol'].lower() for coin in coin_list]

    t0 = {}
    t1 = {}
    for coin in coin_list:
        R = select_coin_bea_by_date(coin, sel_date, config)
        if len(R) == 1:
            t1.update(R[0])
        elif len(R) == 2:
            t0.update(R[0])
            t1.update(R[1])

    print('BEA feats t0:: length ', len(t0))
    print('BEA feats t1:: length ', len(t1))

    block_feats = compute_bea_scores(t0, t1)

    return block_feats


def compute_bea_scores(t0, t1):
    block_feats = {}
    for coin in t1:
        block_feats[coin] = {}

    for coin, v in divide_dict_by_max(t1, 'blockcount').items():
        block_feats[coin]['blockcount'] = v

    for coin, v in divide_dict_by_max(t1, 'txcount').items():
        block_feats[coin]['txcount'] = v

    for coin in t1:
        if coin not in t0:
            continue

        if 'medianfee' in t0 and t0[coin]['medianfee'] and 'medianfee' in t1 and t1[coin]['medianfee']:
        #if (t0[i][pos_medianfee] and t1[i][pos_medianfee]):
            if (t1[coin]['medianfee'] <= t0[coin]['medianfee']):
                #medianfee.append(1)
                block_feats[coin]['medianfee'] = 1
            else:
                #medianfee.append(0)
                block_feats[coin]['medianfee'] = 0
        else:
            #medianfee.append(0)
            block_feats[coin]['medianfee'] = 0

        if 'activeaddresses' in t0 and t0[coin]['activeaddresses'] and 'activeaddresses' in t1 and t1[coin]['activeaddresses']:
            if (t1[coin]['activeaddresses'] >= t0[coin]['activeaddresses']):
                block_feats[coin]['activeaddresses'] = 1
            else:
                block_feats[coin]['activeaddresses'] = 0
        else:
            block_feats[coin]['activeaddresses'] = 0

    for coin, v in divide_dict_by_max(t1, 'NetHashesPerSecond').items():
        block_feats[coin]['hashrate'] = v

    return block_feats


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


def bea_scores(sel_date = datetime.date.today(), coin_list = None, config = None):
    feats = bea_features(sel_date, coin_list, config)
    scores = bea_scoring_function(feats)
    return scores, feats


if __name__ == "__main__":
    scores, feats = bea_scores()
