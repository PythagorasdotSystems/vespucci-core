import sys
sys.path.append('..')
import utils


#def max_from_list_of_lists(l, pos = 0):
#    return max([i[pos] for i in l if i[pos] is not None])

def max_from_list(l):
    return max([i for i in l if i is not None])

#def divide_list_by_max(l, pos = 0):
#    return list(map(lambda x: (0 if x is None else x) / max_from_list_of_lists(l,pos), [i[pos] for i in l]))

def divide_dict_by_max(d, pos):
    max_value = max_from_list([v[pos] for v in d.values()])
    #print(max_value)
    results = {}
    for k, v in d.items():
        #print(v[pos])
        if max_value != 0 and v[pos] != None:
            results[k] = v[pos] / max_value
        else:
            results[k] = 0
    return results


def find_description_position(description, feature_name):
    return [i for i,x in enumerate([d[0] for d in description]) if x == feature_name][0]


# Block Explorer Analysis features
def bea_features(db = None):
    if not db:
        #db = DB()
        config = utils.tools.ConfigFileParser('../config.yml')
        db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    #cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >= 10/9/2018  AND CM_Timestamp <  11/9/2018')
    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -3, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -2, GETDATE())')
    R = cursor.fetchall()
    pos = find_description_position(cursor.description, 'Symbol')
    t0 = {}
    for r in R:
        t0[r[pos]] = list(r)

    print('BEA feats:: t0 length ', len(t0))
    #print(t0)

    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -2, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -1, GETDATE())')
    #cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >= 9/9/2018  AND CM_Timestamp <  10/9/2018')
    R = cursor.fetchall()
    pos = find_description_position(cursor.description, 'Symbol')
    t1 = {}
    for r in R:
        t1[r[pos]] = list(r)

    print('BEA feats:: t1 length ', len(t1))
    #print(t1)

    #cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -2, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -1, GETDATE())')
    # get position of symbol
    pos = find_description_position(cursor.description, 'Symbol')
    #descr = []

    block_feats = {}
    #for i in range(len(t1)):
    #    descr.append(t1[i][pos])
    #    block_feats[t1[i][pos]] = {}
    ##block_feats['symbol'] = (descr)
    for coin in t1:
        block_feats[coin] = {}

    # get position of feature
    pos = find_description_position(cursor.description, 'blockcount')
    #block_feats['blockcount'] = divide_list_by_max(t1, pos)
    #for i, v in enumerate(divide_list_by_max(t1, pos)):
    #    block_feats[descr[i]]['blockcount'] = v
    for coin, v in divide_dict_by_max(t1, pos).items():
        block_feats[coin]['blockcount'] = v
    #print(block_feats.items())

    # get position of feature
    pos = find_description_position(cursor.description, 'txcount')
    #block_feats['txcount'] = divide_list_by_max(t1, pos)
    #for i, v in enumerate(divide_list_by_max(t1, pos)):
    #    block_feats[descr[i]]['txcount'] = v
    for coin, v in divide_dict_by_max(t1, pos).items():
        block_feats[coin]['txcount'] = v
    #print(block_feats.items())

    # get positions of features
    pos_medianfee = find_description_position(cursor.description, 'medianfee')
    pos_activeaddresses = find_description_position(cursor.description, 'activeaddresses')
    #medianfee = []
    #activeaddresses = []
    #for i in range(len(t0) if len(t0) < len(t1) else len(t1)):
    for coin in t1:
        if coin in t0 and t0[coin][pos_medianfee] and t1[coin][pos_medianfee]:
        #if (t0[i][pos_medianfee] and t1[i][pos_medianfee]):
            if (t1[coin][pos_medianfee] <= t0[coin][pos_medianfee]):
                #medianfee.append(1)
                block_feats[coin]['medianfee'] = 1
            else:
                #medianfee.append(0)
                block_feats[coin]['medianfee'] = 0
        else:
            #medianfee.append(0)
            block_feats[coin]['medianfee'] = 0

        if coin in t0 and t0[coin][pos_activeaddresses] and t1[coin][pos_activeaddresses]:
            if (t1[coin][pos_activeaddresses] >= t0[coin][pos_activeaddresses]):
                #activeaddresses.append(1)
                block_feats[coin]['activeaddresses'] = 1
            else:
                #activeaddresses.append(0)
                block_feats[coin]['activeaddresses'] = 0
        else:
            #activeaddresses.append(0)
            block_feats[coin]['activeaddresses'] = 0

    #block_feats['medianfee'] = medianfee
    #block_feats['activeaddresses'] = activeaddresses

    cursor.execute('select * from FtaCryptoCompare where LastBlockExplorerUpdateTS >=  DATEADD(DAY, -2, GETDATE()) AND LastBlockExplorerUpdateTS <  DATEADD(DAY, -1, GETDATE())')
    #t1 = cursor.fetchall()
    R = cursor.fetchall()
    pos = find_description_position(cursor.description, 'Symbol')
    t1 = {}
    for r in R:
        t1[r[pos]] = list(r)


    #descr = []
    #pos = find_description_position(cursor.description, 'Symbol')
    #for i in range(len(t1)):
    #    descr.append(t1[i][pos])

    pos = find_description_position(cursor.description, 'NetHashesPerSecond')
    #block_feats['hashrate'] = divide_list_by_max(t1, pos)
    #for i, v in enumerate(divide_list_by_max(t1, pos)):
    #    if descr[i] not in block_feats.keys():
    #        block_feats[descr[i]] = {}
    #    block_feats[descr[i]]['hashrate'] = v

    for coin, v in divide_dict_by_max(t1, pos).items():
        if coin not in block_feats:
            block_feats[coin] = {}
        block_feats[coin]['hashrate'] = v
    #print(block_feats.items())

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


def bea_scores():
    feats = bea_features()
    scores = bea_scoring_function(feats)
    return scores, feats


if __name__ == "__main__":
    scores, feats = bea_scores()
