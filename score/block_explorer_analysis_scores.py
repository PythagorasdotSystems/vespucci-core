import sys
sys.path.append('..')
import utils


def max_from_list(l, pos = 0):
    return max([i[pos] for i in l if i[pos] is not None])


def divide_list_by_max(l, pos = 0):
    return list(map(lambda x: (0 if x is None else x) / max_from_list(l,pos), [i[pos] for i in l]))


def find_description_position(description, feature_name):
    return [i for i,x in enumerate([d[0] for d in description]) if x == feature_name][0]


# Block Explorer Analysis features
def bea_features(db = None):
    if not db:
        #db = DB()
        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
        db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    #cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >= 10/9/2018  AND CM_Timestamp <  11/9/2018')
    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -3, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -2, GETDATE())')
    t0 = cursor.fetchall()
    print(len(t0))
    cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >=  DATEADD(DAY, -2, GETDATE()) AND CM_Timestamp <  DATEADD(DAY, -1, GETDATE())')
    #cursor.execute('select * from FtaCoinMetrics where CM_Timestamp >= 9/9/2018  AND CM_Timestamp <  10/9/2018')
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
