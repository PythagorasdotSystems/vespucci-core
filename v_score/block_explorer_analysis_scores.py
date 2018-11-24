import sys
sys.path.append('..')
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
    max_value = max([x[feat_name] for x in d.values() if feat_name in x and x[feat_name] is not None])
    #print(max_value)
    results = {}
    for k, v in d.items():
        if max_value != 0 and feat_name in v and v[feat_name] is not None:
            results[k] = v[feat_name] / max_value
        else:
            results[k] = 0
        #print(k,results[k])
    return results


#def find_description_position(description, feature_name):
#    return [i for i,x in enumerate([d[0] for d in description]) if x == feature_name][0]


def select_bea_by_date(sel_date):
    sel_date = datetime.datetime(sel_date.year, sel_date.month, sel_date.day)
    prev_sel_date = sel_date - datetime.timedelta(1)
    #print(sel_date)
    #print(prev_sel_date)

    config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    cursor.execute('select Symbol, blockcount, txcount, medianfee, activeaddresses from FtaCoinMetrics where CM_Timestamp >= ?  AND CM_Timestamp < ?', prev_sel_date, sel_date)
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

    cursor.execute('select Symbol, NetHashesPerSecond from FtaCryptoCompare where LastBlockExplorerUpdateTS >= ? AND LastBlockExplorerUpdateTS <  ?', prev_sel_date, sel_date)
    R = cursor.fetchall()
    for r in R:
        if r[0] not in d:
            d[r[0]] = {}
        d[r[0]]['NetHashesPerSecond'] = r[1]

    #for i in d:
    #    print(i,len(d[i]))
    #    print(i, d[i])

    return d

# Block Explorer Analysis features
def bea_features(sel_date = datetime.date.today(), db = None):

    prev_sel_date = sel_date - datetime.timedelta(1)
    t0 = select_bea_by_date(prev_sel_date)
    t1 = select_bea_by_date(sel_date)

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


def bea_scores(sel_date = datetime.date.today()):
    feats = bea_features(sel_date)
    scores = bea_scoring_function(feats)
    return scores, feats


if __name__ == "__main__":
    scores, feats = bea_scores()
