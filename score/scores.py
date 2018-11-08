import sys
sys.path.append('..')
import utils

import datetime
import time

import technical_analysis_scores
import sentiment_analysis_scores
import developer_analysis_scores
import expert_analysis_scores
import block_explorer_analysis_scores


## Technical Analysis (ta) features
#def ta_features(db = None):
#    if not db:
#        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
#        db=utils.DB(config.database)
#    db.connect()
#    cursor = db.cnxn.cursor()
#
#    # execute example
#    #cursor.execute('SELECT * FROM FtaDeveloper')
#    #rows = cursor.fetchall()
#
#
#    # after finishing everything disconnect from db
#    db.disconnect()
#    # and return if results in a dictionary; else write them to db
#    #return ta_feats


## Technical Analysis (ta) scoring function
#def ta_scoring_function(ta_feats = None, db = None):
#    # ta_feats if results in dictionary or
#    if not db:
#        config = utils.tools.ConfigFileParser('/home/pythagorasdev/searchers/config.yml')
#        db=utils.DB(config.database)
#    db.connect()
#    cursor = db.cnxn.cursor()
#
#    # dictionary with scores, format should be something like {'ltc': 50, 'btc': 75, 'eos': 85, etc.}
#    # you can check the functions bellow as examples
#    scores = {}
#
#    # after finishing everything disconnect from db
#    db.disconnect()
#    # return dictionay with scores
#    return scores


# Scoring Function to combine all scores
def scoring_function():

    total_analysis = {}

    # Block Explorer Analysis

    bea_scores, bea_feats = block_explorer_analysis_scores.bea_scores()

    total_analysis['bea_feats'] = bea_feats
    total_analysis['bea_scores'] = bea_scores

    #for coin in bea_scores:
    #    print(coin + ', ' + str(bea_scores[coin]))

    ## Expert Analysis

    #ea_scores, ea_feats = expert_analysis_scores.ea_scores()

    #total_analysis['ea_feats'] = ea_feats
    #total_analysis['ea_scores'] = ea_scores

    #for coin in ea_scores:
    #    print(coin + ', ' + str(ea_scores[coin]))

    # Sentiment Analysis

    sa_scores, sa_feats = sentiment_analysis_scores.sa_scores()

    total_analysis['sa_feats'] = sa_feats
    total_analysis['sa_scores'] = sa_scores

    #for coin in sa_scores:
    #    print(coin + ', ' + str(sa_scores[coin]))

    # Developer Analysis

    da_scores, da_feats = developer_analysis_scores.da_scores()

    total_analysis['da_feats'] = da_feats
    total_analysis['da_scores'] = da_scores

    # Technical Analysis

    ta_scores, ta_feats = technical_analysis_scores.ta_scores()
    # change keys to lowercase
    ta_scores = {k.lower(): v for k, v in ta_scores.items()}
    ta_feats = {k.lower(): v for k, v in ta_feats.items()}

    total_analysis['ta_feats'] = ta_feats
    total_analysis['ta_scores'] = ta_scores

    # look only for the ones we have block explorer feats
    #print('coin, bea, ea, sa, da, ta')
    #for coin in bea_scores:
    #    print(coin + ': ' + str(bea_scores[coin]) + ', ' + str(ea_scores[coin]) + ', ' + str(sa_scores[coin]) + ', ' + str(da_scores[coin]) + ', ' + str(ta_scores[coin]))

    #total_scores = {}
    #for coin in bea_scores:
    #    total_scores[coin] = 0.4 * ( 0.3 * bea_scores[coin] + 0.1 * ea_scores[coin] + 0.25 * da_scores[coin] ) + 0.2 * ( 1 * sa_scores[coin] ) + 0.4 * (ta_scores[coin])

    total_scores = {}
    total_scores['total'] = {}
    total_scores['fta'] = {}
    total_scores['ta'] = {}
    total_scores['sa'] = {}
    for coin in bea_scores:
        # if coin does not have developer analysis scores
        if coin not in da_scores.keys():
            print(coin.upper(), ' not in DA!!')
            da_scores[coin] = -100
        if coin not in sa_scores.keys():
            print(coin.upper(), ' not in SA!!')
            sa_scores[coin] = 0
        if coin not in ta_scores.keys():
            print(coin.upper(), ' not in TA!!')
            ta_scores[coin] = 0

        total_scores['fta'][coin] = 0.7 * bea_scores[coin] + 0.3 * da_scores[coin]
        total_scores['ta'][coin] = 1 * ta_scores[coin]
        total_scores['sa'][coin] = 1 * sa_scores[coin]

        #total_scores[coin] = 0.4 * ( 0.3 * bea_scores[coin] + 0.25 * da_scores[coin] ) + 0.2 * ( 1 * sa_scores[coin] ) + 0.4 * (ta_scores[coin])
        #total_scores['total'][coin] = 0.4 * (total_scores['fta'][coin]) + 0.2 * (total_scores['sa'][coin]) + 0.4 * (total_scores['ta'][coin])
        total_scores['total'][coin] = 0.45 * (total_scores['fta'][coin]) + 0.25 * (total_scores['sa'][coin]) + 0.3 * (total_scores['ta'][coin])

    return total_scores, total_analysis


def updateScoresDB(scores):
    config = utils.tools.ConfigFileParser('/home/pythagorasdev/Pythagoras/config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()
    for coin in scores['total']:
        #print(coin)
        cursor.execute('INSERT INTO VespucciScores(Symbol,Score,FTA_score,TA_score,SA_score,createdAt) VALUES (?,?,?,?,?,?)',
                            coin.upper(),
                            round(scores['total'][coin],10),
                            round(scores['fta'][coin],10),
                            round(scores['ta'][coin],10),
                            round(scores['sa'][coin],10),
                            (datetime.datetime.now())
                        )
        cursor.commit()


if __name__ == "__main__":
    # Sleep
    t=datetime.datetime.now()
    t0=datetime.datetime(t.year, t.month, t.day, 12)
    t1=t0 + datetime.timedelta(days=1)
    if t.hour < 12:
        t0=datetime.datetime(t.year, t.month, t.day, 12, 30)
        print('Sleep for ' + str((t0-t).total_seconds()))
        time.sleep((t0-t).total_seconds())

    total_scores, total_analysis = scoring_function()
    while(1):
        total_scores, total_analysis = scoring_function()
        print('Update DB')
        updateScoresDB(total_scores)

        t=datetime.datetime.now()
        t0=datetime.datetime(t.year, t.month, t.day, 12, 30)
        t1=t0 + datetime.timedelta(days=1)
        print('Sleep until ', t1)
        print(':: Sleep for ' + str((t1-t0).total_seconds()))
        time.sleep((t1-t0).total_seconds())

