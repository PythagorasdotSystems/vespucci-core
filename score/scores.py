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

    # Expert Analysis

    ea_scores, ea_feats = expert_analysis_scores.ea_scores()

    total_analysis['ea_feats'] = ea_feats
    total_analysis['ea_scores'] = ea_scores

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
    print('coin, bea, ea, sa, da, ta')
    for coin in bea_scores:
        print(coin + ': ' + str(bea_scores[coin]) + ', ' + str(ea_scores[coin]) + ', ' + str(sa_scores[coin]) + ', ' + str(da_scores[coin]) + ', ' + str(ta_scores[coin]))

    total_scores = {}
    for coin in bea_scores:
        total_scores[coin] = 0.4 * ( 0.3 * bea_scores[coin] + 0.1 * ea_scores[coin] + 0.25 * da_scores[coin] ) + 0.2 * ( 1 * sa_scores[coin] ) + 0.4 * (ta_scores[coin])

    return total_scores, total_analysis


if __name__ == "__main__":
    total_scores, total_analysis = scoring_function()

