import sys
sys.path.append('..')
import utils


def __db_cursor(config_file):
    config = utils.tools.ConfigFileParser(config_file)
    db=utils.DB(config.database)
    db.connect()
    return db.cnxn.cursor()


def __coin_scores_dict(coin_scores):
    return {
            'id': coin_scores[0],
            'score': float(coin_scores[1]),
            'FTA_score': float(coin_scores[2]),
            'TA_score': float(coin_scores[3]),
            'SA_score': float(coin_scores[4]),
            'createdAtTS': round(coin_scores[5].timestamp())
            }


def vespucci_scores(config_file):
    
    cursor = __db_cursor(config_file)

    scores = []
    cursor.execute('SELECT DISTINCT Symbol from VespucciScores')
    coins_list = cursor.fetchall()
    for r in coins_list:
        coin = r[0]
        cursor.execute('select top 1 Symbol, Score, FTA_score, TA_score, SA_score, createdAt from VespucciScores WITH (NOLOCK)  WHERE [Symbol]=?  ORDER BY myid  DESC',coin.upper())
        coin_scores = cursor.fetchall()[0]
        #scores.append({'id': coin,
        #    'score': float(coin_scores[1]),
        #    'FTA_score': float(coin_scores[2]),
        #    'TA_score': float(coin_scores[3]),
        #    'SA_score': float(coin_scores[4]),
        #    'createdAtTS': round(coin_scores[5].timestamp())})
        scores.append(__coin_scores_dict(coin_scores))
    #__db.cnxn.close()
    scores = sorted(scores, key = lambda i: i['score'], reverse=True)
    return scores


def vespucci_coin_scores(config_file, coin):
    coin = coin.upper()
    
    cursor = __db_cursor(config_file)

    cursor.execute('SELECT DISTINCT Symbol from VespucciScores')
    coins_list = cursor.fetchall()
    coins_list = [c[0].upper() for c in coins_list]
    if coin not in coins_list:
        return ''

    scores = []

    cursor.execute('select top 30 Symbol, Score, FTA_score, TA_score, SA_score, createdAt from VespucciScores WITH (NOLOCK)  WHERE [Symbol]=?  ORDER BY myid  DESC',coin)
    coin_history = cursor.fetchall()
    for coin_snapshot in coin_history:
        #scores.append({'id': coin,
        #    'score': float(coin_snapshot[1]),
        #    'FTA_score': float(coin_snapshot[2]),
        #    'TA_score': float(coin_snapshot[3]),
        #    'SA_score': float(coin_snapshot[4]),
        #    'createdAtTS': round(coin_snapshot[5].timestamp())})
        scores.append(__coin_scores_dict(coin_snapshot))
    #__db.cnxn.close()

    return scores


def vespucci_scores_coins_list(config_file):
    #__db.connect()
    #cursor = __db.cnxn.cursor()
    
    cursor = __db_cursor(config_file)

    cursor.execute('SELECT DISTINCT Symbol from VespucciScores')
    coins_list = cursor.fetchall()
    coins_list = [c[0].upper() for c in coins_list]

    #__db.cnxn.close()

    return coins_list

