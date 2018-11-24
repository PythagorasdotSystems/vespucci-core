import sys
sys.path.append('..')
import utils

import datetime

def select_da_by_date(sel_date):

    config = utils.tools.ConfigFileParser('../config.yml')
    db=utils.DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()

    sel_date = datetime.datetime(sel_date.year, sel_date.month, sel_date.day)
    prev_sel_date = sel_date - datetime.timedelta(1)

    cursor.execute('select Symbol, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks, last_updated from FtaDeveloper where last_updated >= ? AND last_updated < ?', prev_sel_date, sel_date)
    R = cursor.fetchall()
    #print(R)
    d = {}
    for r in R:
        #print(r)
        if r[0] not in d:
            d[r[0]] = {}
        d[r[0]]['forks'] = r[1]
        d[r[0]]['stars'] = r[2]
        d[r[0]]['subscribers'] = r[3]
        d[r[0]]['total_issues'] = r[4]
        d[r[0]]['closed_issues'] = r[5]
        d[r[0]]['pull_requests_merged'] = r[6]
        d[r[0]]['pull_request_contributors'] = r[7]
        d[r[0]]['commit_count_4_weeks'] = r[8]
    print('t',str(r[-1]))
    print('t',len(R))
    print('t',len(d))
    print('t:',R[0])
    print('t:',R[-1])
    print('FTA_developer: len',len(d))
    return d


def compute_da_scores(t0, t1):
    da_feats = {}
    for coin in t1:

        # check if coin exists in both lists
        if coin not in t0:
            print('FTA_developer: coin feats only for one day:', coin)
            continue

        da_feats[coin] = {}

        da_feats[coin]['open_source'] = (1 if sum(t1[coin].values()) > 0 else -1)

        da_feats[coin]['commit_count_4_weeks'] = (1 if t1[coin]['commit_count_4_weeks'] > 0 else 0)

        if t1[coin]['total_issues'] > 0 and t0[coin]['total_issues'] > 0 and (t0[coin]['total_issues'] - t0[coin]['closed_issues']) > 0 and (t1[coin]['total_issues'] - t1[coin]['closed_issues']) > 0 and ((t1[coin]['closed_issues'] / ( t1[coin]['total_issues'] - t1[coin]['closed_issues'] )) >= (t0[coin]['closed_issues'] / ( t0[coin]['total_issues'] - t0[coin]['closed_issues'] ))):
            da_feats[coin]['issues_rate'] = 1
        else:
            da_feats[coin]['issues_rate'] = 0
        
        t1_fss = t1[coin]['forks'] + t1[coin]['stars'] + t1[coin]['subscribers']
        t0_fss = t0[coin]['forks'] + t0[coin]['stars'] + t0[coin]['subscribers']
        if t1_fss > 0 and t1_fss >= t0_fss:
            da_feats[coin]['forks_stars_subs'] = 1
        else:
            da_feats[coin]['forks_stars_subs'] = 0
        
        t1_pr = t1[coin]['pull_requests_merged'] + t1[coin]['pull_request_contributors']
        t0_pr = t0[coin]['pull_requests_merged'] + t0[coin]['pull_request_contributors']
        if t1_pr > 0 and t1_pr > t0_pr:
            da_feats[coin]['pull_requests'] = 1
        else:
            da_feats[coin]['pull_requests'] = 0

        #t1[5]/(t1[4] - t1[5])
    return da_feats


# Developer Analysis features
def da_features(sel_date = datetime.date.today()):
    print(sel_date)
    #cursor.execute('select Symbol, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks from FtaDeveloper where last_updated >=  DATEADD(DAY, -3, GETDATE()) AND last_updated <  DATEADD(DAY, -2, GETDATE())')
    #R = cursor.fetchall()
    ##print(R)
    #t0 = {}
    #for r in R:
    #    if r[0] not in t0:
    #        t0[r[0]] = {}
    #    t0[r[0]]['forks'] = r[1]
    #    t0[r[0]]['stars'] = r[2]
    #    t0[r[0]]['subscribers'] = r[3]
    #    t0[r[0]]['total_issues'] = r[4]
    #    t0[r[0]]['closed_issues'] = r[5]
    #    t0[r[0]]['pull_requests_merged'] = r[6]
    #    t0[r[0]]['pull_request_contributors'] = r[7]
    #    t0[r[0]]['commit_count_4_weeks'] = r[8]

    #cursor.execute('select Symbol, forks, stars, subscribers, total_issues, closed_issues, pull_requests_merged, pull_request_contributors, commit_count_4_weeks from FtaDeveloper where last_updated >=  DATEADD(DAY, -2, GETDATE()) AND last_updated <  DATEADD(DAY, -1, GETDATE())')
    #R = cursor.fetchall()
    ##print(R)
    #t1 = {}
    #for r in R:
    #    if r[0] not in t1:
    #        t1[r[0]] = {}
    #    t1[r[0]]['forks'] = r[1]
    #    t1[r[0]]['stars'] = r[2]
    #    t1[r[0]]['subscribers'] = r[3]
    #    t1[r[0]]['total_issues'] = r[4]
    #    t1[r[0]]['closed_issues'] = r[5]
    #    t1[r[0]]['pull_requests_merged'] = r[6]
    #    t1[r[0]]['pull_request_contributors'] = r[7]
    #    t1[r[0]]['commit_count_4_weeks'] = r[8]


    prev_sel_date = sel_date - datetime.timedelta(1)
    t0 = select_da_by_date(prev_sel_date)
    t1 = select_da_by_date(sel_date)

    #da_feats = {}
    #for coin in t1:

    #    # check if coin exists in both lists
    #    if coin not in t0:
    #        continue

    #    da_feats[coin] = {}

    #    da_feats[coin]['open_source'] = (1 if sum(t1[coin].values()) > 0 else -1)

    #    da_feats[coin]['commit_count_4_weeks'] = (1 if t1[coin]['commit_count_4_weeks'] > 0 else 0)

    #    if t1[coin]['total_issues'] > 0 and t0[coin]['total_issues'] > 0 and (t0[coin]['total_issues'] - t0[coin]['closed_issues']) > 0 and (t1[coin]['total_issues'] - t1[coin]['closed_issues']) > 0 and ((t1[coin]['closed_issues'] / ( t1[coin]['total_issues'] - t1[coin]['closed_issues'] )) >= (t0[coin]['closed_issues'] / ( t0[coin]['total_issues'] - t0[coin]['closed_issues'] ))):
    #        da_feats[coin]['issues_rate'] = 1
    #    else:
    #        da_feats[coin]['issues_rate'] = 0
    #
    #    t1_fss = t1[coin]['forks'] + t1[coin]['stars'] + t1[coin]['subscribers']
    #    t0_fss = t0[coin]['forks'] + t0[coin]['stars'] + t0[coin]['subscribers']
    #    if t1_fss > 0 and t1_fss >= t0_fss:
    #        da_feats[coin]['forks_stars_subs'] = 1
    #    else:
    #        da_feats[coin]['forks_stars_subs'] = 0
    #
    #    t1_pr = t1[coin]['pull_requests_merged'] + t1[coin]['pull_request_contributors']
    #    t0_pr = t0[coin]['pull_requests_merged'] + t0[coin]['pull_request_contributors']
    #    if t1_pr > 0 and t1_pr > t0_pr:
    #        da_feats[coin]['pull_requests'] = 1
    #    else:
    #        da_feats[coin]['pull_requests'] = 0

    #    #t1[5]/(t1[4] - t1[5])

    da_feats = compute_da_scores(t0,t1)

    return da_feats


# Developer Analysis (da) scoring function
def da_scoring_function(da_feats):
    score = {}
    for coin in da_feats:
        score[coin] = 0
        if 'open_source' in da_feats[coin]:
            #score[coin] += da_feats[coin]['open_source'] * 50
            #if da_feats[coin]['open_source'] == -1:
            #    score[coin] += -100
            #else:
            #    score[coin] += 50
            score[coin] += (-100 if da_feats[coin]['open_source'] == -1 else 50)
        if 'commit_count_4_weeks' in da_feats[coin]:
            score[coin] += da_feats[coin]['commit_count_4_weeks'] * 12.5
        if 'issues_rate' in da_feats[coin]:
            score[coin] += da_feats[coin]['issues_rate'] * 12.5
        if 'forks_stars_subs' in da_feats[coin]:
            score[coin] += da_feats[coin]['forks_stars_subs'] * 12.5
        if 'pull_requests' in da_feats[coin]:
            score[coin] += da_feats[coin]['pull_requests'] * 12.5
    return score


def da_scores(sel_date = datetime.date.today()):
    feats = da_features(sel_date)
    scores = da_scoring_function(feats)
    return scores, feats


if __name__ == "__main__":
    scores, feats = da_scores()
