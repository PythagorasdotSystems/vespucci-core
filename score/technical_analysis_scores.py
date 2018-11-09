from ta import *
import pandas as pd
from .technical_analysis_funcs import *

import sys
sys.path.append('..')
import utils

def ta_features():
       #Connect to Database
       config = utils.tools.ConfigFileParser('/home/pythagorasdev/Pythagoras/config.yml')
       db=utils.DB(config.database)
       db.connect()
       cursor = db.cnxn.cursor()

       cursor.execute('Select Symbol,Close_value,Date,High,Low,Market_Cap,Open_value,Volume from ta.cryptoHistory WHERE Date >  DATEADD(DAY, -200, GETDATE())')
       R = cursor.fetchall()
       t0 = {}
       for r in R:
       	if r[0] not in t0:
       		t0[r[0]] = {}
       		t0[r[0]]['Close'] = []
       		t0[r[0]]['Date'] = []
       		t0[r[0]]['High'] = []
       		t0[r[0]]['Low'] = []
       		t0[r[0]]['Market_Cap'] = []
       		t0[r[0]]['Open'] = []
       		t0[r[0]]['Volume'] = []
       	t0[r[0]]['Close'].append(r[1])
       	t0[r[0]]['Date'].append(r[2])
       	t0[r[0]]['High'].append(r[3])
       	t0[r[0]]['Low'].append(r[4])
       	t0[r[0]]['Market_Cap'].append(r[5])
       	t0[r[0]]['Open'].append(r[6])
       	t0[r[0]]['Volume'].append(r[7])
       return t0
       
def ta_scoring_function(ta_feats):
       scores = {}
       for key in ta_feats.keys():
       	df = pd.DataFrame.from_dict(ta_feats[key],orient='index').transpose()
       	score = IndicatorsScore(df)
       	scores[key] = int(score*100)
       return scores
       
def IndicatorsScore(df):
       Bollinger_Bands( df, []);
       IchimokuKinkoHyo(df,[]);
       MACD(df, []);
       RSI(df,[]);
       Moving_average_crossover(df, [],False);
       #FibonacciRetracements(df);
       new = df[['Date','Close','volatility_bbm','bb_high_indicator','bb_low_indicator','trend_macd', 'trend_macd_diff', 'momentum_rsi','9-21band','9-30band','9-50band','9-100band','9-200band','trend_ichimoku_a']].copy()
       #Bolinger Bands Score
       new['BB_score'] = np.where(new['Close'] > new['volatility_bbm'], 0.5, 0)
       new['BB_score'] = np.where(new['Close'] < new['volatility_bbm'], -0.5,new['BB_score'])
       new['BB_score'] = np.where(new['bb_high_indicator'] > 0, 1, new['BB_score'])
       new['BB_score'] = np.where(new['bb_low_indicator'] > 0, -1, new['BB_score'])
       #MACD
       new['macd_score1'] = np.where(new['trend_macd'] > 0, 0.5, 0)
       new['macd_score1'] = np.where(new['trend_macd'] < 0, -0.5,new['macd_score1'])
       new['macd_score2'] = np.where(new['trend_macd_diff'] > 0, 1, 0)
       new['macd_score2'] = np.where(new['trend_macd_diff'] < 0, -1, new['macd_score2'])
       new['macd_score'] = np.where((new['macd_score1'] == new['macd_score2']), new['macd_score2'], (new['macd_score1'] + new['macd_score2']))
       new['macd_score'] = np.where((new['macd_score'] == -1.5), -1, new['macd_score'])
       new['macd_score'] = np.where((new['macd_score'] == 1.5),1,new['macd_score'])
       #RSI
       new['rsi_score'] = np.where(new['momentum_rsi'] < 30, -1, 0)
       new['rsi_score'] = np.where(new['momentum_rsi'] > 70, 1, new['rsi_score'])
       #ICHIMOKU
       new['ichimoku_score'] = new['Close']-new['trend_ichimoku_a']
       new['ichimoku_score'] = np.where(new['ichimoku_score'] > 0,1,new['ichimoku_score'])
       new['ichimoku_score'] = np.where(new['ichimoku_score'] < 0,-1,new['ichimoku_score'])
       #MAC
       new['mac_score'] = 0.05*new['9-21band'] + 0.10*new['9-30band'] + 0.15*new['9-50band'] + 0.20*new['9-100band'] + 0.50*new['9-200band']
       #Indicators Score
       score = new[['Date','Close','BB_score','macd_score','rsi_score','ichimoku_score','mac_score']].copy()
       score2 = score[len(score)-2:len(score)].copy()
       score2['5indicators'] = score2['BB_score'] + score2['macd_score'] + score2['rsi_score'] + score2['ichimoku_score'] + score2['mac_score']
       score2['final_score'] = np.where(score2['5indicators'] > 3,(0.5*score2['5indicators']/5.0 + 0.5*score2['macd_score']),0.5*score2['macd_score'])
       score = score2['final_score'][len(score2)-1:len(score2)]
       return score.iloc[0]

def ta_scores():
    feats = ta_features()
    scores = ta_scoring_function(feats)
    return scores, feats

if __name__== '__main__':
    scores, feats = ta_scores()

