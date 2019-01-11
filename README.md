### Requirements

```
# Technical Analysis Library in Python https://github.com/bukosabino/ta
pip3 install ta

# Python wrapper for CryptoCompare public API
pip3 install crycompare

# pycoingecko: Python wrapper for the CoinGecko API
pip3 install pycoingecko
# or from source
git clone https://github.com/man-c/pycoingecko.git
python3 setup.py install

# coinmetrics:  Python wrapper for the CoinMetrics API
git clone https://github.com/man-c/coinmetrics.git
python3 setup.py install
```

### Usage Examples
```
import v_score
import utils
```

Set the config file
```
utils.tools.set_config('path-to-config-yml-file')
```

Compute today's total scores (FTA, TA & SA)
```
total_scores, total_analysis=v_score.scoring_function()
```

TA - Technical Analysis scores
```
scores, feats = v_score.technical_analysis_scores.ta_scores()
```

FTA - Developer Analysis (Github) scores
```
scores, feats = v_score.developer_analysis_scores.da_scores()

```
FTA - Block Explorer Analysis scores
```
scores, feats = v_score.block_explorer_analysis_scores.bea_scores()
```

SA - Sentiment Analysis scores
```
scores, feats = v_score.sentiment_analysis_scores.sa_scores()
```
