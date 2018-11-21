### Requirements

```
pip3 install crycompare
# pycoingecko and coinmetrics must be cloned in the root folder of the project
git clone https://github.com/man-c/pycoingecko.git
python3 setup.py install
git clone https://github.com/man-c/coinmetrics.git
python3 setup.py install
```

### Usage Examples
Compute todays score
```
import v_score
total_scores = v_score.scoring_function()
```