import utils


# Expert Analysis features
def ea_features():
    scores_expert = {}
    with open('/home/pythagorasdev/Pythagoras/dev/vespucci-core/ea_scores.txt', 'r') as f:
        for line in f:
            line=line.strip().split(',')
            # normalize values from [1,10] to [0.1,1]
            scores_expert[line[1].lower()] = float(line[0])/10.0
    return scores_expert


# Expert Analysis (ea) scoring function
def ea_scoring_function(ea_feats):
    score = {}
    for coin in ea_feats:
        score[coin] = round(ea_feats[coin] * 100.0,2)
    return score


def ea_scores():
    feats = ea_features()
    scores = ea_scoring_function(feats)
    return scores, feats

if __name__ == "__main__":
    scores, feats = ea_scores()

