import utils

import v_ta
import v_fta
import v_score

import sys, getopt

# load config file
configfile='./config.yml'
try:
    opts, args = getopt.getopt(sys.argv[1:],'c:',['config='])
except getopt.GetoptError:
    print('cron_run.py -c <configfile>')
    sys.exit(2)

for opt, arg in opts:
    if opt in ('-c','--config'): configfile=arg

# set config file
utils.tools.set_config(configfile)

# create logger
logger = utils.logger_default('fta_&_scores_cron', '../fta_scores_cron.log')

logger.info('Starting (Update TA & FTA feats and compute score)')

# update TA, FTA and compute new scores

# TA features
logger.info('Update TA feats')
utils.cron_job.run_cron_func(v_ta.daily_update, config.healthchecks['ta'])

# FTA features
logger.info('Update FTA feats')
utils.cron_job.run_cron_func(v_fta.update, config.healthchecks['fta'])


# Compure new scores
logger.info('Compute score')
utils.cron_job.run_cron_func(v_score.update, config.healthchecks['score'])

logger.info('Finished (update TA & FTA feats and compute score)')
