import utils

import v_ta
import v_fta
import v_score

config = utils.tools.ConfigFileParser('./config.yml')

logger = utils.logger_default('fta_&_scores_cron', '../fta_scores_cron.log')

logger.info('Starting (Update FTA feats and compute score)')

# TA features
logger.info('Update TA feats')
utils.cron_job.run_cron_func(v_ta.daily_update, config.healthchecks['ta'])

# FTA features
logger.info('Update FTA feats')
utils.cron_job.run_cron_func(v_fta.update, config.healthchecks['fta'])


# Compure new scores
logger.info('Compute score')
utils.cron_job.run_cron_func(v_score.update, config.healthchecks['score'])

logger.info('Finished (update FTA feats and compute score)')
