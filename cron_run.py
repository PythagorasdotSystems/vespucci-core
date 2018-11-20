import utils

import fta
import score

config = utils.tools.ConfigFileParser('/home/pythagorasdev/Pythagoras/config.yml')

logger = utils.logger_default('fta_&_scores_cron', '/home/pythagorasdev/Pythagoras/fta_scores_cron.log')

logger.info('Starting (Update FTA feats and compute score)')


# FTA features
logger.info('Update FTA feats')
utils.cron_job.run_cron_func(fta.update, config.healthchecks['fta'])


# Compure new scores
logger.info('Compute score')
utils.cron_job.run_cron_func(score.update, config.healthchecks['score'])

logger.info('Finished (update FTA feats and compute score)')
