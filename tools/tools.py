import logging


def logger_default(logger_name, logger_fname):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # file handler logs debug msg
    fh = logging.FileHandler(logger_fname)
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    #smtp_handler = logging.handlers.SMTPHandler(
    #mailhost=("smtp.gmail.com", 465),
    #        fromaddr="manolis@pythagoras.systems",
    #        toaddrs="manolis@pythagoras.systems",
    #        subject=u"AppName error!")

    #add the handlers to logger
    logger.addHandler(fh)
    #logger.addHandler(smtp_handler)

    return logger
