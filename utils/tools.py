import logging
import pyodbc
import yaml

__global_vars={}
__global_vars['config']=None

# Logger
#----------------------------------------------------
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


# Config
#----------------------------------------------------
class ConfigFileParser:
    def __init__(self, fname):
        with open(fname, 'r') as f:
            self.__config = yaml.load(f)
        #print(self.__config)
        
        self.database = self.__config['db']
        self.twitter = self.__config['twitter']
        self.healthchecks = self.__config['healthchecks']

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, value):
        #TODO add checks
        self.__database = value

    @property
    def twitter(self):
        return self.__twitter

    @twitter.setter
    def twitter(self, value):
        #TODO add checks
        self.__twitter = value


def set_config(config_file):
    __global_vars['config']=ConfigFileParser(config_file)


def get_config():
    return __global_vars['config']


# Database
#----------------------------------------------------
class DB:
    def __init__(self, config_db):
        self.__server = config_db['server']
        self.__database = config_db['database']
        self.__usrname = config_db['username']
        self.__password = config_db['password']
        self.__driver= config_db['driver']

        self.cnxn = None

    def connect(self):
        self.cnxn = pyodbc.connect('DRIVER=' + self.__driver + ';SERVER=' + self.__server + ';PORT=1443;DATABASE=' + self.__database + ';UID=' + self.__usrname + ';PWD=' + self.__password)

    def disconnect(self):
        self.cnxn.close()


# Vespucci
#----------------------------------------------------
def vespucci_coin_list(config=None):
    if not config: config = get_config()
    db = DB(config.database)
    db.connect()
    cursor = db.cnxn.cursor()
    cursor.execute('select Symbol,Name,WebsiteSlug from Coins')
    rows = cursor.fetchall()
    coins = []
    for r in rows:
        coins.append({'Symbol':r[0], 'Name':r[1], 'WebsiteSlug':r[2]})
    return coins

