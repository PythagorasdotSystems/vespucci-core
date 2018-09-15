import logging
import pyodbc
import yaml

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

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, value):
        #TODO add checks
        print('databse setterr!')
        self.__database = value

    @property
    def twitter(self):
        return self.__twitter

    @twitter.setter
    def twitter(self, value):
        #TODO add checks
        print('twitter checks')
        self.__twitter = value



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
