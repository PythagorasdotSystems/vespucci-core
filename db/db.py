import pyodbc

class DB:
    def __init__(self):
        self.server = ''
        self.database = ''
        self.usrname = ''
        self.password = ''

        self.driver=''

        self.cnxn = None


    def connect(self):
        self.cnxn = pyodbc.connect('DRIVER=' + self.__driver + ';SERVER=' + self.__server + ';PORT=1443;DATABASE=' + self.__database + ';UID=' + self.__usrname + ';PWD=' + self.__password)

    def disconnect(self):
        self.cnxn.close() 
