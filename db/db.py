import pyodbc

class DB:
    def __init__(self):
        self.__server = ''
        self.__database = ''
        self.__usrname = ''
        self.__password = ''
        self.__driver= ''

        self.cnxn = None


    def connect(self):
        self.cnxn = pyodbc.connect('DRIVER=' + self.__driver + ';SERVER=' + self.__server + ';PORT=1443;DATABASE=' + self.__database + ';UID=' + self.__usrname + ';PWD=' + self.__password)

    def disconnect(self):
        self.cnxn.close()
