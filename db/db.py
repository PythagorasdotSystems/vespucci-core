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
        self.cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+usrname+';PWD='+ password)

    def disconnect(self):
        self.cnxn.close() 
