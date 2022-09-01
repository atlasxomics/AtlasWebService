from tkinter import E
import mysql.connector

class MariaDB(object):
    def __init__(self, auth):
        
        self.auth=auth
        self.client=None
        self.host = self.auth.app.config["MYSQL_HOST"]
        self.port = self.auth.app.config["MYSQL_PORT"]
        self.username = self.auth.config["MYSQL_USERNAME"]
        self.password = self.auth.config["MYSQL_PASSWORD"]
        self.db = None
        self.initialize()
    
    def initialize(self):
        print('init')
        try:
            self.client=mysql.connector.connect(user=self.username, password=self.password, host=self.host, port=self.port)
        except Exception as e:
            print(e)