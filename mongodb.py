from pymongo import MongoClient
import logging
import traceback

from singleton import Singleton
from database_config import MongoDataBaseConfig
from config import Config

'''
important to use pymongo with srv and tls to connect to mongodb atlas
python -m pip install pymongo[srv]
requirements
pymongo[tls,srv]==3.11.1

'''


class MongoDataBase(metaclass=Singleton):

    def __init__(self, ticker_priority_exchange):
        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.client = None
        self.db = None
        self.ticker_priority_exchange = ticker_priority_exchange
        self.last_insert = 0

    

    def insert(self, collection, document):

        if collection == "tickers" and document["source"] != self.ticker_priority_exchange:
            diff = document["epoch"] - self.last_insert
            if diff < 60:
                self.logger.debug(f"recent ticker insert {diff}s from priority exchange {self.ticker_priority_exchange}, discard ticker insert from {document['source']}")
                return
        if self.db != None:
            try:
                result = self.db[collection].insert_one(document)
            except Exception as e:
                self.logger.error(
                    f"Error insert_one ticker:{e}->{traceback.format_exc()}")
                raise e
            else:
                self.logger.debug(f"insert ticker result:{result.inserted_id}")
                if collection == "tickers" and document["source"] == self.ticker_priority_exchange:
                    self.last_insert = document["epoch"]
                return

    def close(self):
        self.client.close()
        self.client = None
        self.db = None

    def connect(self):
        '''
        w setting: how many nodes should acknowledge the write before declaring it a success. 
        w=majority
        retryWrites=true retry certain write operations a single time if they fail.
        '''
        if MongoDataBaseConfig.atlas:
            uri = f"mongodb+srv://{MongoDataBaseConfig.user}:{MongoDataBaseConfig.password}@{MongoDataBaseConfig.host}/{MongoDataBaseConfig.database}?retryWrites=true&w=majority"
        else:
            uri = f"mongodb://{MongoDataBaseConfig.user}:{MongoDataBaseConfig.password}@{MongoDataBaseConfig.host}/{MongoDataBaseConfig.database}"
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        except Exception as e:
            self.logger.error(
                f"MongoClient Connection ERROR:{e}->{traceback.format_exc()}")
            raise e

        db = client.get_database(MongoDataBaseConfig.database)
        try:
            response = db.command(
                {'connectionStatus': 1, 'showPrivileges': False})
            self.logger.debug(response)
        except Exception as e:
            self.logger.error(
                f"MongoClient DataBase Connection ERROR:{e}->{traceback.format_exc()}")
            raise e
        else:
            if response['ok'] == 1.0:
                self.logger.info(f"DB Connected OK:{response}")
                self.client = client
                self.db = db
            else:
                self.logger.error(f"DB Connected ERROR:{response}")
