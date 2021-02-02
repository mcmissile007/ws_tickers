import logging
import asyncio
import json
import traceback
import sys
import time
from collections import defaultdict
from pymongo import MongoClient
from aiohttp import ClientSession
from mongodb import MongoDataBase
from mysqldb import MysqlDataBase

from datetime import datetime


from config import Config
from exchange import Exchange

class Bittrex(Exchange):

    def __init__(self, mongodb: MongoDataBase, mysqldb: MysqlDataBase, pairs_to_record: list):

        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.logger.debug(f"Init {str(__name__)}")
        
        self.mongodb = mongodb
        self.mysqldb = mysqldb
        self.logger.debug(f"mongodb id:{id(self.mongodb)}")
        self.logger.debug(f"mysqldb id:{id(self.mysqldb)}")

        self.min_diff_to_insert = 10
        
        self.pairs_to_record =  [ quote + "-" + base for base,quote in pairs_to_record]

    def __parse_ticker_response(self,response: dict) -> dict:

        if type(response) == dict and "code" not in response:
            return self.__parse_ticker(response)
        else:
            return None
    
    def __parse_ticker(self, ticker: dict) -> dict:
        if type(ticker) == dict:
            if all( (key in ticker for key in ['symbol','lastTradeRate',"bidRate","askRate"]) ):
                new_ticker = {}
                quote,base = ticker['symbol'].split("-")
                new_ticker['source'] = "bittrex"
                new_ticker['epoch'] = int(time.time())
                new_ticker['ts'] = datetime.utcfromtimestamp(new_ticker['epoch']).strftime('%Y-%m-%d %H:%M:%S')
                new_ticker['pair'] = base + "_" + quote
                new_ticker['last'] = float(ticker['lastTradeRate'])
                new_ticker['ask'] = float(ticker['askRate'])
                new_ticker['bid'] = float(ticker['bidRate'])
                return new_ticker 
        else:
            return None
    
    async def get_tickers(self,lock):
        #base_url = "https://api.bittrex.com/api/v1.1/public/getticker?market="
        base_url = "https://api.bittrex.com/v3/markets/"
        #/markets/{marketSymbol}/ticker
        while True:
            try:
                async with ClientSession() as session:
                    for pair  in self.pairs_to_record:
                        async with session.get(base_url + pair + "/ticker") as response:
                            response = await response.read()
                            response = json.loads(response)
                            ticker = self.__parse_ticker_response(response)
                            if ticker is not None:
                                try:
                                    async with lock:
                                        self.mongodb.insert("tickers",ticker)
                                except Exception as e:
                                    self.logger.error(f"Exception in Insert MongoDataBase:{e}->{traceback.format_exc()}")
                                    return
                                
                                try:
                                    async with lock:
                                        self.mysqldb.insert("tickers",ticker)
                                except Exception as e:
                                    self.logger.error(f"Exception in Insert MysqlDataBase:{e}->{traceback.format_exc()}")
                                    return
                            await asyncio.sleep(1)


            except Exception as e:
                self.logger.error(f"Exception:{e}->{traceback.format_exc()}")
                await asyncio.sleep(60)

           
            await asyncio.sleep(self.min_diff_to_insert)
