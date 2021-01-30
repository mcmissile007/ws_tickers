import logging
import asyncio
import websockets
import json
import traceback
import sys
import time
from collections import defaultdict
from mongodb import MongoDataBase
from datetime import datetime


from config import Config
from exchange import Exchange



 
class Poloniex(Exchange):

    def __init__(self, mongodb: MongoDataBase, pairs_to_record: list):


        # dot notation parent.child
        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.logger.debug(f"Init {str(__name__)}")

        self.mongodb = mongodb
        self.logger.debug(f"mongodb id:{id(self.mongodb)}")

        self.min_diff_to_insert = 10
        self.last_insert_epoch = defaultdict(int)

        self.pairs_to_record = [ base + "_" + quote for base,quote in pairs_to_record]
        self.pair_ids = defaultdict(str)
        with open('./json/poloniex_usdt_pair_ids.json', 'r') as file:
            self.pair_ids.update(json.load(file))


    def __parse_ticker_response(self, response: list) -> dict:

        if type(response) == list and len(response) == 3:
            _channel, _sequence_id, ticker = response
            return self.__parse_ticker(ticker)
        else:
            return None

    def __parse_ticker(self, ticker: list) -> dict:
        if type(ticker) == list and len(ticker) == 10:
            currency_pair_id = int(ticker[0])
            currency_pair = self.pair_ids[str(currency_pair_id)]
            if currency_pair in self.pairs_to_record:
                new_ticker = {}
                new_ticker['source'] = "poloniex"
                new_ticker['epoch'] = int(time.time())
                new_ticker['date'] = datetime.utcfromtimestamp(new_ticker['epoch']).strftime('%Y-%m-%d %H:%M:%S')
                new_ticker['pair'] = str(currency_pair)
                new_ticker['last'] = float(ticker[1])
                new_ticker['ask'] = float(ticker[2])
                new_ticker['bid'] = float(ticker[3])
                return new_ticker
            else:
                return None
        else:
            return None

    def __insert_ticker(self,ticker):
        insert_diff = ticker['epoch'] - self.last_insert_epoch[ticker['pair']]
        if (insert_diff > self.min_diff_to_insert):
            try:
                self.logger.debug(f"Poloniex:{ticker}")
                self.mongodb.insert("tickers",ticker)
                self.last_insert_epoch[ticker['pair']] = ticker['epoch']
            except Exception as e:
                self.logger.error("Error in  Database")
                raise e


    async def get_tickers(self,lock):
        ws_uri = "wss://api2.poloniex.com"
        channel = 1002
        
        
        while True:
            try:
                self.logger.debug("Starting connection with Poloniex")
                async with websockets.connect(ws_uri) as websocket:
                    data = {"command": "subscribe", "channel": channel}
                    await websocket.send(json.dumps(data))
                    while True:
                        try:
                            r = await websocket.recv()
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.error(
                                f"websockets.exceptions.ConnectionClosed:{e}>{traceback.format_exc()}")
                            break
                        except Exception as e:
                            self.logger.error(
                                f"Exception:{e}->{traceback.format_exc()}")
                            break
                        else:
                            response = json.loads(r)
                            ticker = self.__parse_ticker_response(response)
                            if ticker is not None:
                                try:
                                    async with lock:
                                        self.__insert_ticker(ticker)
                                except Exception as e:
                                    self.logger.error(f"Exception in Insert DataBase:{e}->{traceback.format_exc()}")
                                    return
                                

            except Exception as e:
                self.logger.error(f"Exception:{e}->{traceback.format_exc()}")

            self.logger.info("Connection lost with poloniex")
            await asyncio.sleep(5)

