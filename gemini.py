import logging
import asyncio
import websockets
import json
import traceback
import sys
import time
from collections import defaultdict
from pymongo import MongoClient
from database import DataBase
from datetime import datetime

from config import Config
from exchange import Exchange

class Gemini(Exchange):

    def __init__(self, database: DataBase, pairs_to_record: list):

        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.logger.debug(f"Init {str(__name__)}")
        
        self.database = database
        self.logger.debug(f"database id:{id(self.database)}")

        self.pairs_to_record = [quote + base for base,quote in pairs_to_record]
        self.candles_type = "candles_5m"


    def __parse_candle_response(self, response: dict) -> dict:

        if type(response) == dict and all( (key in response for key in ['type',"symbol","changes"]) ):
            if response['type'] == self.candles_type + "_updates":
                if response['symbol'] in self.pairs_to_record:
                    changes = list(response['changes'])
                    return self.__parse_candle(response['symbol'],changes[0])
        else:
            return None
    def __parse_candle(self,pair: str, candle: list) -> dict:
        if type(candle) == list and len(candle) == 6:
            new_candle = {}
            new_candle['source'] = "gemini"
            new_candle['frame'] = self.candles_type.split("_")[1]
            new_candle['epoch'] = int(candle[0]/1000) #in seconds
            new_candle['date'] = datetime.utcfromtimestamp(new_candle['epoch']).strftime('%Y-%m-%d %H:%M:%S')
            new_candle['pair'] = str(pair)
            new_candle['open'] = float(candle[1])
            new_candle['high'] = float(candle[2])
            new_candle['low'] = float(candle[3])
            new_candle['close'] = float(candle[4])
            new_candle['volume'] = float(candle[5])
            return new_candle
        else:
            return None
    
    async def get_tickers(self,lock):
        ws_uri = "wss://api.gemini.com/v2/marketdata"
        while True:
            try:
                self.logger.debug("Starting connection with Gemini")
                async with websockets.connect(ws_uri) as websocket:
                    data = {"type": "subscribe","subscriptions":[{"name":self.candles_type,"symbols":self.pairs_to_record}]}
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
                            #self.logger.debug(f"Gemini:{response}")
                            candle = self.__parse_candle_response(response)
                            if candle is not None:
                                self.logger.debug(f"Gemini candle:{candle}")
                                try:
                                    async with lock:
                                        self.database.insert("candles",candle)
                                except Exception as e:
                                    self.logger.error(f"Exception in Insert DataBase:{e}->{traceback.format_exc()}")
                                    return
            except Exception as e:
                self.logger.error(f"Exception:{e}->{traceback.format_exc()}")

            self.logger.info("Connection lost with Gemini")
            await asyncio.sleep(5)

