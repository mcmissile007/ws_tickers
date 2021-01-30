
import asyncio
import websockets
import json
import time
import concurrent.futures
import traceback
from poloniex import Poloniex
from gemini import Gemini
from bittrex import Bittrex
import logging
from logging.handlers import TimedRotatingFileHandler
from config import Config
import os
import sys
from mongodb import MongoDataBase
from aiohttp import web


# https://hynek.me/articles/waiting-in-asyncio/


def configure_logging(name):
    # https://docs.python.org/3/howto/logging-cookbook.html

    logging.getLogger('websockets').setLevel(logging.INFO)
    logging.getLogger('asyncio').setLevel(logging.INFO)

    logger = logging.getLogger(name)  # by default root.
    '''
    The level set in the logger determines which severity of messages it will pass to its handlers.
    The level set in each handler determines which messages that handler will send on.
    '''
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '[%(asctime)s] {%(name)s:%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.info("Console logging configured OK")

    if (os.path.exists("./logs")):
        filename = "./logs/" + name + "-" + str(int(time.time())) +".log"
        file_hanlder = TimedRotatingFileHandler(
            filename, when="midnight", interval=1)
        file_hanlder.suffix = "%Y%m%d"
        file_hanlder.setLevel(logging.INFO)
        file_hanlder.setFormatter(formatter)
        logger.addHandler(file_hanlder)
        logger.info("File logging configured OK")
    else:
        logger.warning(
            "File logging not configured. Path ./log does not exist")

    return logger


async def state(request):
    logger.debug("Running:" + str(request))
    res = {'state': 1, 'epoch': int(time.time())}
    return web.json_response(res)



async def main():

    while True:

        logger.info("Start Main")
        lock = asyncio.Lock()

        database = MongoDataBase(Config.ticker_PRIORITY_EXCHANGE)

        app = web.Application()
        app.add_routes([web.get('/', state)])

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
       

        try:
            database.connect()
        except:
            logger.error("Error connecting to Data Base")
            await asyncio.sleep(5)
            continue

       

        pairs_to_record_tickers = [("USDT","BTC"),("USDT","ETH")]
        pairs_to_record_candles = [("USD","BTC"),("USD","ETH")]
        poloniex = Poloniex(database,pairs_to_record_tickers)
        gemini = Gemini(database,pairs_to_record_candles)
        bittrex = Bittrex(database,pairs_to_record_tickers)
        poloniex_task_tickers = asyncio.create_task(poloniex.get_tickers(lock))
        gemini_task_tickers = asyncio.create_task(gemini.get_tickers(lock))
        bittrex_task_tickers = asyncio.create_task(bittrex.get_tickers(lock))
        
        _done, pending = await asyncio.wait([poloniex_task_tickers, gemini_task_tickers,bittrex_task_tickers],return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()

        await runner.cleanup()
        logger.info("End Main")


if __name__ == "__main__":

   
    
    logger = configure_logging(Config.LOGGING_NAME)
    logger.info("Configure logging OK")
 
    asyncio.run(main())
  
