import pymysql
import logging
import traceback

from singleton import Singleton
from database_config import MysqlDataBaseConfig
from config import Config



class MysqlDataBase(metaclass=Singleton):

    def __init__(self, ticker_priority_exchange):
        self.logger = logging.getLogger(
            Config.LOGGING_NAME + "." + str(__name__))
        self.connection = None
        self.ticker_priority_exchange = ticker_priority_exchange
        self.last_insert = 0



    def __parse_sql(self,table : str,row : dict):
        if type(table) != str and type(row) != dict:
            return False
        sql = "insert into " + table + " ("
        row = {k:v for k,v in row.items() if type(k) == str and not k.startswith("_")}
        for key in row:
            sql += key + ","
        sql = sql[:-1]
        sql += ") values("
        for key in row:
            if type(row[key]) == str:
                sql += "'" + row[key] + "',"
            else:
                sql += str(row[key]) + ","
        sql = sql[:-1]
        sql += ")"
        return sql

    

    def insert(self, table, row):

        if table == "tickers" and row["source"] != self.ticker_priority_exchange:
            diff = row["epoch"] - self.last_insert
            if diff < 60:
                self.logger.debug(f"recent ticker insert {diff}s from priority exchange {self.ticker_priority_exchange}, discard ticker insert from {row['source']}")
                return
        if self.connection != None:
            lastrowid = -1
            try:
                sql = self.__parse_sql(table,row)
                self.logger.debug(sql)
                if sql:
                    with self.connection.cursor() as cursor:
                        cursor.execute(sql)
                        lastrowid = cursor.lastrowid
                    self.connection.commit()

            except pymysql.OperationalError as error:
                self.logger.error(
                    f"OperationalError ticker:{error}->{traceback.format_exc()}")
                raise e

            except Exception as e:
                self.logger.error(
                    f"Error insert mysql ticker:{e}->{traceback.format_exc()}")
                raise e
            else:
                self.logger.debug(f"insert ticker result:{lastrowid}")
                if table == "tickers" and row["source"] == self.ticker_priority_exchange:
                    self.last_insert = row["epoch"]
                return

    def close(self):
        self.connection.close()
        self.connection = None
     

    def connect(self):
       
        try:
            self.connection = pymysql.connect(host=MysqlDataBaseConfig.host,
                                        port=MysqlDataBaseConfig.port,
                                        user=MysqlDataBaseConfig.user,
                                        password=MysqlDataBaseConfig.password,
                                        db=MysqlDataBaseConfig.database,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor)
        except Exception as e:
            self.logger.error(
                f"Mysql Connection ERROR:{e}->{traceback.format_exc()}")
            raise e



 
