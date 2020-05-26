import pandas as pd

from dbConnectionPool import dbConnectionPool
from logHandler import LogHandler
from config import dbConfig, db_field_types
from sqlalchemy import create_engine


class dbConnectionEngine:
    __db_name = None
    __table_debug = (
        'yahoofinance_stock_balance_sheet',
        'yahoofinance_stock_income_statement',
        'yahoofinance_stock_price',
        'yahoofinance_stock_valuation_measures'
    )
    __engine = None
    __config = dbConfig
    __db_type = db_field_types

    def __init__(self):
        self.__db_name = dbConfig['database']
        LogHandler.log_msg("DB connection initializing...")
        self.__engine = create_engine('mysql://%s:%s@%s:%s/%s' % (
            self.__config['user'],
            self.__config['password'],
            self.__config['host'],
            self.__config['port'],
            self.__config['database'],
        ), echo=False)
        LogHandler.log_msg("Done.")

    def getEngine(self):
        return self.__engine

    def getDbType(self):
        return self.__db_type

    def insert(self, table, cols, rows, **args):
        pass

    def select(self, table, fields, conditions=None, **args):
        pass

    def update(self, table, fields, conditions):
        return True

    def loadModelProperties(self, mp):
        for x in mp:
            mp[x] = tuple(self.__db_type[x].keys())
        return mp

    def getConfig(self):
        return self.__config
