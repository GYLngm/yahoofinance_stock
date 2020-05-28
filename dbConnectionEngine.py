from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, OperationalError, DataError
from sqlalchemy.pool import QueuePool
import pymysql

from logHandler import LogHandler
from config import dbConfig, db_field_types


class dbConnectionEngine:
    __db_name = None
    __engine = None
    __config = dbConfig
    __db_type = db_field_types

    def __init__(self):
        self.__db_name = dbConfig['database']
        LogHandler.log_msg("DB connection initializing...")
        self.__engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=%s' % (
            self.__config['user'],
            self.__config['password'],
            self.__config['host'],
            self.__config['port'],
            self.__config['database'],
            self.__config['charset'],
        ), echo=False)
        LogHandler.log_msg("Done.")

    def getEngine(self):
        return self.__engine

    def getDbType(self):
        return self.__db_type

    def engine_insert_update(self, dataframe, table, **args):
        onDupUpdateKey = []

        for c in dataframe.columns:
            if c != 'ReportDate' and c != 'Code' and c != 'Date' and c != 'ValuationMethod':
                onDupUpdateKey.append('%s=VALUES(%s)' % (c.replace(" ", ""), c.replace(" ", "")))

        sql_insert = 'INSERT INTO %s(%s) VALUES(%s) %s' % (
            table,
            ','.join(dataframe.columns),
            ','.join(['%s'] * len(dataframe.columns)),
            'ON DUPLICATE KEY UPDATE ' + ','.join(onDupUpdateKey),
        )

        try:
            with self.__engine.connect() as con:
                row = [tuple(x) for x in dataframe.values]
                con.execute(sql_insert, *row)
        except DBAPIError as err:
            LogHandler.log_exceptions("""
                            Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
                    """.format(args['filename'], sql_insert, err))
        except DataError as e:
            LogHandler.log_exceptions("""
                                        Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
                                """.format(args['filename'], sql_insert, e))
        except OperationalError as ope:
            LogHandler.log_exceptions("""
                                        Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
                                """.format(args['filename'], sql_insert, ope))
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
