from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError

from logHandler import LogHandler
from orm.dbconfig import dbConfig


class dbConnection:
    engine = None
    __db_name = None

    def __init__(self):
        self.__db_name = dbConfig['database']
        LogHandler.log_msg("DB engine initializing...")
        try:
            self.engine = create_engine(create_engine('mysql://%s:%s@%s:%s/%s' % (
                dbConfig['user'],
                dbConfig['password'],
                dbConfig['host'],
                dbConfig['port'],
                dbConfig['database'],
            ), echo=False))
        except DBAPIError as err:
            LogHandler.log_exceptions(err)
        finally:
            LogHandler.log_msg("Done.")

    def getConnect(self):
        return self.engine

    def insert(self, **args):
        pass

    def select(self, **args):
        pass

    def insert_update_from_dataframe(self, **args):
        pass

    def update(self, table, fields, conditions):
        pass

