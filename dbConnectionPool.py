import mysql.connector
from mysql.connector import pooling
from logHandler import LogHandler
from config import dbConfig


class dbConnectionPool:

    def __init__(self):
        self.__db_name = dbConfig['database']
        LogHandler.log_msg("Create connection pool..")
        self.__cnxPool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=4,
            **dbConfig)
        LogHandler.log_msg("Done.")

    def getPoolConnect(self):
        return self.__cnxPool.get_connection()

