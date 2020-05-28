import mysql.connector
from logHandler import LogHandler
from config import dbConfig


class dbConnection:
    __sqlConnect = None
    __db_name = None

    def __init__(self):
        self.__db_name = dbConfig['database']
        if self.__sqlConnect is None:
            self.__sqlConnect = \
                mysql.connector.connect(**dbConfig)
        # self.loadModelProperties()

    def getConnect(self):
        return self.__sqlConnect

    def insert(self, table, dataframe, **args):
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
            row = [tuple(r) for r in dataframe.values]
            self.__sqlConnect.cursor().executemany(sql_insert, row)
            # NB : you won't get an IntegrityError when reading
            self.__sqlConnect.commit()
        except mysql.connector.Error as err:
            LogHandler.log_exceptions("""
                    Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
            """.format(args['filename'], sql_insert, err))
        finally:
            self.__sqlConnect.cursor().close()

    def select(self, table, fields, conditions=None, **args):
        pass

    def update(self, table, fields, conditions):
        return True

    def close(self):
        self.__sqlConnect.close()
