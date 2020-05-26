import mysql.connector
from dbConnectionPool import dbConnectionPool
from logHandler import LogHandler
from config import dbConfig


class dbConnection:
    __sqlConnect = None
    __db_name = None
    __table_debug = (
        'yahoofinance_stock_balance_sheet',
        'yahoofinance_stock_income_statement',
        'yahoofinance_stock_price',
        'yahoofinance_stock_valuation_measures'
    )

    def __init__(self):
        self.__db_name = dbConfig['database']
        LogHandler.log_msg("DB connection initializing...")
        if self.__sqlConnect is None:
            LogHandler.log_msg("Getting new connection..")
            self.__sqlConnect = mysql.connector.MySQLConnection(**dbConfig)
        self.__sqlConnect.autocommit = True
        LogHandler.log_msg("Done.")

    def getConnect(self):
        return self.__sqlConnect

    def insert(self, table, cols, rows, **args):
        onDupUpdateKey = []
        # print(rows)
        for i in range(len(rows)):
            if cols[i] != 'ReportDate' and cols[i] != 'Code' and cols[i] != 'Date' and cols[i] != 'ValuationMethod':
                onDupUpdateKey.append('%s=\'%s\'' % (cols[i], rows[i]))

        sql_insert = 'INSERT IGNORE INTO %s(%s) VALUES(%s) %s' % (
            table,
            ','.join(cols),
            ','.join(['%s'] * len(rows)),
            'ON DUPLICATE KEY UPDATE ' + ','.join(onDupUpdateKey),
        )
        try:
            self.__sqlConnect.cursor().execute(sql_insert, rows)
            # NB : you won't get an IntegrityError when reading
        except mysql.connector.Error as err:
            LogHandler.log_msg("""
                    Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
            """.format(args['filename'], sql_insert, err))

    def select(self, table, fields, conditions=None, **args):
        cursor = self.__sqlConnect.cursor(buffered=True, dictionary=True)
        sql_select = ''
        con_rows = []
        for i in range(len(conditions)):
            con_rows.append("{0}='{1}'".format(list(conditions.keys())[i], conditions[list(conditions.keys())[i]]))
        if conditions is not None:
            sql_select = 'SELECT %s from %s WHERE %s' % (
                fields,
                table,
                ' AND '.join(con_rows)
            )
        else:
            sql_select = 'SELECT %s from %s ' % (
                fields,
                table
            )

        try:
            cursor.execute(sql_select)
        except mysql.connector.Error as err:
            LogHandler.log_msg("""
                Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
            """.format(args['filename'], sql_select, err))

        record = cursor.fetchall()
        rows = []
        for row in record:
            rows.append(row['COLUMN_NAME'])
        return tuple(rows)

    def insert_update_from_dataframe(self, table, dataframe, **args):
        onDupUpdateKey = []

        cols = list(dataframe.columns)
        for c in list(dataframe.columns):
            if c != 'ReportDate' and c != 'Code' and c != 'Date' and c != 'ValuationMethod':
                onDupUpdateKey.append('%s=new.%s' % (c.replace(" ", ""), c.replace(" ", "")))

        sql_insert = 'INSERT IGNORE INTO %s(%s) VALUES(%s) AS new %s' % (
            table,
            ','.join([x.replace(" ", "") for x in cols]),
            ','.join(['%s'] * len(cols)),
            'ON DUPLICATE KEY UPDATE ' + ','.join(onDupUpdateKey),
        )
        try:
            self.__sqlConnect.cursor().executemany(
                sql_insert,
                [tuple(x) for x in dataframe.values])
        except mysql.connector.errors as err:
            LogHandler.log_msg("""
                    Parsing file {}\nSQL Query: {}\nSomething went wrong: {}\n
            """.format(args['filename'], sql_insert, err))

    def update(self, table, fields, conditions):
        return True

    def closeConnection(self):
        self.__sqlConnect.close()

    def loadModelProperties(self, mp):
        mp['yahoofinance_stock_balance_sheet'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_balance_sheet',
                'TABLE_SCHEMA': self.__db_name,
            })
        mp['yahoofinance_stock_income_statement'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_income_statement',
                'TABLE_SCHEMA': self.__db_name,
            })
        mp['yahoofinance_stock_price'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_price',
                'TABLE_SCHEMA': self.__db_name,
            })
        mp['yahoofinance_stock_valuation_measures'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_valuation_measures',
                'TABLE_SCHEMA': self.__db_name,
            })
        return mp
