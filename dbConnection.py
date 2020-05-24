import mysql.connector
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
        if self.__sqlConnect is None:
            self.__sqlConnect = mysql.connector.connect(**dbConfig)

    def getConnect(self):
        return self.__sqlConnect

    def alter(self, table, col, db_name=__db_name):
        cursor = self.__sqlConnect.cursor(buffered=True, dictionary=True)
        sql_select = """SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME='%s' 
                AND TABLE_SCHEMA='%s' 
                AND COLUMN_NAME='%s' LIMIT 1;""" % (
            table,
            db_name,
            col,
        )
        cursor.execute(sql_select)
        result = cursor.fetchone()
        if result['count'] == 0:
            LogHandler.log_msg("Alter column '%s' to table '%s'\n" % (
                col,
                table
            ))
            sql_alter = """ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s FLOAT NULL DEFAULT 0;""" % (
                table,
                col
            )
            cursor.execute(sql_alter)
        elif result['count'] > 0:
            return False
        elif result is None:
            return False
        return True

    def insert(self, table, cols, rows, **args):
        onDupUpdateKey = []
        # print(rows)
        for i in range(len(rows)):
            if cols[i] != 'ReportDate' and cols[i] != 'Code' and cols[i] != 'Date':
                onDupUpdateKey.append('%s=\'%s\'' % (cols[i], rows[i]))

        sql_insert = 'INSERT IGNORE INTO %s(%s) VALUES(%s) %s' % (
            table,
            ','.join(cols),
            ','.join(['%s'] * len(rows)),
            'ON DUPLICATE KEY UPDATE ' + ','.join(onDupUpdateKey),
        )

        if table in self.__table_debug:
            LogHandler.log_msg(args['org_dict'])

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

    def update(self, table, fields, conditions):
        return True

    def closeConnection(self):
        self.__sqlConnect.close()
        print('Connection closed')

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
