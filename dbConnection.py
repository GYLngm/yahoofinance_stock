import mysql.connector

class dbConnection:
    __sqlConnect = None
    __db_name = None
    __model_properties = {
            'yahoofinance_stock_balance_sheet': (),
            'yahoofinance_stock_income_statement': (),
            'yahoofinance_stock_price': (),
            'yahoofinance_stock_valuation_measures': ()
        }

    def __init__(self, host, user, password, database):
        self.__db_name = database
        config = {
            'host': host,
            'user': user,
            'password': password,
            'database': self.__db_name,
        }
        if self.__sqlConnect is None:
            self.__sqlConnect = mysql.connector.connect(**config)
        self.loadModelProperties()

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
            print("Alter column '%s' to table '%s'" % (
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

    def insert(self, table, cols, rows):     
        onDupUpdateKey = []
        # print(rows)
        for i in range(len(rows)):
            onDupUpdateKey.append('%s=\'%s\'' % (cols[i], rows[i]))
        sql_insert = 'INSERT IGNORE INTO %s(%s) VALUES(%s) %s' % (
            table,
            ','.join(cols),
            ','.join(['%s']*len(rows)),
            'ON DUPLICATE KEY UPDATE '+','.join(onDupUpdateKey)
        )

        # print(sql_insert)
        flag = self.__sqlConnect.cursor().execute(sql_insert, rows)
        return flag

    def select(self, table, fields, conditions=None):
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
        cursor.execute(sql_select)
        record = cursor.fetchall()
        rows = []
        for row in record:
            rows.append(row['COLUMN_NAME'])
        return tuple(rows)

    def update(self, table, fields, conditions):
        return True

    def getModelProperties(self):
        return self.__model_properties

    def closeConnection(self):
        self.__sqlConnect.close()
        print('Connection closed')

    def loadModelProperties(self):
        self.__model_properties['yahoofinance_stock_balance_sheet'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_balance_sheet',
                'TABLE_SCHEMA': self.__db_name,
            })
        self.__model_properties['yahoofinance_stock_income_statement'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_income_statement',
                'TABLE_SCHEMA': self.__db_name,
            })
        self.__model_properties['yahoofinance_stock_price'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_price',
                'TABLE_SCHEMA': self.__db_name,
            })
        self.__model_properties['yahoofinance_stock_valuation_measures'] = self.select(
            'INFORMATION_SCHEMA.COLUMNS',
            'COLUMN_NAME',
            {
                'TABLE_NAME': 'yahoofinance_stock_valuation_measures',
                'TABLE_SCHEMA': self.__db_name,
            })
