import re
import sqlalchemy
from sqlalchemy.exc import DBAPIError

from dbConnection import dbConnection
from logHandler import LogHandler
from dbConnectionEngine import dbConnectionEngine


class myTools:
    __con = None
    __mysqlCon = None
    mp = {
        'yahoofinance_stock_balance_sheet': (),
        'yahoofinance_stock_income_statement': (),
        'yahoofinance_stock_price': (),
        'yahoofinance_stock_valuation_measures': (),
    }

    def __init__(self):
        LogHandler.log_msg("Initializing tools..")
        self.__con = dbConnectionEngine()
        LogHandler.log_msg("Fetch current table attribute")
        self.__con.loadModelProperties(self.mp)
        LogHandler.log_msg("Done.")
        self.__mysqlCon = dbConnection()
    
    def matchFile(self, filename, **ar):
        property = {'cols': {}, 'rows': [], 'key': '', 'filename': filename}
        if ar['isPrice']:
            # 是否为 Table 4 的文件
            property['cols']['Code'] = filename.replace('.csv', '')
            property['table'] = 'yahoofinance_stock_price'
            property['key'] = 'price'
            return property

        # 是否为 Table 1 的结构
        searchObj = re.search(r'(.*)_(.*)_(.*)_(measures)', filename, re.M | re.I)
        if searchObj:
            # print ("match --> matchObj.group() : ", searchObj1.group())
            property['cols']['Code'] = searchObj.group(1)
            property['cols']['ValuationMethod'] = searchObj.group(2)
            property['cols']['ReportDate'] = ''
            property['key'] = searchObj.group(4)
            property['table'] = 'yahoofinance_stock_valuation_measures'
            return property

        # 是否为 Table 2 的结构
        searchObj = re.search(r'(.*)_(.*)_(financials)', filename, re.M | re.I)
        if searchObj:
            # print ("match --> matchObj.group() : ", searchObj1.group())
            property['cols']['Code'] = searchObj.group(1)
            property['cols']['ValuationMethod'] = searchObj.group(2)
            property['cols']['ReportDate'] = ''
            property['key'] = searchObj.group(3)
            property['table'] = 'yahoofinance_stock_income_statement'
            return property

        # 是否为 Table 3 的结构
        searchObj = re.search(r'(.*)_(.*)_(balance)', filename, re.M | re.I)
        if searchObj:
            # print ("match --> matchObj.group() : ", searchObj3.group())
            property['cols']['Code'] = searchObj.group(1)
            property['cols']['ReportDate'] = ''
            property['key'] = searchObj.group(3)
            property['table'] = 'yahoofinance_stock_balance_sheet'
            return property

    def getMp(self):
        return self.mp

    def checkIfIsDate(self, Columns):
        searchObj = re.search(r'(\d{2}/\d{2}/\d{4})|(ttm)', Columns[1], re.M | re.I)
        if searchObj:
            return True
        return False

    def convertNumberWithCommaInArray(self, arg):
        res = arg
        if type(arg) != float:
            res = arg.replace(',', '')
        return res

    def regroupRowsFromDict(self, dr, c, fileProperty):
        pass

    def regroupRowsFromDictForPrice(self, dr, fileProperty):
        pass

    def save(self, dataframe, table, filename="defualt"):
        self.__con.engine_insert_update(dataframe=dataframe, table=table, filename=filename)
        pass

    def save_using_mycon(self, table, df, filename="defualt"):
        for r in df.values:
            self.__mysqlCon.insert(table=table, dataframe=df, filename=filename)
        pass

    def saveDataFrame(self, dataframe, table):
        try:
            dataframe.to_sql(name=table, con=self.__con.getEngine(),
                             index=False,
                             if_exists='append',
                             dtype=self.__con.getDbType()[table])
        except DBAPIError as e:
            LogHandler.log_exceptions("Sql Exceptions: %s\r\n" % e)
            pass
        finally:
            pass

    # Development function DEBUG USAGE Not important
    def generateTabelOnConsole(self, table, df):
        fields = []
        args = []
        for x in self.__con.getDbType()[table]:
            type = self.__con.getDbType()[table][x]
            type_str = ''
            if type == sqlalchemy.types.Date:
                type_str = 'DATE PRIMARY KEY NOT NULL'
                args.append('`%s`' % x)
            elif type == sqlalchemy.types.String:
                type_str = 'VARCHAR(50) PRIMARY KEY NOT NULL'
                args.append('`%s`' % x)
            elif type == sqlalchemy.types.Float:
                type_str = 'FLOAT'
            fields.append(("`%s` %s" % (x, type_str)))
        print("""
            CREATE TABLE `%s`(
                %s,
                PRIMARY KEY (%s)
            )
        """ % (
            table,
            ",".join(fields),
            ",".join(args)
        ))
        pass
