import re
import time
import os
from datetime import datetime
from logHandler import LogHandler
import numpy as np

from dbConnection import dbConnection


class myTools:
    __con = None

    def __init__(self):
        self.__con = dbConnection()
    
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
        args = []
        for i in range(1, len(c)):
            tmp = {}
            for x in range(len(dr[c[i]])):
                mp = self.__con.getModelProperties()
                if dr['name'][x].strip() in mp[fileProperty['table']]:
                    tmp[dr['name'][x].strip()] = self.convertNumberWithCommaInArray(dr[c[i]][x])
                    tmp['Code'] = fileProperty['cols']['Code']
                    if c[i] == 'ttm':
                        tmp['ReportDate'] = time.strftime("%Y-%m-%d", time.localtime())
                    else:
                        tmp['ReportDate'] = datetime.strptime(c[i], '%m/%d/%Y').strftime('%Y-%m-%d')
                    if fileProperty['key'] != 'balance':
                        tmp['ValuationMethod'] = fileProperty['cols']['ValuationMethod']
                else:
                    # print(dr['name'][x].strip())
                    LogHandler.log_properties(filename=fileProperty['filename'], p=dr['name'][x].strip())
            args = np.append(args, tmp)
        return args

    def regroupRowsFromDictForPrice(self, dr, fileProperty):
        args = []
        for i in range(len(dr)):
            dr[i]['Code'] = fileProperty['cols']['Code']
            tmp = {}
            for x in range(len(dr[i])):
                key = list(dr[i].keys())[x].replace(' ', '')
                tmp[key] = dr[i][list(dr[i].keys())[x]]
            args = np.append(args, tmp)
        return args

    def saveData(self, args, table):
        flag = False
        """"
        for c in range(len(list(args[0].keys()))):
            self.__con.alter(table=table, col=list(args[0].keys())[c])
        """
        for i in range(len(args)):
            flag = self.__con.insert(table=table, cols=list(args[i].keys()), rows=list(args[i].values()))
        self.__con.getConnect().commit()

    def closeDbConnection(self):
        self.__con.closeConnection()