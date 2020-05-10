import datetime, gc, os, re, time
from locale import *
from pathlib import Path
import pandas as pd
import numpy as np
from mytools import myTools

mytools = myTools()

setlocale(LC_NUMERIC, 'English_US')

print("Start...")
for root, directories, files in os.walk("csv"):
    mytools.log_msg("{0} files in directory".format(len(files)))
    for filename in files:
        # checkCSVInTable
        file_path = os.path.join(root, filename)
        csvdata = pd.read_csv(file_path).fillna(value='0')

        if csvdata.empty:
            continue

        dataset = csvdata.T
        args = []
        # print(csvdata)
        if mytools.checkIfIsDate(csvdata.columns):
            fileProperty = mytools.matchFile(filename, isPrice=False)
            dr = csvdata.to_dict()
            c = list(dr.keys())
            args = mytools.regroupRowsFromDict(dr=dr, c=c, fileProperty=fileProperty)
        else:
            fileProperty = mytools.matchFile(filename, isPrice=True)
            dr = dataset.to_dict()
            args = mytools.regroupRowsFromDictForPrice(dr=dr, fileProperty=fileProperty)
        
        print('Parsing file %s' % filename)
        # Do variable convertions
        mytools.saveData(args=args, table=fileProperty['table'])

        del dataset, csvdata, dr, args, fileProperty, file_path, filename
        gc.collect()

print("END")
mytools.closeDbConnection()

"""
        print("Proceeding file", filename, ", code:", code)
        dataset = pd.read_csv(file_path)

        data_values = dataset.dropna(axis=0, how='any').values

        print("Ready to insert ", len(data_values), " data")

        priceHelper.insertAllItem(data_values)

        print(priceHelper.getHandleCount(), " data inserted")

        historyObj = priceHelper.getAllPrice()
        historyJson = []

        for x in historyObj:
            historyJson.append(x.toServilize())

        del dataset
        del data_values
        gc.collect()
"""
