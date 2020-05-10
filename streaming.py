import datetime, gc, os, re, time
from locale import *
from pathlib import Path
import pandas as pd
import numpy as np
from mytools import myTools
from logHandler import LogHandler

mytools = myTools()

setlocale(LC_NUMERIC, 'English_US')

print("Start...")
number = 0
for root, directories, files in os.walk("csv"):
    LogHandler.log_msg("{0} files in directory".format(len(files)))
    for filename in files:
        number += 1
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
        
        print('Parsing file %s/%s %s \n' % (
            number,
            len(files),
            filename
        ))

        # Do variable convertions
        mytools.saveData(args=args, table=fileProperty['table'])

        del dataset, csvdata, dr, args, fileProperty, file_path, filename
        gc.collect()

print("END")
mytools.closeDbConnection()
