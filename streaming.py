import gc
import os
import time
from locale import *
import pandas as pd

from logHandler import LogHandler
from mytools import myTools

mytools = myTools()

setlocale(LC_NUMERIC, 'English_US')

LogHandler.log_msg("Start...")
number = 0
t_start = time.process_time()
file_nums = 0
for root, directories, files in os.walk("csv"):
    file_nums = len(files)
    LogHandler.log_msg("{0} files in directory".format(len(files)))
    for filename in files:
        t3 = time.process_time()
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

        # Do variable convertions
        mytools.saveData(args=args, table=fileProperty['table'])
        t4 = time.process_time()
        LogHandler.log_msg('Parsing file %s/%s %s in %ss\n' % (
            number,
            len(files),
            filename,
            (t4 - t3)
        ))

t_end = time.process_time()
LogHandler.log_msg("END, total time: %ss" % (t_end-t_start))
LogHandler.log_msg("Performance average: %ss" % round((t_end-t_start)/file_nums, 3))
mytools.closeDbConnection()
