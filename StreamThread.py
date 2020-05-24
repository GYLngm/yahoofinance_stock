import os
import time
import pandas as pd
from logHandler import LogHandler
from mytools import myTools


class StreamThread:

    def streaming(self, sublist, threadCode):
        print("%s: %s" % (threadCode, time.ctime(time.time())))
        t_start = time.process_time()
        mytools = myTools()
        number = 0
        datamapping_performance = 0
        for filename in sublist:
            t3 = time.process_time()
            number += 1
            # checkCSVInTable
            csvdata = pd.read_csv("csv/"+filename).fillna(value='0')
            if csvdata.empty:
                continue

            dataset = csvdata.T
            t1 = time.process_time()
            if mytools.checkIfIsDate(csvdata.columns):
                fileProperty = mytools.matchFile(filename, isPrice=False)
                dr = csvdata.to_dict()
                c = list(dr.keys())
                args = mytools.regroupRowsFromDict(dr=dr, c=c, fileProperty=fileProperty)
                t2 = time.process_time()
                datamapping_performance += (t2 - t1)
                LogHandler.log_msg("Mapping data in %ss" % round(t2 - t1, 4))
            else:
                fileProperty = mytools.matchFile(filename, isPrice=True)
                dr = dataset.to_dict()
                args = mytools.regroupRowsFromDictForPrice(dr=dr, fileProperty=fileProperty)
                t2 = time.process_time()
                datamapping_performance += (t2 - t1)
                LogHandler.log_msg("Mapping data in %ss" % round(t2 - t1, 4))

            # Do variable convertions
            mytools.saveData(args=args, table=fileProperty['table'])
            t4 = time.process_time()
            LogHandler.log_msg('Parsing file %s/%s %s in %ss\n' % (
                number,
                len(sublist),
                filename,
                (t4 - t3)
            ))
        mytools.commitTransactions()
        t_end = time.process_time()
        LogHandler.log_msg("END, total time: %ss" % (t_end - t_start))
        LogHandler.log_msg("Main thread performance average / file: %ss" %
                           round((t_end - t_start) / len(sublist), 3)
                           )
        LogHandler.log_msg("Data mapping Performance average / file: %ss" %
                           round(datamapping_performance / len(sublist), 3)
                           )
