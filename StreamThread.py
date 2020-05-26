from datetime import datetime
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
            LogHandler.log_msg('Parsing file %s/%s %s\n' % (
                number,
                len(sublist),
                filename,
            ))
            # checkCSVInTable
            csvdata = pd.read_csv("csv/"+filename).fillna(value='0')

            if csvdata.empty:
                continue

            # Check file type
            if mytools.checkIfIsDate(csvdata.columns):
                fileProperty = mytools.matchFile(filename, isPrice=False)
                # Pre-processing data: remove fields which is not in sql table
                csvdata = csvdata.loc[
                    (csvdata['name'].str.strip().isin(mytools.getMp()[fileProperty['table']]))
                ]

                # Fetch table fields from origin csv dataframe
                cols = csvdata['name'].str.strip()  # Series Obj

                # Remove ',' of each element and convert to type float
                dataframe = csvdata.T.iloc[1:] \
                    .applymap(
                    lambda x: x.replace(',', '') if type(x) == str else x
                )

                # Rename columns' names to string
                dataframe.rename(columns=cols.to_dict(), inplace=True)

                # Append Code, ReportDate, ValuationMethod to dataframe
                dataframe['Code'] = fileProperty['cols']['Code']
                dataframe['ReportDate'] = csvdata.columns.to_series().iloc[1:].apply(
                    lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d') if x != 'ttm' else '0000-00-00'
                )
                if 'ValuationMethod' in tuple(fileProperty['cols'].keys()):
                    dataframe['ValuationMethod'] = fileProperty['cols']['ValuationMethod']

                # Save in Database
                mytools.saveDataFrame(dataframe, fileProperty['table'])
            else:
                fileProperty = mytools.matchFile(filename, isPrice=True)

                # Save in Database
                mytools.saveDataFrame(csvdata, fileProperty['table'])
            # END

            t4 = time.process_time()
            LogHandler.log_msg('Finished in %ss\n' % (t4 - t3))

        t_end = time.process_time()
        LogHandler.log_msg("END, total time: %ss" % (t_end - t_start))
        LogHandler.log_msg("Main thread performance average / file: %ss" %
                           round((t_end - t_start) / len(sublist), 3)
                           )
        LogHandler.log_msg("Data mapping Performance average / file: %ss" %
                           round(datamapping_performance / len(sublist), 3)
                           )
