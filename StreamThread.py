from datetime import datetime
import time
import pandas as pd
from logHandler import LogHandler
from mytools import myTools


def streaming(sublist, threadCode):
    success_msg = ''
    LogHandler.log_msg("%s: %s" % (threadCode, time.ctime(time.time())))
    t_start = time.process_time_ns()
    mytools = myTools()
    number = 0
    data_count = 0
    file_nums = 0
    datamapping_performance = 0
    for filename in sublist:
        file_nums = len(sublist)
        number += 1
        LogHandler.log_msg('[%s]: Parsing file %s/%s %s\n' % (
            threadCode,
            number,
            len(sublist),
            filename,
        ))
        t3 = time.process_time_ns()

        # checkCSVInTable
        csvdata = pd.read_csv("csv/" + filename).fillna(value='0')
        if csvdata.empty:
            continue

        # Check file type
        if mytools.checkIfIsDate(csvdata.columns):
            fileProperty = mytools.matchFile(filename, isPrice=False)
            # Pre-processing data: remove fields which is not in sql table
            csvdata['name'] = csvdata['name'].str.strip()
            cols = csvdata['name'].str.strip()
            csvdata = csvdata.iloc[
                (csvdata['name'].isin(mytools.getMp()[fileProperty['table']])).index]

            # Remove ',' of each element and convert to type float
            dataframe = csvdata.T.iloc[1:] \
                .applymap(
                lambda x: float(x.replace(',', '')) if type(x) != float else float(x))

            # Rename columns' names to string
            dataframe.rename(columns=cols.to_dict(), inplace=True)

            # Remove unrelated table fields
            dataframe = dataframe.loc[:,
                        cols.loc[(cols.isin(mytools.getMp()[fileProperty['table']]))].str.strip().to_list()]

            # Append Code, ReportDate, ValuationMethod to dataframe
            dataframe['Code'] = fileProperty['cols']['Code']
            dataframe['ReportDate'] = csvdata.columns.to_series().iloc[1:].apply(
                lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d') if x != 'ttm' else '0000-00-00'
            )
            if 'ValuationMethod' in tuple(fileProperty['cols'].keys()):
                dataframe['ValuationMethod'] = fileProperty['cols']['ValuationMethod']

            # Save in Database
            mytools.save(dataframe=dataframe, table=fileProperty['table'], filename=filename)
            data_count += dataframe.size
        else:
            fileProperty = mytools.matchFile(filename, isPrice=True)
            csvdata.rename(
                columns={csvdata.columns[x]: csvdata.columns[x].replace(" ", "") for x in
                         range(csvdata.columns.size)}, inplace=True)

            # Append Code
            csvdata['Code'] = fileProperty['cols']['Code']

            # Save in Database
            mytools.save(dataframe=csvdata, table=fileProperty['table'], filename=filename)
            data_count += csvdata.size
        # END

        t4 = time.process_time_ns()
        LogHandler.log_msg('Finished in %sms\n' % round((t4 - t3) / 1000000, 5))

    t_end = time.process_time_ns()
    success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
    success_msg += "    END, total time: %sms\r\n" % round((t_end - t_start) / 1000000, 5)
    success_msg += "    Main thread performance average/file: %sms" % round((t_end - t_start) / 1000000 / file_nums, 5)
    success_msg += "    Parsed data: %s\r\n" % data_count
    success_msg += "    Performance average/data: %sms\r\n" % round((t_end - t_start) / 1000000 / data_count, 5)
    success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
    LogHandler.success(success_msg)
