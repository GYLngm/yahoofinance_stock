from datetime import datetime
import os
import time
from locale import *
import pandas as pd

from logHandler import LogHandler
from mytools import myTools


setlocale(LC_NUMERIC, 'English_US')

LogHandler.log_msg("Start...")
mytools = myTools()
number = 0
t_start = time.process_time_ns()
success_msg = ''
file_nums = 0
data_count = 0
datamapping_performance = 0.0
table_org_attributes = {}
pd.set_option('display.max_columns', 10)
for root, directories, files in os.walk("csv"):
    file_nums = len(files)
    LogHandler.log_msg("{0} files in directory".format(len(files)))
    for filename in files:
        number += 1
        LogHandler.log_msg('Parsing file %s/%s %s\n' % (
            number,
            len(files),
            filename,
        ))
        t3 = time.process_time_ns()
        file_path = os.path.join(root, filename)
        # open and extract data from csv and return dataframe object
        csvdata = pd.read_csv(file_path, parse_dates=True).fillna(value='0')
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
                lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d') if x != 'ttm' else '0000-00-00')
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

        t4 = time.process_time_ns()
        LogHandler.log_msg('Finished in %ss\n' % round((t4 - t3)/1000000, 5))
t_end = time.process_time_ns()
success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
success_msg += "    END, total time: %ss\r\n" % round((t_end - t_start)/1000000, 5)
success_msg += "    Main thread performance average/file: %ss" % round((t_end - t_start)/1000000 / file_nums, 5)
success_msg += "    Data mapping Performance average/file: %ss" % round(datamapping_performance/1000000 / file_nums, 5)
success_msg += "    Parsed data: %s\r\n" % data_count
success_msg += "    Performance average/data: %ss\r\n" % round((t_end - t_start)/1000000 / data_count, 5)
success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
LogHandler.success(success_msg)
