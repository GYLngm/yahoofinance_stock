from datetime import datetime
import os
import time
from locale import *
import pandas as pd

from logHandler import LogHandler
from mytools import myTools

setlocale(LC_NUMERIC, 'English_US')
pd.set_option('display.max_columns', 10)

# Global variables
mytools, table_org_attributes, success_msg = myTools(), {}, ''
number, file_nums, single_file_data_count, data_count, datamapping_performance = 1, 0, 0, 0, 0.0

# Program start here
LogHandler.log_msg("Start...")
t_start = time.process_time_ns()
for root, directories, files in os.walk("csv"):
    file_nums = len(files)
    LogHandler.log_msg("{0} files in directory".format(len(files)))
    for filename in files:

        LogHandler.log_msg('Extracting from files %s/%s %s\n' % (
            number,
            len(files),
            filename))

        t3 = time.process_time_ns()
        file_path = os.path.join(root, filename)
        # open and extract data from csv and return dataframe object
        csvdata = pd.read_csv(file_path, parse_dates=True)
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
                lambda x: float(x.replace(',', '')) if type(x) != float and x != 'Null' else x)

            # Rename columns' names to string
            dataframe.rename(columns=cols.to_dict(), inplace=True)

            # Remove unrelated table fields
            dataframe = dataframe.loc[:,
                        cols.loc[(cols.isin(mytools.getMp()[fileProperty['table']]))].str.strip().to_list()]

            # Remove duplcated columns
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

            # Append Code, ReportDate, ValuationMethod to dataframe
            dataframe['Code'] = fileProperty['cols']['Code']
            dataframe['ReportDate'] = csvdata.columns.to_series().iloc[1:].apply(
                lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%Y-%m-%d') if x != 'ttm' else '0000-00-00')
            if 'ValuationMethod' in tuple(fileProperty['cols'].keys()):
                dataframe['ValuationMethod'] = fileProperty['cols']['ValuationMethod']

            dataframe = dataframe.where(dataframe.notna(), None)
            # Save in Database
            mytools.save(dataframe=dataframe, table=fileProperty['table'], filename=filename)
            # mytools.save_using_mycon(table=fileProperty['table'], df=dataframe, filename=filename)
            single_file_data_count = dataframe.size
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
            # mytools.save_using_mycon(table=fileProperty['table'], df=csvdata, filename=filename)
            single_file_data_count = csvdata.size
            data_count += single_file_data_count

        t4 = time.process_time_ns()
        LogHandler.log_msg('%s data parsed, finished in %sms\n' % (single_file_data_count, round((t4 - t3) / 1000000, 5)))
t_end = time.process_time_ns()
success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
success_msg += "    END, total time: %sms\r\n" % round((t_end - t_start) / 1000000, 5)
success_msg += "    Main thread performance average/file: %sms\r\n" % round((t_end - t_start) / 1000000 / file_nums, 5)
success_msg += "    Parsed data: %s\r\n" % data_count
success_msg += "    Performance average/data: %sms\r\n" % round((t_end - t_start) / 1000000 / data_count, 5)
success_msg += "\r\n-------------------------------------------------------------------------------\r\n"
LogHandler.success(success_msg)
# End
