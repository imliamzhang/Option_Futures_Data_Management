import pandas as pd
import numpy as np
import csv
import os
import os.path
import datetime


class DataBase:

    def __init__(self, db_name, db_folder_path):
        self.name = db_name
        self.path = db_folder_path
        self.unique_char = len(self.name)  # Used to rule out files with wrong names. Ex: ._sseo_20200609.csv.

    def file_list_from_folder(self):
        """
        :returns file_folder: A list containing all the legal file names from the designated folder path.
        """

        rfiles = os.listdir(self.path)
        file_folder = []

        for r in rfiles:
            if r[:int(self.unique_char)] == str.lower(self.name):
                file_folder.append(r)

        file_folder.sort()
        return file_folder  #

    def folder_report(self):
        file_folder = self.file_list_from_folder()

        print(self.name + ':')
        print("# of files: " + str(len(file_folder)))
        print("First file: " + str(file_folder[0]))
        print("Last file: " + str(file_folder[-1]))

    def date_selector(self, start, end=None):
        """
        :param start: Start date. Format: YYYYMMDD
        :param end: Optional end date. Format: YYYYMMDD
        :return: A list consisted of the names of file within the range selected.
        """

        file_list = self.file_list_from_folder()
        date_specific_file_list = []
        db_name = self.name

        if end is None:
            file_name = db_name + '_' + start + '.csv'
            date_specific_file_list.append(file_name)
            return date_specific_file_list

        else:
            if int(start) > int(end):
                print('Start date is after the end date.')
                exit()

            elif int(start) == int(end):
                file_name = db_name + '_' + start + '.csv'
                date_specific_file_list.append(file_name)
                return date_specific_file_list

            else:
                file_name_start = db_name + '_' + start + '.csv'
                file_name_end = db_name + '_' + end + '.csv'

                start_index = file_list.index(file_name_start)
                end_index = file_list.index(file_name_end, start_index)

                while start_index <= end_index:
                    for i in file_list[start_index:end_index + 1]:
                        date_specific_file_list.append(i)
                        start_index += 1

                return date_specific_file_list

    def instrument_id(self, start=None, end=None):

        '''
        :return: Returns instrument_id as a list containing all instrument IDs from the DB folder's daily data files.
        '''

        if start is None:
            instrument_id = []
            file_name_list = self.file_list_from_folder()

            for f in file_name_list:  # Get file name
                # print (f)  # For debugging purpose - Display current file name
                f_read = pd.read_csv((self.path + f), error_bad_lines=False)  # Read the file from designated file path
                unique_id = f_read.InstrumentID.unique()  # Extract the unique instrument id as an array
                for i in unique_id:
                    if i not in instrument_id:
                        instrument_id.append(i)
                        # print (len(instrument_id))  # For debugging purpose - Tally # of instrument added

            return instrument_id

        else:
            instrument_id = []
            file_list_date_specific = self.date_selector(start, end)
            print(file_list_date_specific)

            for f in file_list_date_specific:  # Get file name
                # print (f)  # For debugging purpose - Display current file name
                f_read = pd.read_csv((self.path + f), error_bad_lines=False)  # Read the file from designated file path
                unique_id = f_read.InstrumentID.unique()  # Extract the unique instrument id as an array
                for i in unique_id:
                    if i not in instrument_id:
                        instrument_id.append(i)
                        # print (len(instrument_id))  # For debugging purpose - Tally # of instrument added

            return instrument_id

    def instrument_transaction_record(self, instrument_id, start=None, end=None):

        # Obtain instrument record from all files in the database:
        if start is None:
            # Create empty data frame, add date as the first column
            column_names = ["Date", "InstrumentID", "UpdateTime", "UpdateMillisec", "LastPrice", "Volume",
                            "OpenInterest", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1",
                            "UpperLimitPrice", "LowerLimitPrice", "OpenPrice", "ClosePrice", "HighestPrice",
                            "LowestPrice", "SecondOfDay", "Turnover"]
            instrument_record = pd.DataFrame(columns=column_names)
            instrument_id_list = self.instrument_id()  # Get all unique instrument IDs
            file_folder = self.file_list_from_folder()  # Get all file names from folder

            # Main function
            if instrument_id not in instrument_id_list:
                print("Instrument ID does not exist. Instrument may from other data bases.")

            else:
                print('Processing...')
                for f in file_folder:  # Iterate through files in SSEO folder

                    # Extract date from file name, convert to str
                    date = f[self.unique_char + 1:-4]  # self.unique_char+1: omit 'sseo_'; -4: omit '.csv'
                    date_time_obj = datetime.datetime.strptime(date, '%Y%m%d')
                    date_str = datetime.datetime.strftime(date_time_obj, '%Y-%m-%d')
                    # print (date_str)

                    # Read file
                    f_read = pd.read_csv((self.path + f), error_bad_lines=False)
                    # For each file, check the passed argument is in the file and get the index
                    match_index = f_read.index[f_read['InstrumentID'] == instrument_id].tolist()
                    for i in match_index:
                        # Append instrument data
                        instrument_row_data = f_read.loc[i]  # Read the data from given index
                        instrument_row_dframe = pd.DataFrame(instrument_row_data).transpose()  # Convert series to df
                        instrument_row_dframe['Date'] = date_str  # add date to the data
                        instrument_record = instrument_record.append(instrument_row_dframe, ignore_index=True)
                        # print (instrument_record)

                return instrument_record

        # Obtain instrument record from only the selected dates' files in the database:
        else:
            file_list_date_specific = self.date_selector(start, end)

            # Create empty data frame, add date as the first column
            column_names = ["Date", "InstrumentID", "UpdateTime", "UpdateMillisec", "LastPrice", "Volume",
                            "OpenInterest", "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1",
                            "UpperLimitPrice", "LowerLimitPrice", "OpenPrice", "ClosePrice", "HighestPrice",
                            "LowestPrice", "SecondOfDay", "Turnover"]
            instrument_record = pd.DataFrame(columns=column_names)
            instrument_id_list = self.instrument_id()  # Get all unique instrument IDs
            file_folder = self.file_list_from_folder()  # Get all file names from folder

            # Main function
            if instrument_id not in instrument_id_list:
                print("Instrument ID does not exist. Instrument may from other data bases.")

            else:
                print('Processing...')
                for f in file_list_date_specific:  # Iterate through files in SSEO folder

                    # Extract date from file name, convert to str
                    date = f[self.unique_char + 1:-4]  # self.unique_char+1: omit 'sseo_'; -4: omit '.csv'
                    date_time_obj = datetime.datetime.strptime(date, '%Y%m%d')
                    date_str = datetime.datetime.strftime(date_time_obj, '%Y-%m-%d')
                    # print (date_str)

                    # Read file
                    f_read = pd.read_csv((self.path + f), error_bad_lines=False)
                    # For each file, check the passed argument is in the file and get the index
                    match_index = f_read.index[f_read['InstrumentID'] == instrument_id].tolist()
                    for i in match_index:
                        # Append instrument data
                        instrument_row_data = f_read.loc[i]  # Read the data from given index
                        instrument_row_dframe = pd.DataFrame(instrument_row_data).transpose()  # Convert series to df
                        instrument_row_dframe['Date'] = date_str  # add date to the data
                        instrument_record = instrument_record.append(instrument_row_dframe, ignore_index=True)
                        # print (instrument_record)

                return instrument_record