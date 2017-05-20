#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  5 13:10:24 2016

@author: zack
"""

# this module deals with the wx data
import Utils
import csv

# some string literals
WX_KEY = "weather"
TEMP_KEY = "temp"
PM_KEY = "pm2.5"

class WxData:
    def __init__(self):
        self.wx_data = self.gen_data_dict_by_time_slice()
        
    def gen_data_dict_by_time_slice(self):
        empty_dict = dict()
        for time_slice in range(1, Utils.TIME_SLICE_NUMBER_DAY + 1):
            empty_dict[time_slice] = dict() # data struct for each slice is a dict    
        return empty_dict
        
    def load_wx_data_by_day(self, file_path, weather_date):
        with open(file_path, newline='') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=Utils.GLOBAL_DELIMITER)
            for data_row in csv_reader:
                date_time = data_row[0].split(Utils.SECONDARY_DELIMITER)
                if weather_date != date_time[0]:
                    continue
                time_slice_id = Utils.get_time_slice_id(date_time[1])
                if Utils.DEFAULT_TIME_SLICE_ID == time_slice_id:
                    continue
                if len(self.wx_data[time_slice_id]) != 0: # there has been data in the time slice
                    continue
                wx_data_dict = dict() # make a dict to store the wx content
                wx_data_dict[WX_KEY] = int(data_row[1]) # the value is of int
                wx_data_dict[TEMP_KEY] = float(data_row[2]) # the value is of double
                wx_data_dict[PM_KEY] = float(data_row[3]) # the value is of double
                
                self.wx_data[time_slice_id] = wx_data_dict
    
    def proc_wx_data_by_day(self):
        for time_slice_key in sorted(self.wx_data.keys()):
            # the wx_data_dict could be null, if there is not weather data provided for this time slice
            # then we use the data of neighbouring slices
            if len(self.wx_data[time_slice_key]) == 0:
                self.update_time_slice_data(time_slice_key)
                
    def update_time_slice_data(self, time_slice_key):
        updated = False
        # firstly we try to use the data from one of the previous slices
        for step in range(1, Utils.TIME_SLICE_NUMBER_HOUR):
            if time_slice_key - step <= 0:
                break
            if len(self.wx_data[time_slice_key - step]) != 0:
                self.wx_data[time_slice_key] = self.wx_data[time_slice_key - step]
                updated = True
                break         
        # if no available data in pre slices, we try to use what inside next slices
        for step in range(1, Utils.TIME_SLICE_NUMBER_HOUR):
            if updated:
                break            
            if time_slice_key + step > Utils.TIME_SLICE_NUMBER_DAY:
                break
            if len(self.wx_data[time_slice_key + step]) != 0:
                self.wx_data[time_slice_key] = self.wx_data[time_slice_key + step]
                updated = True
                break

        # when no available data in neighbouring slices, we have to set it use default value
        if not updated:
            self.wx_data[time_slice_key][WX_KEY] = 1
            self.wx_data[time_slice_key][TEMP_KEY] = 20.0
            self.wx_data[time_slice_key][PM_KEY] = 50.0
            
    def write_to_file(self, file_path, delimiter_string):
        with open(file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=delimiter_string)
            for time_slice_key in self.wx_data.keys():
                csv_row = [time_slice_key]
                csv_row.append(str(self.wx_data[time_slice_key][WX_KEY]))
                csv_row.append(str(self.wx_data[time_slice_key][TEMP_KEY]))
                csv_row.append(str(self.wx_data[time_slice_key][PM_KEY]))
                csv_writer.writerow(csv_row)
                    
            
            