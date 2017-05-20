#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  4 19:53:08 2016

@author: zack
"""

# This module deals with the traffic data
import Utils
import csv

class TrafficData:
    jam_lvl = 4 # indicates the levels of the conjestion
    
    def __init__(self, cluster_map):
        self.traffic_data = self.gen_traffic_data_dict_by_district(cluster_map) # empty data dictionary
        self.cls_map = cluster_map
 
    # GENERATE an empty dictionary to store orders by time slice
     # return: dict(time_slice_id->empty_list_orders)
    def gen_traffic_data_dict_by_time_slice(self):
        empty_dict = dict()
        for time_slice in range(1,Utils.TIME_SLICE_NUMBER_DAY + 1):
            empty_dict[time_slice] = dict() 
        return empty_dict       
    
    # GENERATE an empty dictionary to store orders by district
    # return: dict(district_id -> dict(time_slice_id->empty_list_orders))
    def gen_traffic_data_dict_by_district(self, cluster_map):
        empty_dict = dict()
        for district_id in list(cluster_map.values()):
            empty_dict[district_id] = self.gen_traffic_data_dict_by_time_slice()
        return empty_dict
        
#    def dist_hash_to_id(self, dist_hash, cluster_map):
#        return cluster_map.get(dist_hash, Utils.DEFAULT_DISTRICT_ID)
        
    def load_traffic_data_by_day(self, file_path, traffic_date):
        with open(file_path, newline='') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=Utils.GLOBAL_DELIMITER)
            for data_row in csv_reader:
                dist_id = Utils.dist_hash_to_id(data_row[0], self.cls_map)
                if Utils.DEFAULT_DISTRICT_ID == dist_id:
                    continue
                date_time = data_row[len(data_row) - 1].split(Utils.SECONDARY_DELIMITER)
                if traffic_date != date_time[0]:
                    continue
                time_slice_id = Utils.get_time_slice_id(date_time[1])
                if Utils.DEFAULT_TIME_SLICE_ID == time_slice_id:
                    continue
                # when already have data for the same slice, just ignore it
                if len(self.traffic_data[dist_id][time_slice_id]) == 0:
                    tmp_jam_dict = dict()
                    for iter_i in range(1, len(data_row) - 1):
                        trfData = data_row[iter_i].split(Utils.JAM_DELIMITER)
                        tmp_jam_dict[int(trfData[0])] = int(trfData[1])
                    
                    self.traffic_data[dist_id][time_slice_id] = tmp_jam_dict
            
    def proc_traffic_data_by_day(self):
        for dist_id in self.traffic_data.keys():
            for time_slice in sorted(self.traffic_data[dist_id].keys()):
                # replace the original data with a dict
                self.proc_traffic_lvl(dist_id, time_slice)
    
    # take jam lvl per time, return a dict standing for 
    # the number of roads at each jam lvl and the total jam score in int type       
    def proc_traffic_lvl(self, dist_key, time_slice_key): # trf_per_time is a list of pairs
        if len(self.traffic_data[dist_key][time_slice_key]) == 0:
            updated = False
            # firstly we try to use the data from one of the previous slices
            for step in range(1, Utils.TIME_SLICE_NUMBER_HOUR):
                if time_slice_key - step <= 0:
                    break
                #print([dist_key, time_slice_key, step])
                if len(self.traffic_data[dist_key][time_slice_key - step]) != 0:
                    self.traffic_data[dist_key][time_slice_key] = \
                      self.traffic_data[dist_key][time_slice_key - step]
                    updated = True
                    break
            # if no available data in pre slices, we try to use what inside next slices    
            for step in range(1, Utils.TIME_SLICE_NUMBER_HOUR):
                if time_slice_key + step > Utils.TIME_SLICE_NUMBER_DAY:
                    break
                if len(self.traffic_data[dist_key][time_slice_key + step]) != 0:
                    self.traffic_data[dist_key][time_slice_key] = \
                      self.traffic_data[dist_key][time_slice_key + step]
                    updated = True
                    break
            # when no available data in neighbouring slices, we have to set it use default value    
            if not updated:
                tmp_jam_dict = dict()
                for iter_i in range(1, TrafficData.jam_lvl + 1):
                    tmp_jam_dict[iter_i] = 0
                tmp_jam_dict['score'] = 0
                self.traffic_data[dist_key][time_slice_key] = tmp_jam_dict
        # if no socre key in the dict, the info dict has not been updated
        if not 'score' in self.traffic_data[dist_key][time_slice_key]:
            score = 0
            for (jam_key, jam_value) in self.traffic_data[dist_key][time_slice_key].items():
                score += jam_key * jam_value
            self.traffic_data[dist_key][time_slice_key]['score'] = score


    # take self.traffic_data and write it to a file (usually in csv fmt)
    def write_to_file(self, file_path, delimiter_string):
        with open(file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=delimiter_string)
            for dist_key in sorted(self.traffic_data.keys()):
                for time_slice_key in sorted(self.traffic_data[dist_key].keys()):
                    csv_row = [dist_key, time_slice_key]
                    for traffic_data_value in self.traffic_data[dist_key][time_slice_key].values():
                        csv_row.append(str(traffic_data_value))
                    csv_writer.writerow(csv_row)
            
                    
                    

















