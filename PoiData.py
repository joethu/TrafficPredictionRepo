#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  5 14:04:12 2016

@author: zack
"""
# this module deals with the poi info

import Utils
import csv


class PoiData:
    def __init__(self, cluster_map):
        self.cls_map = cluster_map
        self.poi_data = self.gen_data_dict_by_dist(cluster_map)
        
    def gen_data_dict_by_dist(self, cluster_map):
        empty_dict = dict()
        for dist_id in sorted(cluster_map.values()):
            empty_dict[dist_id] = dict()
        return empty_dict
        
        
    def load_poi_data(self, file_path):
        with open(file_path, newline='') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=Utils.GLOBAL_DELIMITER)
            for data_row in csv_reader:
                dist_id = Utils.dist_hash_to_id(data_row[0], self.cls_map)
                if Utils.DEFAULT_DISTRICT_ID == dist_id:
                    continue
                dist_info = dict()
                for iter_i in range(1, len(data_row) - 1):
                    # first we drag the facility num out of the item
                    info_item = data_row[iter_i].split(Utils.POI_NUM_DELIMITER)
                    lvl_info_item = info_item[0].split(Utils.POI_LVL_DELIMITER)
                    has_lvl_two = False
                    if len(lvl_info_item) > 1:
                        lvl_one = lvl_info_item[0]
                        lvl_two = lvl_info_item[1]
                        info_num = info_item[1]
                        has_lvl_two = True
                    else:
                        lvl_one = info_item[0]
                        info_num = info_item[1]
                    lvl_info = dict()
                    if has_lvl_two:
                        lvl_info[int(lvl_two)] = int(info_num)
                    else:
                        lvl_info['other'] = int(info_num)
                    if lvl_one in dist_info.keys():
                        dist_info[lvl_one].update(lvl_info)
                    else:
                        dist_info[lvl_one] = lvl_info
                self.calc_score(dist_info)
                self.poi_data[dist_id] = dist_info
    
    # calculate the score per the POI info of the district 
    # and update the in-argument which is a dict           
    def calc_score(self, dist_info_dict):
        total_quant = 0
        for key in dist_info_dict.keys():
            if isinstance(dist_info_dict[key], dict):
                sub_quant = 0
                for quant in dist_info_dict[key].values():
                    sub_quant += quant
                dist_info_dict[key]['subscore'] = sub_quant
                total_quant += sub_quant
            # the item is a key-value with the key as "other"
            else:
                total_quant += dist_info_dict[key]
        dist_info_dict['score'] = total_quant
                        