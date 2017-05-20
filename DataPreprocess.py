#!/usr/bin/python

import os
import re
import Utils
import ClusterMap
import OrderData
import TrafficData
import WxData
import PoiData
    
# Function: preprocesss data from input_home_dir and export result to output_home_dir
 # input_home_dir: must have sub-folders like cluster_map,order_data, poi_data and etc.
 # output_home_dir: sub-folders like demand_supply, order_data and etc. will be created
 # export_preprocessed_order: flag of whether to export preprocessed order
def preprocess_order_data(input_home_dir,output_home_dir,export_preprocessed_order):
    #1.load cluster map
    cluster_file_path =os.path.join(input_home_dir,Utils.CLUSTER_FOLDER_NAME,Utils.CLUSTER_FILE_NAME)
    dict_cluster_map = ClusterMap.load_cluster_map(cluster_file_path)
    #2.preprocess order data
    order_dir = os.path.join(input_home_dir,Utils.ORDER_FOLDER_NAME)
    preprocess_ds_dir = os.path.join(output_home_dir,Utils.DS_FOLDER_NAME)
    preprocess_order_dir = os.path.join(output_home_dir,Utils.ORDER_FOLDER_NAME)
    # created folder to store the pre-processed order data and demand_supply
    if not os.path.isdir(preprocess_ds_dir):
        os.makedirs(preprocess_ds_dir)
    if not os.path.isdir(preprocess_order_dir):
        os.makedirs(preprocess_order_dir)
    # 2.1 read order data
    for tmp_dir_paths,tmp_dir_names,file_names in os.walk(order_dir):
        for order_file_name in file_names:
            # simply regex check
            date_search_obj = re.search(Utils.DATE_REGEX,order_file_name)        
            if date_search_obj == None:
                continue;
            date = date_search_obj.group(0)
            order_file_path = os.path.join(order_dir,order_file_name)
            order_data = OrderData.OrderData(date,dict_cluster_map)
            order_data.load_orders_by_day(order_file_path)
            Utils.log_debug("order preprocessed -  " + date) 
            if export_preprocessed_order:
                order_data.export_orders_by_day(os.path.join(preprocess_order_dir,order_file_name))
                Utils.log_debug("order exported - " + date)
            # 2.2 calculate demand-supply-gap
            order_data.calc_demand_supply_by_day()
            Utils.log_debug("demand_supply preprocessed -  " + date)
            ds_file_name = Utils.DS_FILE_PREFIX + date
            order_data.export_ds_by_day(os.path.join(preprocess_ds_dir,ds_file_name),False)
            Utils.log_debug("demand_supply exported -  " + date)

def preprocess_traffic_data(input_folder):
    cluster_file_path = os.path.join(input_folder, Utils.CLUSTER_FOLDER_NAME, Utils.CLUSTER_FILE_NAME)
    cluster_map = ClusterMap.load_cluster_map(cluster_file_path)
    # traffic data folders
    traffic_data_dir = os.path.join(input_folder, Utils.TRAFFIC_FOLDER_NAME)
#    preprocessed_traffic_dir = os.path.join(output_folder, PROC_TRAFFIC_FOLDER_NAME)
#    if not os.path.isdir(preprocessed_traffic_dir):
#        os.makedirs(preprocessed_traffic_dir)
    # make a dict of traffic_data obj with traffic date as the key
    traffic_data_dict = dict()
    for _, _, file_names in os.walk(traffic_data_dir):
        for traffic_data_file in file_names:
            # regex to check date
            date_search_obj = re.search(Utils.DATE_REGEX, traffic_data_file)
            if date_search_obj == None:
                continue
            traffic_date = date_search_obj.group(0)
            trf_file_path = os.path.join(traffic_data_dir, traffic_data_file)
            traffic_data = TrafficData.TrafficData(cluster_map)
            traffic_data.load_traffic_data_by_day(trf_file_path, traffic_date)
            traffic_data.proc_traffic_data_by_day()           
            traffic_data_dict[traffic_date] = traffic_data
            Utils.log_debug("traffic data preprocessed -  " + traffic_date)

    return traffic_data_dict

def preprocess_wx_data(input_folder):
    wx_data_dir = os.path.join(input_folder, Utils.WEATHER_FOLDER_NAME)

    wx_data_dict = dict()
    for _, _, file_names in os.walk(wx_data_dir):
        for wx_data_file in file_names:
            # regex to check date
            date_search_obj = re.search(Utils.DATE_REGEX, wx_data_file)
            if date_search_obj == None:
                continue
            wx_date = date_search_obj.group(0)
            wx_file_path = os.path.join(wx_data_dir, wx_data_file)
            wx_data = WxData.WxData()
            wx_data.load_wx_data_by_day(wx_file_path, wx_date)
            wx_data.proc_wx_data_by_day()
            # add the wx_data obj to the dict
            wx_data_dict[wx_date] = wx_data
            Utils.log_debug("weather data preprocessed -  " + wx_date)
    
    return wx_data_dict

def preprocess_poi_data(input_folder):
    cluster_file_path = os.path.join(input_folder, Utils.CLUSTER_FOLDER_NAME, Utils.CLUSTER_FILE_NAME)
    cluster_map = ClusterMap.load_cluster_map(cluster_file_path)
    
    poi_data_dir = os.path.join(input_folder, Utils.POI_FOLDER_NAME)
    poi_file_path = os.path.join(poi_data_dir, Utils.POI_FILE_NAME)
    poi_data = PoiData.PoiData(cluster_map)
    poi_data.load_poi_data_by_day(poi_file_path)
    Utils.log_debug("poi data loaded")
    
    return poi_data
    
def write_trf_data_to_files(trf_data_dict, output_folder):
    preprocessed_trf_dir = os.path.join(output_folder, Utils.TRF_FOLDER_NAME)
    if not os.path.isdir(preprocessed_trf_dir):
        os.makedirs(preprocessed_trf_dir)
    for trf_data_obj_key in trf_data_dict.keys():
        file_name = Utils.TRF_FILE_PREFIX + trf_data_obj_key
        file_path = os.path.join(preprocessed_trf_dir, file_name)
        trf_data_dict[trf_data_obj_key].write_to_file(file_path, Utils.GLOBAL_DELIMITER)
        Utils.log_debug("traffic data exported -  " + trf_data_obj_key)

        
def write_wx_data_to_files(wx_data_dict, output_folder):
    preprocessed_wx_dir = os.path.join(output_folder, Utils.WX_FOLDER_NAME)
    if not os.path.isdir(preprocessed_wx_dir):
        os.makedirs(preprocessed_wx_dir)
    for wx_data_obj_key in wx_data_dict.keys():
        file_name = Utils.WX_FILE_PREFIX + wx_data_obj_key
        file_path = os.path.join(preprocessed_wx_dir, file_name)
        wx_data_dict[wx_data_obj_key].write_to_file(file_path, Utils.GLOBAL_DELIMITER)
        Utils.log_debug("weather data exported -  " + wx_data_obj_key)

#1) preprocess Training Data
Utils.log_info("START PREPROCESSING TRAIN DATA ...")
 
trn_data_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.TRN_FOLDER_NAME)
preprocessed_trn_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.PREPROCESS_TRN_FOLDER_NAME)
# order
preprocess_order_data(trn_data_home_dir,preprocessed_trn_home_dir,False)
# traffic
trn_traffic_data = preprocess_traffic_data(trn_data_home_dir)
write_trf_data_to_files(trn_traffic_data, preprocessed_trn_home_dir)
# weather
trn_wx_data = preprocess_wx_data(trn_data_home_dir)
write_wx_data_to_files(trn_wx_data, preprocessed_trn_home_dir)
# place of interest
poi_info = preprocess_poi_data(trn_data_home_dir)
# TODO by joe 2016/06/15: map poi to a 1-dimensional value and export to file
#2) preprocess Test set
Utils.log_info("START PREPROCESSING TEST SET ...")
test_set_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.TEST_FOLDER_NAME)
preprocessed_test_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.PREPROCESS_TEST_FOLDER_NAME)
# order
preprocess_order_data(test_set_home_dir,preprocessed_test_home_dir,True)
# traffic
test_traffic_data = preprocess_traffic_data(test_set_home_dir)
write_trf_data_to_files(test_traffic_data, preprocessed_test_home_dir)
# weather
test_wx_data = preprocess_wx_data(test_set_home_dir)
write_wx_data_to_files(test_wx_data, preprocessed_test_home_dir)
# place of interest
test_poi_info = preprocess_poi_data(test_set_home_dir)

