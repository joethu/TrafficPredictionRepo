import os
import re
import csv
import Utils
import ClusterMap
import OrderData

GAP_EXPORT_FORMAT = "%.1f"

#Function: load demand-supply data of all days in folder of "ds_dir"
def load_ds_from_folder(ds_dir,cluster_map):
    ds_all_dates = dict()    
    for tmp_dir_paths,tmp_dir_names,file_names in os.walk(ds_dir):
        for ds_file_name in file_names:
            # simply regex check
            date_search_obj = re.search(Utils.DATE_REGEX,ds_file_name)        
            if date_search_obj == None:
                continue;
            ds_date = date_search_obj.group(0)
            ds_file_path = os.path.join(ds_dir,ds_file_name)
            order_data = OrderData.OrderData(ds_date,cluster_map)
            order_data.load_demand_supply_by_day(ds_file_path)
            ds_all_dates[ds_date] = order_data.ds_data
    return ds_all_dates

#EXPORT PREDICTION RESULT
#Function: transform data structure storing predictions:
 #list_predictions:list of [date,time_slice_id,dict(district_id->gap)]
 #return: dict(district_id->dict(date->dict(time_slice_id->gap)))
def save_predictons_to_dict(list_predictions):
    res = dict()
    for [date,time_slice_id,dict_gaps] in list_predictions:
        for district_id in sorted(dict_gaps.keys()):
            if not district_id in res.keys():
                res[district_id] = dict()
            if not date in res[district_id].keys():
                res[district_id][date] = dict()
            res[district_id][date][time_slice_id] = dict_gaps[district_id]
    return res
            
#Function: export prediction result to csv file whose line 
#  is like "district_id,str_date_time_slice_id,ds_gap"
 #dict_predictions: dict(district_id->dict(date->dict(time_slice_id->gap)))
def export_predictions_to_csv(file_path,dict_predictions):
    writer = csv.writer(open(file_path,"w"))
    for district_id in sorted(dict_predictions.keys()):
        dict_date = dict_predictions[district_id]
        for date in sorted(dict_date.keys()):
            dict_time_slice = dict_date[date]
            for time_slice_id in sorted(dict_time_slice.keys()):
                ds_gap = dict_time_slice[time_slice_id]
                str_date_time = Utils.connect_date_time_slice(date,time_slice_id)
                str_ds_gap = GAP_EXPORT_FORMAT % ds_gap
                writer.writerow([str(district_id),str_date_time,str_ds_gap])

#ALGORITHMS
# ALGO-1: Jjust calc the average gap of the last several time slices
# ds_dates: dict(date->dict(district_id -> dict(time_slice_id->[d s g])))
# window_length: the number of time slices that are ahead of the time slice
#   in question and are used to calc the average ds gap
# return: dict(district_id->the gap of the time slice in question)
def algo_avg_n(ds_dates,districts,date,time_slice_id,window_length):
    dict_gaps = dict()
    if date in ds_dates.keys():
        for district_id in districts:
            ds = ds_dates[date][district_id]
            sum_gaps = 0.0
            num_gaps = 0
            for i in range(time_slice_id-window_length,time_slice_id):
                if Utils.is_valid_time_slice_id(i):
                    num_gaps += 1
                    sum_gaps += float(ds[i][OrderData.GAP_KEY])
            avg_ds = 0.0
            if num_gaps > 0:
                avg_ds = sum_gaps/float(num_gaps)
            dict_gaps[district_id] = avg_ds
    return dict_gaps

#Function: execute average-n algorithm on all time slices in the range set
 #time_range: list of [date,time_slice_id]
 #return: list of [date,time_slice_id,dict(district_id->gap)]
def exex_algo_avg_n(ds_dates,districts,time_range,window_length):
    res = []
    for [date,time_slice_id] in time_range:
       res.append([date,time_slice_id,algo_avg_n(ds_dates,districts,date,time_slice_id,window_length)])
    return res

test_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.TEST_FOLDER_NAME)            
#0.load test range file
test_range_file_path = os.path.join(test_home_dir,Utils.TEST_RANGE_FILE_NAME)
list_test_date_time_range = [] # list of [date,time_slice_id]
test_range_file = open(test_range_file_path,"r")
for test_range_line in test_range_file.readlines():
    test_range_line = test_range_line.rstrip(Utils.RETURN_CHAR)
    # simply regex check
    date_search_obj = re.search(Utils.DATE_REGEX,test_range_line)        
    if date_search_obj == None:
        continue;
    list_test_date_time_range.append(Utils.split_date_time_slice(test_range_line))
test_range_file.close()
Utils.log_info("test date time range loaded.")

#1.load cluster map
cluster_file_path =os.path.join(test_home_dir,Utils.CLUSTER_FOLDER_NAME,Utils.CLUSTER_FILE_NAME)
dict_cluster_map = ClusterMap.load_cluster_map(cluster_file_path)
#2.load demand-supply data from test set
preprocessed_test_home_dir = os.path.join(Utils.DATA_HOME_DIR,Utils.PREPROCESS_TEST_FOLDER_NAME)
test_ds_dir = os.path.join(preprocessed_test_home_dir,Utils.DS_FOLDER_NAME)
test_ds_all_dates = load_ds_from_folder(test_ds_dir,dict_cluster_map)
Utils.log_info("test demand-supply loaded.")
#3.START PREDICTION
# algo-avg-3
NUM_TIME_SLICES_AHEAD_IN_TEST_SET = 3
list_predictions = exex_algo_avg_n(test_ds_all_dates,sorted(list(dict_cluster_map.values())),list_test_date_time_range,NUM_TIME_SLICES_AHEAD_IN_TEST_SET)
Utils.log_info("prediction completed.")
dict_predictions = save_predictons_to_dict(list_predictions)
predictions_file_path = os.path.join(test_home_dir,Utils.PREDICTION_RES_FILE_NAME)
export_predictions_to_csv(predictions_file_path,dict_predictions)
Utils.log_info("prediction exported.")