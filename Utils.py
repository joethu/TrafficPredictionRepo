import math

SET_INDEX = "2"

#DATA_HOME_DIR = "../season_" + SET_INDEX
#DATA_HOME_DIR = "/Users/zack/Developer/ML/Didi/season_" + SET_INDEX
DATA_HOME_DIR = "/Users/qiaohuang/Documents/Tech/DidiTrafficPrediction/season_" + SET_INDEX
#DATA_HOME_DIR = "D:\huangqiao\Personal\Didi\citydata\season_" + SET_INDEX
GLOBAL_DELIMITER = "\t"
RETURN_CHAR = "\n"
SECONDARY_DELIMITER = " "
JAM_DELIMITER = ":"
POI_NUM_DELIMITER = ":"
POI_LVL_DELIMITER = "#"
CSV_COMMA_DELIMITER = ","
DATE_TIME_CONNECTOR = "-"
TIME_SLICE_INTERVAL = 10.0 # in minutes
TIME_SLICE_NUMBER_HOUR = 6 
TIME_SLICE_NUMBER_DAY = 144
DEFAULT_DISTRICT_ID = -1 # when dest district hash not in cluster map, set its id to this value
DEFAULT_TIME_SLICE_ID = -1 # if the time slice id exceeds [1, 144]
DEBUG_MODE = True

TRN_FOLDER_NAME = "training_data"
TEST_FOLDER_NAME = "test_set_" + SET_INDEX
PREPROCESS_TRN_FOLDER_NAME = "preprocessed_training_data"
PREPROCESS_TEST_FOLDER_NAME = "preprocessed_test_set_" + SET_INDEX
ORDER_FOLDER_NAME = "order_data"
DS_FOLDER_NAME = "demand_supply"
TRF_FOLDER_NAME = "trf_data"
WX_FOLDER_NAME = "wx_data"
TRAFFIC_FOLDER_NAME = "traffic_data"
WEATHER_FOLDER_NAME = "weather_data"
POI_FOLDER_NAME = "poi_data"
POI_FILE_NAME = "poi_data"
CLUSTER_FOLDER_NAME = "cluster_map"
CLUSTER_FILE_NAME = "cluster_map"
TEST_RANGE_FILE_NAME = "read_me_" + SET_INDEX + ".txt"
PREDICTION_RES_FILE_NAME = "predictions.csv"
DATE_REGEX = "2016-[0-1][0-9]-[0-3][0-9]" # inaccurate but can do
ORDER_DATA_PREFIX = "order_data_"
DS_FILE_PREFIX = "ds_"
TRF_FILE_PREFIX = "trf_"
WX_FILE_PREFIX = "wx_"

def log_debug(debug_info):
    if DEBUG_MODE:
        print("Debug: " + debug_info)

def log_info(info):
    print("Info: " + info)

# CALCULATE THE TIME SLICE ID DEFINED BY EVERY A TIME_SLICE_INTERVAL
# return an integer
def calc_time_slice_id(time_stamp):
	hh_mm_ss = time_stamp.split(":")
	return int(hh_mm_ss[0])*TIME_SLICE_NUMBER_HOUR + math.floor(float(hh_mm_ss[1])/TIME_SLICE_INTERVAL) + 1

def is_valid_time_slice_id(time_slice_id):
	return  time_slice_id >= 1 and time_slice_id <= TIME_SLICE_NUMBER_DAY

def get_time_slice_id(time_stamp):
    time_slice_id = calc_time_slice_id(time_stamp)
    if not is_valid_time_slice_id(time_slice_id):
        time_slice_id = DEFAULT_TIME_SLICE_ID
    return time_slice_id

def dist_hash_to_id(dist_hash, cluster_map):
    #if hash not in map, return default error value -1
    return cluster_map.get(dist_hash, DEFAULT_DISTRICT_ID)

#Helper: convert date and time_slice_id to string by connecting them with a "-"
 #return: string like "2016-01-01-144"
def connect_date_time_slice(date,time_slice_id):
    return DATE_TIME_CONNECTOR.join([date,str(time_slice_id)])
#Helper: get date and time_slice_id from str_date_time_slice like "2016-01-01-144"
 #return: [date,time_slice_id]
def split_date_time_slice(str_date_time_slice):
    date_time = str_date_time_slice.split(DATE_TIME_CONNECTOR)
    date = DATE_TIME_CONNECTOR.join(date_time[0:3])
    time_slice_id = int(date_time[3])
    return [date,time_slice_id]