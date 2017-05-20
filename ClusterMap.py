import csv
import Utils

def load_cluster_map(file_path):
	dict_cluster = dict();
	reader = csv.reader(open(file_path),delimiter=Utils.GLOBAL_DELIMITER)
	for cluster in reader:
		dict_cluster[cluster[0]] = int(cluster[1])
	return dict_cluster