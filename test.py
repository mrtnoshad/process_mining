# Process Mining Code
# Morteza Noshad;  Stanford
import numpy as np
import pandas as pd
from process_mining import generate_process_mining
# read the event log into dataframe
dataframe = pd.read_csv('data_randomized.csv')
print('data loaded')


user_id_column ='user_id'
time_column = 'time'
event_label_columns = ['event_type', 'event_name'] # Event categories come first
types_to_include = ['ADT', 'Order Procedure', 'Order Medication', 'Lab Result',
 'Radiology Report', 'Medication Given']
include_event_list = [] 
filter_encoding_dict={'Order Procedure': 'OP', 'Lab Result':'LR', '2001002':'ER', 'ADT-':'', 'Started': 'Done', 'ALTEPLASE 100': 'ALTEPLASE (tPA) 100', 'OXYGEN:': 'OXYGEN'}
num_nodes = 12
edge_weight_lower_bound = 10


# plot the process mining graph
generate_process_mining(dataframe, user_id_column = user_id_column , time_column = time_column, 
	event_label_columns = event_label_columns , types_to_include = types_to_include, 
	filter_encoding_dict = filter_encoding_dict , num_nodes = num_nodes, edge_weight_lower_bound = edge_weight_lower_bound)




