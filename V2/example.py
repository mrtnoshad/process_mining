# Process Mining Code
# Morteza Noshad;  Stanford
import numpy as np
import pandas as pd
from process_mining import plot_process
# read the event log into dataframe
dataframe = pd.read_csv('data_randomized.csv')
print('data loaded')

# plot the process mining graph
plot_process(dataframe, user_id_column ='user_id', time_column = 'time', 
	event_label_columns = ['event_name', 'event_type'] , include_all_events = True, include_event_list = [], 
	filter_encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, 
	output_file_name='output')



