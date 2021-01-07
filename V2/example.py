
# Process Mining Code
# Morteza Noshad;  Stanford
import numpy as np
import pandas as pd

# read the event log into dataframe
dataframe = read_csv(event_log.csv)

# plot the process mining graph
plot_process(dataframe, user_id_column ='enc_id', time_column = 'time_diff', event_label_columns = ['event_type', 'event_category'] , include_all_events = True, include_event_list = [], filter encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, output_file_name='output')



