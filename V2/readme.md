
### The function is in the following format:

plot_process(dataframe, user_id_column ='enc_id', time_column = 'time_diff', event_label_columns = ['event_type', 'event_category'] , include_all_events = True, include_event_list = [], filter encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, output_file_name='output')
 
### Description of the arguments:
 
\textbf{dataframe:} the event log data in the format of a dataframe. The columns of the data should be as follows:
 
* A column for the user id (specified in user_id_column)

* The time of the event (specified in time_column)

* One or several columns for the labels of the events (specified in event_label_columns)


\textbf{include_all_events:}



