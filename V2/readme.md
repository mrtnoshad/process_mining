
### The function is in the following format:

plot_process(dataframe, user_id_column ='enc_id', time_column = 'time_diff', event_label_columns = ['event_type', 'event_category'] , include_all_events = True, include_event_list = [], filter encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, output_file_name='output')
 
### Description of the arguments:
 
#### dataframe: 
The event log data in the format of a dataframe. The columns of the data should be as follows:
 
* A column for the user id (specified in user_id_column)

* The time of the event (specified in time_column)

* One or several columns for the labels of the events (specified in event_label_columns)


#### include_all_events: 
A Boolean (True/False) to specify if the process mining graph should include all of the event types or only a subset of them. If this is False, then include_event_list should specify a list of the event names to include in the graph. 

#### include_event_list: 
The list of the event names to include in the graph. If several columns are specified as labels in event_label_columns  then the filter only applies on the first column.

#### encoding_dict: 
A dictionary of the abbreviations/encoding/text_replacement in the node names event_label_column 

#### num_nodes: 
number of nodes 

#### edge_weight_lower_bound: 
if the weight of the edge is less than the bound it won't be present in the graph 

#### plot_most_common_path: 
show the most common path in the graph output_file_name

#### output_file_name: 
the name of the output file



