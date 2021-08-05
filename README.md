# Clinical Process Mining


### The function is in the following format:

generate_process_mining(dataframe, user_id_column = user_id_column , time_column = time_column, 
	event_label_columns = event_label_columns , types_to_include = types_to_include, 
	filter_encoding_dict = filter_encoding_dict , num_nodes = num_nodes, edge_weight_lower_bound = edge_weight_lower_bound)


 
### Description of the arguments:
 
#### dataframe: 
The event log data in the format of a dataframe. The columns of the data should be as follows:
 
* A column for the user id (specified in user_id_column)

* The time of the event (specified in time_column)

* One or several columns for the labels of the events (specified in event_label_columns)


#### include_all_events: 
A Boolean (True/False) to specify if the process mining graph should include all of the event types or only a subset of them. If this is False, then include_event_list should specify a list of the event names to include in the graph. 

#### types_to_include: 
The list of the event type names to include in the graph. If several columns are specified as labels in event_label_columns  then the filter only applies on the first column.

#### encoding_dict: 
A dictionary of the abbreviations/encoding/text_replacement in the node names event_label_column 

#### num_nodes: 
number of nodes 

#### edge_weight_lower_bound: 
if the weight of the edge is less than the bound it won't be present in the graph 







## Requirements:

The following requirements need to be installed on your machine:


- Python==3.6
- networkx==2.5
- pygraphviz==1.3
- Pandas=1.1.3
