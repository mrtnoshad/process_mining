# Clinical Process Mining

The process mining function is in the following format:

plot_process(data, event_types, node_names, abbreviation_dict ,num_nodes, edge_weight_lower_bound, plot_most_common_path, output_file_name)

Here is a description of the input arguments:

 data: the input data with the following columns:
 'jc_uid', 'enc_id', 'event_type', 'event_name', 'event_time',
      'emergencyAdmitTime', 'tpaAdminTime', 'time_diff', 'user_type',
       'event_category', 'node_name'


Event_type: the type of the event (Order medication, order procedure, etc)
node_names: What would be the node names. If it's a list, then the node name is concatenation of the names
abbreviation_dict: A dictionary of the abbreviations in the node names
num_nodes : number of nodes
edge_weight_lower_bound: if the weight of the edge is less than the bound it won't be present in the graph
plot_most_common_path: show the most common path in the graph 
output_file_name

## Requirements:

The following packages need to be installed on your machine:

* networkx
* pygraphviz
* Pandas
* Numpy
* Google Cloud SDK (only if you are reading the data from GCP)
