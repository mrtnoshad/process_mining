
# Process Mining Code
# Morteza Noshad;  Stanford


import numpy as np
import pandas as pd
import math
import networkx as nx
import pygraphviz as pgv # pygraphviz should be available

def plot_process(dataframe, user_id_column ='user_id', time_column = 'time', event_label_columns = ['event_name', 'event_type'] , include_all_events = True, include_event_list = [], filter_encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, output_file_name='output'):

	df_f = dataframe
	abbreviation_dict = filter_encoding_dict

# node name preprocessing
	print('Creating	the column "node_name"...')
	
	if len(abbreviation_dict.keys())>0:
		Apply_ABB = True # Apply abbreviation
	else:
		Apply_ABB = False

	# Create The column node_name
	if isinstance(event_label_columns, str):
		df_f['node_name'] = df_f[event_label_columns]
	else:
		df_f['node_name'] = df_f[event_label_columns[0]]

		for i in range(1,len(event_label_columns)):
			df_f['node_name'] = df_f['node_name'] +' - ' +df_f[event_label_columns[i]]   

	# Node name edits and abbrevations
	if Apply_ABB==True:
		for x in abbreviation_dict.keys():
			df_f['node_name']=df_f['node_name'].replace(x,abbreviation_dict[x])

	# Choose top k nodes
	#print("Now we only need the following columns: 'enc_id', 'node_name', 'time_diff' ")
	#print("Choosing top k nodes")

	k = num_nodes 

	#df_enc_event = df_f['node_name']
	#print(df_enc_event.head())
	#df_enc_event['time_diff'] = df_f[time_column]
	#print(df_enc_event.head())
	#df_enc_event['enc_id'] = df_f[user_id_column]
	#print(df_enc_event.head())
	
	df_enc_event = df_f[[user_id_column, time_column,'node_name'  ]]
	#print(df_enc_event .columns)
	df_enc_event = df_enc_event.rename(columns={user_id_column:'enc_id', time_column: 'time_diff'})
	#print(df_enc_event.head())
	#print(df_enc_event.columns)
	#print(df_f[time_column])
	#print(df_f[user_id_column])
	#print(df_f.columns)


	# drop duplicate node_names (might be of different time_diff)
	df_enc_event = df_enc_event.drop_duplicates(subset=['enc_id','node_name'])


	# find the sorted event types
	def my_f(x):
	    d = []
	    d.append(len(x['node_name']))
	    d.append(x['time_diff'].median())
	    return pd.Series(d) #, index=[['count', 'time_avg']])

	df_sorted = df_enc_event.groupby('node_name').apply(my_f).reset_index().rename(columns={0:'count', 1:'time_avg'}).sort_values('count',ascending=False) #.sort_values('count',ascending=False)  
	df_sorted = df_sorted.head(k)

	# filter the events
	df_enc_event = df_enc_event.merge(df_sorted, how='inner', on='node_name')[['enc_id', 'node_name','time_diff','time_avg']].sort_values(['enc_id','time_diff'],ascending=True)


# Create table of unique encounter id
	print('Creating table of unique user id')

	unq_PC_enc_event = df_enc_event.groupby(['enc_id'])['node_name'].apply(list).reset_index().rename(columns={'enc_id':'enc_id', 0:'event_list'}).sort_values('enc_id',ascending=True)  
	unq_PC_enc_time = df_enc_event.groupby(['enc_id'])['time_diff'].apply(list).reset_index().rename(columns={'enc_id':'enc_id', 0:'time_diff'}).sort_values('enc_id',ascending=True)  

	unq_PC_enc = unq_PC_enc_event.merge(unq_PC_enc_time, how='left',on='enc_id')


	superlist = unq_PC_enc['node_name'].tolist()
	superlist_time = unq_PC_enc['time_diff'].tolist()
	i = 10
	#print(superlist[i])

	# unq_PC_enc.to_csv('all_workflows.csv')


# Get consecuative pairs
	print('Extracting consecuative event pairs')

	A = {}
	A['node1']=[]
	A['node2']=[]
	A['time_weight']=[]
	A['node1_time']=[]
	A['node2_time']=[]

	df_g = pd.DataFrame(A)

	for i in range(len(superlist)):
		t = 0
		for x, y in zip(superlist[i], superlist[i][1:]):
			t+=1
			td = superlist_time[i][t]-superlist_time[i][t-1]
			#print(x,y)
			df_temp = pd.DataFrame({'node1':[x],'node2':[y],'time_weight':[td], 'node1_time':[df_sorted[df_sorted.node_name==x].time_avg.iloc[0]], 'node2_time':[df_sorted[df_sorted.node_name==y].time_avg.iloc[0]]})
			df_g = pd.concat([df_g, df_temp])
	        

# Save the weighted edges
	print('Save the weighted edges')

	E = {} # wighted frequency
	T = {}
	Node1_Time={}
	Node2_Time={}

	for i in range(len(superlist)):
		t=0
		for x, y in zip(superlist[i], superlist[i][1:]):
			t+=1
			td = superlist_time[i][t]-superlist_time[i][t-1]
			#t2 = superlist_time[i][t]
			#t1 = superlist_time[i][t-1]
			t1 = df_sorted[df_sorted.node_name==x].time_avg.iloc[0]
			t2 = df_sorted[df_sorted.node_name==y].time_avg.iloc[0]
	        
	        
	        #print(x,y)
			if (x,y) not in E.keys():
				E[(x,y)]=1
				T[(x,y)]=td
				Node1_Time[(x,y)]= t1
				Node2_Time[(x,y)]= t2
			else:
				E[(x,y)]+=1
			T[(x,y)]+=td


# Filter edges and save to a list of edges with different weight types
	print('Filter edges and save to a list of edges with different weight types')

	alpha = edge_weight_lower_bound # lower boud for edge frequency weight to show in the final graph

	up_threshold = 17 # ignore edges with relative time more than up_threshold 

	A = []
	E_max = 0 # maximum frequency of the edges
	for e in E.keys():
		if E[e]>alpha and e[0]!=e[1]:
	        
			t1 = Node1_Time[e]#/E[e]
			t2 = Node2_Time[e]#/E[e]
			rtw= T[e]/E[e] # average relative time of all this type edges
			tw = t2-t1 

			e_type = (e[0],e[1],tw, E[e], t1,t2, rtw)

			if tw<up_threshold and t1<=t2:
				A.append(e_type)
	            
				# find maximum freq of edges in order to normalize the freq
				if E[e]>E_max:
					E_max = E[e]


# Find the median path
	print('Find the median path')

	# for all of the patients

	Score = np.zeros(len(unq_PC_enc))

	for i in range(len(unq_PC_enc)):
		L = unq_PC_enc.node_name.iloc[i]
		#print(L)

		for j in range(len(L)-1):

			Score[i] +=  - math.log(E[(L[j],L[j+1])]/E_max)
	    
		Score[i] = Score[i]#/len(L)

	med = np.argmin(Score)

	path_med_edges = [(unq_PC_enc.node_name.iloc[med][t],unq_PC_enc.node_name.iloc[med][t+1]) for t in range(len(unq_PC_enc.node_name.iloc[med])-1)]


# Find Adjacency matrix
	print('Find Adjacency matrix')

	# assign index for the vertices 

	I={} # index dict
	i = 0
	for e in A:
		if e[0] not in I.keys():
			I[e[0]]=i
			i+=1
	        
		if e[1] not in I.keys():
			I[e[1]]=i
			i+=1

	#######################
	# create adjacency matrix
	#print(i)
	I_inv =  {value : key for (key, value) in I.items()}

	#print(I_inv) # contains node names
	Adj = np.zeros((i,i))
	for e in A:
		Adj[I[e[0]],I[e[1]]]=e[3]
	    #Adj[I[e[1]],I[e[0]]]=1


# Plot the time_ordred map
	print('Plot the time_ordred map')



	path = 'MCP2' # Minimum time path 'MCP1' (Most common Path)  'MCP2'
	path_edges = []

	G = nx.DiGraph()

	for e in A:

		pw = "0.5"


		pl = str(int(100*e[3]/E_max)/100) #str(int(e[2]*100)/100)

		G.add_edge(e[0],e[1], label=pl, penwidth=pw)
	    

	if path == 'MTP':
		path_edges = path1_edges
	elif path == 'MCP1':
		path_edges = path2_edges
	elif path == 'MCP2':
		path_edges = path_med_edges

	G.add_node(path_edges[0][0],color="green", style='filled')
	for e in path_edges:
		G.add_edge(e[0],e[1], penwidth="3", color="green")
	    

	# copy to a new graph in order to assign levels
	new_G = nx.nx_agraph.to_agraph(G)

	Time_node = '-20'

	L2=[]
	for i in np.arange(-20,80,1):
	    
		L = df_sorted[df_sorted.time_avg.astype(int)==i].node_name.tolist()
		#L2 = df_sorted[df_sorted.time_avg.astype(int)==i+1].node_name.tolist()
		L = L+L2
		if len(L)==0:
			continue


	    # join a side graph
		if Time_node != '-20':
			new_G.add_edge(Time_node,str(i))
	        #print(Time_node)
		Time_node = str(i)
		L.append(Time_node)

		new_G.add_subgraph(L,rank='same')

	display(new_G)
	new_G.draw(output_file_name+'.png', prog='dot')
	print('Process Mining File Saved!')



# A function to read data from cloud and put into a dataframe

def read_data_from_cloud():
	# example: read from BigQuery chart to numpy

	client = bigquery.Client("som-nero-phi-jonc101"); # Project identifier
	conn = dbapi.connect(client);
	cursor = conn.cursor();
	query = """
	SELECT EV.*, CAT.string_field_1 AS event_category
	FROM `som-nero-phi-jonc101.noshad.aim2_event_list_all_v4` AS EV
	INNER JOIN `som-nero-phi-jonc101.noshad.aim2_event_category` AS CAT
	ON CAT.string_field_0 = EV.event_name
	""" # Example dataset table

	# to DataFrame

	df= (client.query(query).result().to_dataframe())

	print(df.head())
	print('Unique Event Types: ',df.event_type.unique())
	
	return df


# ***************************************
if __name__=="__main__":


	
	
	event_types = ['Order Medication', 'ADT', 'Order Procedure',#'Access log', # 'Lab Collection', #'Access log',
	'Lab Result',  'Radiology Proc Started', #'Procedure Start',
	'Medication Given', 'Radiology Report']

	node_names = ['event_type', 'event_category']

	abbreviation_dict = {'Order Procedure': 'OP', 'Lab Result':'LR', '2001002':'ER', 'ADT-':'', 'Started': 'Done'}

	data = read_data_from_cloud()

	plot_process(data=data, event_types=event_types, node_names=node_names, abbreviation_dict=abbreviation_dict ,num_nodes = 15, edge_weight_lower_bound = 5, plot_most_common_path = True, output_file_name='output')



