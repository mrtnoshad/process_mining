
# Process Mining Code
# Morteza Noshad;  Stanford


import numpy as np
import pandas as pd
import math
import networkx as nx
import pygraphviz as pgv # pygraphviz should be available
import graphviz
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components





def generate_process_mining(dataframe, user_id_column ='user_id', time_column = 'time', event_label_columns = ['event_name', 'event_type'] , 
								types_to_include=None , filter_encoding_dict={} ,num_nodes = 15, edge_weight_lower_bound = 5):

	
	df_f = dataframe
	abbreviation_dict = filter_encoding_dict

	# node name preprocessing
	print('Creating	the column "node_name"...')

	if types_to_include:
		type_df_filter = pd.DataFrame({'event_type': types_to_include})
		df_f = df_f.merge(type_df_filter, how = 'inner', on='event_type')

	df_f = df_f.sort_values(by=[user_id_column, time_column]).reset_index(drop=True)


	num_users = df_f['user_id'].nunique()
	num_actions = len(df_f['user_id'])

	df_freq = df_f.groupby(['event_name','event_type']).size().reset_index(name='counts').sort_values(by='counts', ascending=False)

	# filter out the too prevalent actions (more than 2X of the cases)
	df_low_count = df_freq[df_freq.counts<(num_users*2)]
	df_f_f1 = df_f.merge(df_low_count, how = 'inner', on=['event_name','event_type'])

	# Apply abbreviations
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
	        df_f['node_name']=df_f['node_name'].replace(abbreviation_dict, regex=True)#replace(x,ABB[x])


	 # choose top k most frequent events
	k = num_nodes 

	df_enc_event = df_f[[user_id_column, time_column,'node_name'  ]]
	df_enc_event = df_enc_event.rename(columns={user_id_column:'enc_id', time_column: 'time_diff'})


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

	# filter the events to include top k events
	df_enc_event = df_enc_event.merge(df_sorted, how='inner', on='node_name')[['enc_id', 'node_name','time_diff','time_avg']].sort_values(['enc_id','time_diff'],ascending=True).reset_index(drop=True)
	df_enc_event = df_enc_event[df_enc_event.time_diff>=0].reset_index(drop=True)


	# Create table of unique encounter id

	unq_PC_enc_event = df_enc_event.groupby(['enc_id'])['node_name'].apply(list).reset_index().rename(columns={'enc_id':'enc_id', 0:'event_list'}).sort_values('enc_id',ascending=True)  
	unq_PC_enc_time = df_enc_event.groupby(['enc_id'])['time_diff'].apply(list).reset_index().rename(columns={'enc_id':'enc_id', 0:'time_diff'}).sort_values('enc_id',ascending=True)  

	unq_PC_enc = unq_PC_enc_event.merge(unq_PC_enc_time, how='left',on='enc_id')

	superlist = unq_PC_enc['node_name'].tolist()
	superlist_time = unq_PC_enc['time_diff'].tolist()


	# Get consecuative pairs

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
	        df_temp = pd.DataFrame({'node1':[x],'node2':[y],'time_weight':[td], 'node1_time':[df_sorted[df_sorted.node_name==x].time_avg.iloc[0]], 'node2_time':[df_sorted[df_sorted.node_name==y].time_avg.iloc[0]]})
	        df_g = pd.concat([df_g, df_temp])


	# Save the weighted edges

	E = {} # wighted frequency
	T = {}
	Node1_Time={}
	Node2_Time={}

	for i in range(len(superlist)):
	    t=0
	    for x, y in zip(superlist[i], superlist[i][1:]):
	        t+=1
	        td = superlist_time[i][t]-superlist_time[i][t-1]
	        t1 = df_sorted[df_sorted.node_name==x].time_avg.iloc[0]
	        t2 = df_sorted[df_sorted.node_name==y].time_avg.iloc[0]

	        if (x,y) not in E.keys():
	            E[(x,y)]=1
	            T[(x,y)]=td
	            Node1_Time[(x,y)]= t1
	            Node2_Time[(x,y)]= t2
	        else:
	            E[(x,y)]+=1
	        T[(x,y)]+=td


	# Filter edges and save to a list of edges with different weight types

	alpha = edge_weight_lower_bound # lower boud for edge frequency weight to show in the final graph

	A = []
	E_max = 0 # maximum frequency of the edges
	for e in E.keys():
	    
	    if E[e]>alpha and e[0]!=e[1]:

	        t1 = Node1_Time[e]#/E[e]
	        t2 = Node2_Time[e]#/E[e]
	        rtw= T[e]/E[e] # average relative time of all this type edges
	        tw = t2-t1 

	        e_type = (e[0],e[1],tw, E[e], t1,t2, rtw)

	        if  True: #t1<=t2 and tw<up_threshold and
	            A.append(e_type)

	            # find maximum freq of edges in order to normalize the freq
	            if E[e]>E_max:
	                E_max = E[e]

	# Find Adjacency matrix

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
	I_inv =  {value : key for (key, value) in I.items()}

	Adj = np.zeros((i,i))
	Adj_lag = np.zeros((i,i))
	for e in A:
	    Adj[I[e[0]],I[e[1]]]=e[3]
	    Adj_lag[I[e[0]],I[e[1]]]=e[6]

	I_org = I

	# Normalize the weights to be probability
	for i in range(Adj.shape[0]):
	    if np.sum(Adj[i])>0:
	        Adj[i] = Adj[i]/np.sum(Adj[i])

	if True: #not node_clustering:
	    dot = graphviz.Digraph()
	    for e in A:

	        prob=Adj[I[e[0]],I[e[1]]] # find the probability weight from the adjacency matrix
	        lag=Adj_lag[I[e[0]],I[e[1]]] # find the probability weight from the adjacency matrix
	        label='('+str(int(prob*100)/100)+', '+str(int(lag*100)/100)+')'
	        dot.edge(e[0],e[1], label=label , penwidth=str(3*prob))#, penwidth=1)
	        #dot.edge.attr['penwidth'] = 1

	    engin = sorted(graphviz.ENGINES)[0]
	    dot.render('process_graph_no_clustering.gv', view=False) 


### Apply graph clustering


	#############################
	# Create A_eta: threshold graph
	eta = 0.1 # threshold for finding connected graphs

	# find edges with less than threshold and the same type
	A_eta=[e for e in A if (e[2]<=eta and e[0].split("-")[0]==e[1].split("-")[0])]


	#############################
	# assign index for the vertices 

	I={} # index dict
	i = 0
	for e in A_eta:
	    if e[0] not in I.keys():
	        I[e[0]]=i
	        i+=1
	        
	    if e[1] not in I.keys():
	        I[e[1]]=i
	        i+=1

	#######################
	# create adjacency matrix
	I_inv =  {value : key for (key, value) in I.items()}
	Adj_sh = np.zeros((i,i))
	for e in A_eta:
	    Adj_sh[I[e[0]],I[e[1]]]=1
	    Adj_sh[I[e[1]],I[e[0]]]=1

	########################
	#### extract connected components
	graph = csr_matrix(Adj_sh)
	n_components, labels = connected_components(csgraph=graph, directed=False, return_labels=True)


	#Create Node names for the clustered components:
	W={}

	for i in range(np.max(labels)+1):
	    I_eq = np.arange(labels.shape[0])[labels==i]
	    W[i] = I_inv[I_eq[0]].split("-")[0] #+' : [' #+' : [' + [I_inv[i].split("-")[1] for i in ]
	    W[i]= W[i]+ ' ['
	    k=0
	    for j in I_eq:
	        if k >0:
	            W[i]= W[i]+ ' ,\n'
	        k+=1
	        W[i]= W[i]+ I_inv[j].split("-")[1]
	    W[i]= W[i]+ ' ]'
	    W[i] = W[i].replace("&", "&amp;")
	    W[i] = W[i].replace(":", "")


	##### Assign hashing from graph nodes to the component labels
	C={} # C {dict} contains the event names that can be grouped together (keys are event names)
	for i in range(labels.shape[0]):
	    C[I_inv[i]]= W[labels[i]] 
    

	###################
	###### Create a new graph (list of edges) with shrinked nodes 
	A_new = []

	for e in A:
	    
	    # consider the edges from the original graph, if one node is in the connected components, 
	    # then create a new edge with the new aggregated nodes
	    e0,e1 = e[0],e[1] # nodes of a edge
	    e0_org = e0
	    e1_org = e1
	    
	    if e0 in C:
	        e0 = C[e0] # new node
	    if e1 in C:
	        e1 = C[e1] # new node
	    
	    # add the edge if not a self-loop and not already existing
	    if e0 !=e1:
	        
	        # compute the weights
	        prob=Adj[I_org[e0_org],I_org[e1_org]] # find the probability weight from the adjacency matrix
	        lag=Adj_lag[I_org[e0_org],I_org[e1_org]] # find the probability weight from the adjacency matrix
	        
	        # check if the edge is already in the list
	        isinlist = False
	        for i in range(len(A_new)):
	            if A_new[i][0]==e0 and A_new[i][1]==e1:
	                isinlist = True
	                # add to the edge count
	                new_prob = np.min([A_new[i][2]+ prob, 1])
	                new_lag = ((A_new[i][3] * A_new[i][4])+prob)/ (1 + A_new[i][4])
	                new_count =1 + A_new[i][4]
	                A_new[i] = (e0,e1,new_prob,new_lag,new_count)
	                
	                
	        if isinlist == False:
	            count = 1
	            A_new.append((e0,e1,prob,lag,count))
	        
	#################
	####### draw the new graph
	if True:
	    dot = graphviz.Digraph()

	    for e in A_new:
	        label='('+str(int(e[2]*100)/100)+', '+str(int(e[3]*100)/100)+')'
	        dot.edge(e[0],e[1], label=label)

	    engin = sorted(graphviz.ENGINES)[0]
	    dot.render('process_graph_with_clustering.gv', view=True) 

	# Create
	print('Process Mining File Saved!')

# Conformity Scores

	list_of_the_scores = []

	for i in range(1,len(unq_PC_enc)):
	    total_cost = 0
	    total_edge_count = 0
	    
	    L = unq_PC_enc[unq_PC_enc.enc_id==i].node_name.to_numpy()[0]
	    # for on all of the nodes of the patient i
	    for j in range(len(L)-1):

	        e0 = L[j]
	        e1 = L[j+1]
	        # is there any aggregation?
	        if e0 in C:
	            e0 = C[e0] # new node
	        if e1 in C:
	            e1 = C[e1] # new node
	        
	        total_edge_count += 1
	        
	        for e in A_new: 
	            if e[0]==e0 and e[1]==e1:
	                total_cost += e[2]
	                
	    
	    total_edge_count = max(total_edge_count,1)

	    list_of_the_scores.append(total_cost/total_edge_count)

	list_of_the_scores = np.array(list_of_the_scores)

	
	# Some printing functions
	def return_node_list(i):
	    edge_list = []
	    L = unq_PC_enc[unq_PC_enc.enc_id==i].node_name.to_numpy()[0]
	    # for on all of the nodes of the patient i
	    node_list = []
	    for j in range(len(L)):

	        e0 = L[j]
	        # is there any aggregation?
	        if e0 in I_org:
	            if e0 in C:
	                e0 = C[e0] # new node
	            node_list.append(e0)

	    return node_list

	def return_edge_list(i):
	    edge_list=[]
	    node_list = return_node_list(i)
	    
	    for j in range(len(node_list)-1):

	        e0 = node_list[j]
	        e1 = node_list[j+1]
	        
	        edge_list.append((e0,e1))

	    return edge_list

	def print_major_events(i):
	    print(df_enc_event[df_enc_event.enc_id==i])
	    return 0

	def print_event_log(i):
	    print(dataframe[dataframe.user_id==i])
	    return 0


	# print the highest and lowest conformity scores
	i= np.argmax(list_of_the_scores)+1
	max_path = return_node_list(i)
	print('The user index with the highst conformity score: ', i)
	print('Conformity score: ', list_of_the_scores[i-1])
	print('Event log (major events) of the user with the highst conformity score:')
	print_major_events(i)


	####### draw the new graph
	G = nx.DiGraph()
	for e in A_new:
	    label='('+str(int(e[2]*100)/100)+', '+str(int(e[3]*100)/100)+')'
	    if (e[0],e[1]) in max_path:
	        
	        G.add_edge(e[0],e[1], label=label, penwidth="3", color="green") #, penwidth=str(3*prob))#, penwidth=1)
	    
	    else:
	        G.add_edge(e[0],e[1], label=label)

	new_G = nx.nx_agraph.to_agraph(G)
	display(new_G)
	new_G.draw('process_graph_with_max_path.png', prog='dot')


	# print the highest and lowest conformity scores
	i= np.argmin(list_of_the_scores)+1
	max_path = return_node_list(i)
	print('The user index with the lowest conformity score: ', i)
	print('Conformity score: ', list_of_the_scores[i-1])
	print('Event log (major events) of the user with the lowest conformity score:')
	print_major_events(i)


    


# ***************************************
if __name__=="__main__":


	print('run tests.py')
