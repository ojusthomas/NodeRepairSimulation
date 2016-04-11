

# ver 1.1 Update Scaling support added as function Scale
# version 2.0 Combined tables  node_fileblk_map to tbl_node_allocation
# version 2.1 adding insert , delete files and make  node fail, live
# Version 2.2 single node scaling required in case of a single node failure. Furthur writes are permitted only when buffer nodes are present through single node scaling.  
# Recovery by Adjustment works only when no buffer nodes are there and when no reads and writes are there.
# from branch3 
import math
import time
#from guppy import hpy
import sys
import os
from multiprocessing import Pool

# define the storage node.
class StorageNode:
	
	#Constructor takes maximum number of blocks in  the node, size of the block, nodeid, type of node - DN(DataNode), PN(ParityNode)
	#@profile
	def __init__(self, max_blocks, block_size, node_id, node_type):
				self.max_blocks = max_blocks
				self.block_size = block_size
				self.block_data = []
				self.block_stat = []
				for blocks in range(max_blocks+1):
					self.block_data.append(-1)
					self.block_stat.append("Free")
				self.block_count = 0
				self.node_stat = "Live"
				self.node_type = node_type  
				self.node_id = node_id
				self.node_availability_status = "Free" # Other possible status = "Used"
			
	# write to the stripes in the node for the first time
	#@profile
	def write_to_node_stripe(self, data, block_no):
		print "inside write_to_node_stripe --- > "+ self.node_stat
		if(self.node_stat == "Live"):
			if(self.node_availability_status=="Free"):
				self.node_availability_status = "Used"
			if(self.block_stat[block_no] == "Free"):
				print "@@@ self.block_count = " + str(self.block_count) 
				self.block_data[block_no] = data
				self.block_stat[block_no] = "Allocated"
				self.block_count = self.block_count + 1
				return "Done"
			else:
				print "Block already in use"
				return "NotDone"
		else:
			print "\nCannot write Node Failed\n"
			return "Failed"
	
	# update the stripes of the node
	def update_node_stripe(self,data,block_no):
		#print "\n My Id : "+str(self.node_id)
		if(self.node_stat == "Live"):
			self.block_data[block_no] = data
			self.block_stat[block_no] = "Allocated"
			#self.block_count += self.block_count
			return "Done"
		else:
			print "\nCannot write Node Failed\n"
			return "Not Done"
			
			
	#Read from a block in the node 	
	def read_from_node(self,block_no):
		if(self.node_stat == "Live"):
			return block_data[self.block_no]
		else:
			print "\nCannot write Node Failed\n"
	
	# return the node status		
	def give_node_status(self):
		return self.node_stat
	
	# make the node to fail
	def make_node_fail(self):
		print "\n ^^^^&&&^--__ Node "+ str(self.node_id)+ " "+str(self.node_stat)
		if(self.node_stat == "Live"):
			self.node_stat = "Failed"
			print "\n ^^^^&&&^^^^ Node "+ str(self.node_id)+ " "+str(self.node_stat)
			return "Failed"
		else:
			print "\n&&& &&& Node already failed\n"
			return "Already Failed"
	
	#	make the node live	
	def make_node_live(self):
		if(self.node_stat == "Live"):
			print "\nNode Already Live\n"
		else:
			self.node_stat = "Live"
			print "\n node live now"
			for blocks in range(self.max_blocks):
				self.block_stat[blocks] == "Free"
				self.block_data[blocks] == -1
		return self.node_stat

	#	return the maximum number of blocks in the node	
	def give_max_blocks(self):
		return self.max_blocks
	
	#	return the size of a block
	def give_block_size(self):
		return self.block_size
	
	# return the status of the block - free or not
	def give_block_stat(self,block_no):
		return block_stat[block_no]
	
	# return the count of used blocks
	def give_block_count(self):
		return self.block_count
	
	# return the details all blocks in the node
	def list_all_blocks(self):
		if(self.node_stat == "Live"):
			for blocks in range(self.max_blocks):
				if(self.block_stat[blocks] == "Free"):
					print " \n Block [ "+str(blocks)+" ] is Free"
				else:
					print " \n Block [ "+str(blocks)+" ] = "+ str( self.block_data[blocks])
		else:
			print "Node Failed"

	# return the next free block
	#@profile
	def give_next_free_block(self):
		#print "give_next_free_block : " 
		free_block = []
		if(self.node_stat == "Live"):
			#print "Max :"+str(self.max_blocks)
			for blocks in range(self.max_blocks):
					#print "Blok "+str(blocks)+" Stat : "+str(self.block_stat)
					if(self.block_stat[blocks] == "Free"):
						#print "Free Block"+str(blocks)
						free_block.append(self.node_id)
						free_block.append(blocks)
						return free_block

	#return the list of all free blocks
	#@profile
	def give_list_free_blocks(self):
		list_free_blocks = []
		#print "\n Node Status "+str(self.node_id)+": "+str(self.node_stat)
		if(self.node_stat == "Live"):
			for blocks in range(self.max_blocks):
					#print "Blok "+str(blocks)+" Stat : "+str(self.block_stat)
					if(self.block_stat[blocks] == "Free"):
						list_free_blocks.append(blocks)
		#print "\n list_free_blocks : "+str(list_free_blocks)
		return list_free_blocks

# defining the master
class Master:
	tbl_file_blocks_map = {} #Tbl1 file to file blocks map eg : A, [A1, A2, PA1, PA2]
	tbl_fileblk_nodeblk_map = {} # Tbl2 File block to node block map eg : {A1, [DN0,1],A2, [DN1,1], PA0,[PN0,1], PA1,[PN1,1]}
	tbl_file_stripe_map = {} # Tbl3 File, stripe map eg : A ,[[DN1,1],[DN2,1], [PN3,1], [PN4,1]]
	tbl_node_allocation = {} # Tbl4 Node allocation map eg : {DN1, [[Fail],[]], DN2,[50,20,'Live',[A1,B1,C1]
	tbl_file_blocks_list = [] # hold the file block list eg. [A1,A2,PA1,PA2]
	tbl_file_stripe_list = [] # hold the file stripe list eg. [[DN1,1],[DN2,1], [PN3,1], [PN4,1]]
	tbl_allocation_list = [] # [50,20]
	storage_node = [] # list of storage nodes
	file_list = [] # list of files stored
	replacement_list = {} #list of nodes with replacement node for each in case of failure. eg. ['DN1','BN1'] BN1-BufferNode1 is the replacement for DN1
	recovery_rule = "Adjust" # "Replace" - replace with a new node, "Adjust" - adjust with existing nodes
	list_free_nodes = {} # list of free nodes i.e having the node_availability_status = "Free"
	#nodeid_nodenumber_map = {} # Keep track of node
	node_fileblk_map = {} # keep track of file blocks in each node
	scale_flag = False
	force_scale_node = ""
	clusters = {}

	#constructor
	#@profile
	def __init__(self, max_nodes, datanodes, paritynodes, buffernodes): # max_nodes = datanodes+paritynodes, buffernodes are for failure handling.
		self.max_nodes  = max_nodes
		self.datanodes = datanodes
		self.paritynodes = paritynodes
		self.buffernodes = buffernodes
		self.totalnodes = datanodes+paritynodes+buffernodes
		self.buffernodes_used = 0
		self.scaled = 1
		lists = []
		for nodes in range(self.datanodes):
			#print "**@@ Nodes :"+str(nodes)+" datanodes = "+str(datanodes)+" datanodes+paritynodes = "+str(datanodes+paritynodes)
			#if (nodes < datanodes):
			self.storage_node.append(StorageNode(5, 100, "DN-"+str(nodes),"DataNode"))# creating data nodes
			node_id = self.storage_node[nodes].node_id
			#print "**** Node number of : "+str(node_id) +" "+str(self.give_node_number(node_id))
			#print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [] # intializing allocation table Tbl4
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
			self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]#lists #populating allocation table Tbl4
			#self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live",[]]
			# @above lists populating allocation table Tbl4,added empty list to combine arrays
			self.list_free_nodes[node_id] = [] # intializing free node list e.g [DN0,'Free']
			self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status) # populating free node lsit
			#self.nodeid_nodenumber_map[node_id] = nodes # creating nodeid nodenumber map
			#self.node_fileblk_map[self.storage_node[nodes].node_id] = [] # initializing node fileblock map e.g {DN0,['A1','B1']}
			#print "**Nodes :"+str(nodes)+" datanodes = "+str(datanodes)+" datanodes+paritynodes = "+str(datanodes+paritynodes)
        	#if((nodes >= datanodes) or (nodes < datanodes+paritynodes)):  
		for nodes in range(datanodes,datanodes+paritynodes):
			#print"&&&&&&"
			#self.storage_node.append(StorageNode(10, 100, "PN"+str((datanodes+paritynodes-1)-nodes),"ParityNode"))
			self.storage_node.append(StorageNode(5, 100, "PN-"+str(nodes),"ParityNode"))
			node_id = self.storage_node[nodes].node_id
			#print "*Nodes :"+str(nodes)+" node_id " + str(node_id)
			self.tbl_node_allocation[node_id] = []
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
			self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]#lists #populating allocation table Tbl4
			self.list_free_nodes[node_id] = []
			self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status)
			#self.nodeid_nodenumber_map[node_id] = nodes
			#self.node_fileblk_map[self.storage_node[nodes].node_id] = []
		for nodes in range(datanodes+paritynodes,datanodes+paritynodes+buffernodes):
			#print"&&&&&&"
			self.storage_node.append(StorageNode(5, 100, "BN-"+str((datanodes+paritynodes+buffernodes-1)-nodes),"BufferNode"))
			node_id = self.storage_node[nodes].node_id
			#print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[node_id] = []
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
			self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]#lists #populating allocation table Tbl4
			self.list_free_nodes[node_id] = []
			self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status)
			#self.nodeid_nodenumber_map[node_id] = nodes
			#self.node_fileblk_map[node_id] = []
	# node registration with master

	# Scaling - Full type where another (n,k) storage will be created, if single type then one node will be created.
	#step 1 : set scale flag = false if true
	#step 2 : if scale type == full then 
	#step 3 : 	create new cluster with all meta data tables initialised.
	#step 4 : else if scale type == single then 
	#step 5	:	create new node with all meta data tables initialised.
	#@profile
	def scale(self,type,node_count = 1) :
		print "Scaling :"+ str(type)
		if(self.scale_flag == True):
			self.scale_flag = False
		if(type == "Full"):
			self.scaled += 1
			print "Full Scaling : "+str(self.scaled)
			start_node = (self.scaled-1) * self.max_nodes + self.buffernodes
			end_node =  start_node + self.datanodes
			print " **** scale : start_node "+ str(start_node)+ " end_node : "+str(end_node)
			raw_input("Please press any button... ")
			for nodes in range(start_node, end_node):
				self.storage_node.append(StorageNode(5, 100, "DN-"+str(nodes),"DataNode"))# creating data nodes
				node_id = self.storage_node[nodes].node_id
				self.tbl_node_allocation[node_id] = [] # intializing allocation table Tbl4
				#print "*Nodes :"+str(nodes)+" node_id " + str(node_id)
				allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
				self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]#lists #populating allocation table Tbl4
				self.list_free_nodes[node_id] = [] # intializing free node list e.g [DN0,'Free']
				self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status) # populating free node lsit
				#self.nodeid_nodenumber_map[node_id] = nodes # creating nodeid nodenumber map
				#self.node_fileblk_map[node_id] = [] # initializing node fileblock map e.g {DN0,['A1','B1']}
			start_node = end_node
			end_node = start_node + self.paritynodes
			print " $$$$ start_node "+ str(start_node)+ " end_node : "+str(end_node)
			for nodes in range(start_node ,end_node):
				self.storage_node.append(StorageNode(5, 100, "PN-"+str(nodes),"ParityNode"))
				node_id = self.storage_node[nodes].node_id
				#print "*Nodes :"+str(nodes)+" node_id " + str(node_id)
				self.tbl_node_allocation[node_id] = []
				allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
				self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]#lists #populating allocation table Tbl4
				self.list_free_nodes[node_id] = []
				self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status)
				#self.nodeid_nodenumber_map[node_id] = nodes
				#self.node_fileblk_map[node_id] = []
			start_node = end_node
			end_node = start_node + self.buffernodes
			print " $$$$ start_node "+ str(start_node)+ " end_node : "+str(end_node)
			for nodes in range(start_node,end_node):
				self.storage_node.append(StorageNode(5, 100, "BN-"+str(nodes),"BufferNode"))
				node_id = self.storage_node[nodes].node_id
				#print "*Nodes :"+str(nodes)+" node_id " + str(node_id)
				self.tbl_node_allocation[node_id] = [] 
				allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
				self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]
				self.list_free_nodes[node_id] = []
				self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status)
				#self.nodeid_nodenumber_map[node_id] = nodes
				#self.node_fileblk_map[node_id] = []
		elif(type == "Single"): # single node scaling
			print "Single Scaling : "+str(self.scaled)
			self.buffernodes += 1 # increment the number of buffer nodes by one
			nodes = (self.scaled) * self.max_nodes + self.buffernodes -1
			self.storage_node.append(StorageNode(10, 100, "BN-"+str(nodes),"BufferNode"))
			node_id = "BN-"+str(nodes) #self.storage_node[nodes].node_id
			print "*Nodes :"+str(nodes)+" node_id " + str(node_id)
			self.tbl_node_allocation[node_id] = [] 
			allocation = 1000#self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
			self.tbl_node_allocation[node_id] = [allocation, allocation, "Live",[]]
			print self.tbl_node_allocation
			self.list_free_nodes[node_id] = []
			self.list_free_nodes[node_id].append(self.storage_node[nodes].node_availability_status)

	# write file to node
	#step 1 : for each node in the current cluster
	#step 2 : 	if the number of live nodes in the cluster != required number of nodes then do not write
	#step 3 :  	else if the number of free blocks in the nodes of the cluster is NULL go for scaling.
	#step 4 :	write data on nodes.

	#@profile
	def write_file(self, file_name, file_data=[]):  
		blk_list = []
		list_blk_list = []
		data_count = 0
		status = "No Duplicates"
		no_of_nodes = len(self.storage_node)
		print "\n @@@@@ no_of_nodes " + str(no_of_nodes)
		live_node_count = 0
		for nodes in range(no_of_nodes):
			if(self.storage_node[nodes].node_stat == "Live"):
				live_node_count += 1
		if(live_node_count < self.max_nodes):
			print "Required "+str(self.max_nodes) +" Available only "+str(live_node_count)
			print "Sufficient number of nodes not available"
			return
		#if(self.add_to_file_list(file_name) == "Duplicate"):  # removed by ojus since it was consuming too much of memory.
		if(status !="No Duplicates"):		 #  
			status = "Duplicate File Exist"  # No specific purpose, but to keep the 
		else:
			if not (self.storage_node[(self.scaled-1) * self.max_nodes+2].give_list_free_blocks()): # checking to see the nodes are full and if so scale
				print "\n Nodes full,					 go for scaling \n"
				status = "Scaling Required"
				print "Scaling stopped"
				#return
				#self.scale("Full")
				return
			elif(self.scale_flag == True):
				print "\n Node "+str(self.force_scale_node) + " full "
				return

			self.tbl_file_blocks_list = self.split_file(file_name)
			#print "LLL"+ str(self.tbl_file_blocks_list)
			self.tbl_file_blocks_map[file_name] = self.split_file(file_name) # append the file blocks list (Tbl1)
			#print "### self.tbl_file_blocks_map[file_name] = "+ str(self.tbl_file_blocks_map[file_name])
			#print self.storage_node[0].give_list_free_blocks()
			print "self.scaled : "+ str(self.scaled)
			if(self.scaled == 1): # checks how many scaling has happened 0 - no scaling, 1 - one scaling , ...
				start_node = 0   # assign start node
				end_node = self.max_nodes + self. buffernodes # end node
			elif(self.scaled > 1): # scaling has happened 
				start_node = (self.scaled-1) * self.max_nodes+self.buffernodes
				end_node = start_node + self.max_nodes
			print "self scale : "+str(self.scaled) +" start : "+str(start_node)+" end_node : "+str(end_node)
			
				#return status 
			for nodes in range(start_node,end_node): # trying to write the data to the node blocks.
				#for blocks in range(storage_node[nodes].max_nodes)
					print "Nodes : "+ str(nodes)
					write_node_id = self.storage_node[nodes].node_id
					print "write_node_id "+ write_node_id
					if(self.storage_node[nodes].node_stat == "Failed"): # added to check if the node is a failed node and skip that node
						continue
					if(self.storage_node[nodes].node_availability_status == "Free"): # rChanging the availability status of the node from Free to Used on first use
						self.storage_node[nodes].node_availability_status == "Used"
						if(write_node_id in self.list_free_nodes): # check for conditions of new buffer nodes
							del(self.list_free_nodes[write_node_id])
					blk_list = self.storage_node[nodes].give_next_free_block()
					#print "blk_list"+str(blk_list)
					#print "***** "+self.storage_node[nodes].node_stat
					#print "***** xxxx "+self.storage_node[nodes].write_to_node_stripe(file_data[data_count],blk_list[1])
					#print "\nwrite_node_id : "+str(write_node_id)
					#print "##### self.tbl_node_allocation[write_node_id][1] : \n" + str(self.tbl_node_allocation[write_node_id][2]) 
					if(self.storage_node[nodes].write_to_node_stripe(file_data[data_count],blk_list[1]) == "Done"):# writing data to node block (Inside if )
						print "~~~##### self.tbl_node_allocation["+write_node_id+"][1] : \n" + str(self.tbl_node_allocation[write_node_id][1]) 
						self.tbl_node_allocation[write_node_id][1] = self.tbl_node_allocation[write_node_id][1] - self.storage_node[nodes].block_size # updating allocation table Tbl4
						print "~~~#####))[] self.tbl_node_allocation["+write_node_id+"][1] : \n" + str(self.tbl_node_allocation[write_node_id][1]) 
						if(self.tbl_node_allocation[write_node_id][1] == 0):
							print "Scale flag True***"
							self.scale_flag = True
							self.force_scale_node = write_node_id
							self.scale("Full")
							break
						#print "##### self.tbl_node_allocation[write_node_id][1] : \n" + str(self.tbl_node_allocation[write_node_id][1])
						#print "*self.storage_node[nodes].node_id-- > "+ self.storage_node[nodes].node_id
						#print "*@@@@self.tbl_file_blocks_map[file_name][nodes]-- > "+ self.tbl_file_blocks_map[file_name][data_count] # nodes- temp to get count from zero
						#self.node_fileblk_map[write_node_id].append(self.tbl_file_blocks_map[file_name][data_count])
						self.tbl_node_allocation[write_node_id][3].append(self.tbl_file_blocks_map[file_name][data_count]) # added to combine arrays nodeid_nodemap,node_fileblk_map with tbl_node_allocation
					elif (self.storage_node[nodes].node_availability_status == "Failed"): 
						print "Node Failed"
						status = "Failed"
						return status  
					#print "&&&&&&&&&&&&& Data Count : "+str(data_count)
					#print "\n###$ "+str(self.tbl_file_blocks_list[data_count])
					#print "\n###$$$$$$ "+str(self.tbl_file_blocks_map[file_name][data_count])
					self.tbl_fileblk_nodeblk_map[self.tbl_file_blocks_map[file_name][data_count]] = blk_list # populating Tbl2
					list_blk_list.append(blk_list)
					#print("--- %s seconds ---" % (time.time() - start_time))
					#print "list_blk_list"+str(list_blk_list)
					data_count += 1
			#print "list_blk_list -- > "+ str(list_blk_list)
			#self.tbl_file_stripe_map[file_name] = list_blk_list
			self.write_stripe_list(file_name,list_blk_list) # adding to file_stripe_map
			#print "self.tbl_file_stripe_map[file_name] - "+ file_name+ " "+str(self.tbl_file_stripe_map)
			del self.tbl_file_blocks_list[:]
			#del blk_list[:]
			#del list_blk_list[:]
			#print "blk_list - "+str(blk_list)
		return status

	#  Delete a file 
	# Step1 - get file fragments of the file to be deleted from the tbl_file_blocks_map.
	# step2 - for each of the fragment delete them from the node block and free the node block. Then update the node_allocation tbl.
	# step3 - check if the whole of the blocks in the node are free, if then add the node to free node list.
	# step4 - delete the meta data of the file from the tbl_file_blocks_map and tbl_file_stripe_map.
	def delete_file(self, file_name):
		if(file_name not in self.tbl_file_blocks_map):
			print " \n No such file exists...."
			status = "File Doesnot Exist"
		else:
			filefraglist = self.tbl_file_blocks_map[file_name] # get fragments of the file to be deleted.
			print "\nfilefraglist :"+str(filefraglist) 
			#for each of the fragment delete them from the node block and free the node block. Then update the node_allocation tbl.
			for filefrags in filefraglist: 
				print "\n Filefrag : "+ filefrags
				print "\n self.tbl_fileblk_nodeblk_map :"+ str(self.tbl_fileblk_nodeblk_map[filefrags])
				if(filefrags in self.tbl_fileblk_nodeblk_map):
					print "yes I found "
					free_node = self.tbl_fileblk_nodeblk_map[filefrags][0]
					free_block = self.tbl_fileblk_nodeblk_map[filefrags][1]
					print free_node
					if(free_node not in self.tbl_node_allocation):
						print "No matching block found"
					else:
						print " Before updating :  self.tbl_node_allocation[free_block][1] : "+str(self.tbl_node_allocation[free_node][3][0])
						self.tbl_node_allocation[free_node][1] += self.storage_node[self.give_node_number(free_node)].block_size
						if(filefrags in self.tbl_node_allocation[free_node][3]):
							delcounter = 0
							dellen = len(self.tbl_node_allocation[free_node][3]) # number of blocks in the node
							while(dellen >= delcounter):
								if(filefrags == self.tbl_node_allocation[free_node][3][delcounter]):
									del self.tbl_node_allocation[free_node][3][delcounter]
									break
								else:
									delcounter += 1
						print " After updating :  self.tbl_node_allocation[free_block][1] : "+ str(self.tbl_node_allocation[free_node])
						#check if the whole of the blocks in the node are free, if then add the node to free node list.
						if(self.tbl_node_allocation[free_node][1] == self.tbl_node_allocation[free_node][0]): 
							self.list_free_nodes[free_node] = []
							free_node_num = self.give_node_number(free_node)
							self.list_free_nodes[free_node].append("Free") # adding the node to the free list
							self.storage_node[free_node_num].block_stat[free_block] = "Free"
							self.storage_node[free_node_num].node_availability_status  = "Free" # make it free internally node level
					del self.tbl_fileblk_nodeblk_map[filefrags]
			#delete the meta data of the file from the tbl_file_blocks_map and tbl_file_stripe_map.
			del(self.tbl_file_stripe_map[file_name])	
			del(self.tbl_file_blocks_map[file_name])
			status = "Deleted"
		return status


	# display all blocks of all nodes
	def display_nodes(self):
		start_node = 0
		end_node = self.max_nodes+self.buffernodes
		for scale in range(0, self.scaled): # to handle scaled case
			#print " \n "+str(scale)+" scale"
			#print " **** start_node "+ str(start_node)+ " end_node : "+str(end_node) + " Scale : "+ str(scale)
			for nodes in range(start_node, end_node):
				print "\n***** Node "+str(self.storage_node[nodes].node_id)+ " "+str(self.storage_node[nodes].node_type)+" Scale : "+ str(scale)+" nodes : "+ str(nodes)+" ***** \n"
				self.storage_node[nodes].list_all_blocks()
			start_node = end_node
			end_node = start_node + (self.max_nodes+self.buffernodes)

	#split file into blocks given the file name
	#@profile
	def split_file(self, filename):
		file_blks = []
		for nodes in range(self.max_nodes-self.paritynodes):
			file_blks.append(filename+str(nodes))
		for nodes in range(self.datanodes,self.max_nodes):
			file_blks.append("P"+filename+str(nodes))
		#print file_blks
		return file_blks
	
	# check for duplicate files and add to file list
	#@profile
	def add_to_file_list(self, file_name):
		status = ""
		if(self.file_list== []):
			self.file_list.append(file_name)
		else:
			for files in range(len(self.file_list)):
				if(self.file_list[files] == file_name):
					status = "Duplicate"
				else:
					self.file_list.append(file_name)
					status = "Added"
		return status

	# returns the list of blocks of a given file.
	def give_file_blocks_list(self,filename):
		#self.tbl_file_blocks_list = self.tbl_file_blocks_map[filename] # removed by Ojus as trial to reduce memory consumption.
		return self.tbl_file_blocks_map[filename]
	
	# update the list of blocks of a given file	
	def write_file_blocks_list(self,filename,file_block_list):
		self.tbl_file_blocks_map[filename] = file_block_list
		
	# return node block given the file block
	def give_nodeblk(self, fileblk):
		return self.tbl_fileblk_nodeblk_map[fileblk]
	
	# update node block given the file block.
	def write_nodeblk(self, fileblk, nodeblk):
		self.tbl_fileblk_nodeblk_map[fileblk].append(nodeblk)
	
	# return the stripes list given the file name	
	def give_stripe_list(self, filename):
		return self.tbl_file_stripe_map
		
	# update the stripe list given the file name
	def write_stripe_list(self, filename, stripe_list):
		#print "\nInside write_stripe_list Filename :"+str(filename) +" stripe_list : "+str(stripe_list)
		#print "\nself.tbl_file_stripe_map[filename].append(stripe_list) :"+str(self.tbl_file_stripe_map[filename])
		self.tbl_file_stripe_map[filename] = stripe_list
		#print "\n Moving out of write_stripe_list Filename :"+str(self.tbl_file_stripe_map) 

	# update node id with a new node id on node failure followed by recovery
	def update_stripe_list_nodeid(self,old_node_id,new_node_id,blknum):
		newlist = ""
		flag = "false"
		#print "\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ old_node_id :"+str(old_node_id)
		#print "\n############################### new_node_id :"+str(new_node_id)
		#print "\nblknum : "+str(blknum)
		for files in self.tbl_file_blocks_map:
			#print "\n&&&&&&&&&&&&&&& File : "+str(files)
			#nodelist = self.tbl_file_stripe_map[files]
			nodelist = []
			filefraglist = self.tbl_file_blocks_map[files]
			#print "\nfilefraglist :"+str(filefraglist) 
			for filefrags in filefraglist:
				if(filefrags in self.tbl_fileblk_nodeblk_map):
					nodelist.append(self.tbl_fileblk_nodeblk_map[filefrags])
			self.write_stripe_list(files,nodelist)
		return

	# return the allocation list for a storage node
	def give_allocation_list(self, node):
		return self.tbl_node_allocation[node]
	
	# update the allocation list given the storage node
	def write_allocation_list(self, node, allocation_list):
		self.tbl_node_allocation[node].append(allocation_list) 
	
	# display all tables of the master
	#@profile
	def display_tables(self):
		print "\n******************************************\n"
		print "\n tbl_file_blocks_map : "+str(self.tbl_file_blocks_map)
		print "\n tbl_fileblk_nodeblk_map :" +str(self.tbl_fileblk_nodeblk_map)
		print "\n tbl_file_stripe_map : " +str(self.tbl_file_stripe_map)
		print "\n tbl_node_allocation :" + str(self.tbl_node_allocation)
		print "\n list_free_nodes : "+ str(self.list_free_nodes)
		#print "\n nodeid_nodenumber_map : "+str(self.nodeid_nodenumber_map)
		#print "\n node_fileblk_map : "+str(self.node_fileblk_map) #Ojus removed as aprt of combining arrays
		print "\n*******************************************\n"
	
	# make a specific node fail
	def make_node_fail(self, node, can_scale="Yes"):
		#if(node >= 0 and node < self.max_nodes):
		print "### can_scale : "+can_scale
		if(can_scale == "Yes"):
			self.scale("Single") # single node scale to compensate for the failed node. All data of failed node will get regenerated to new node
		fail_node_id = self.storage_node[node].node_id
		result = self.storage_node[node].make_node_fail()
		print "result = "+str(result)
		if(result =="Failed"):
			self.tbl_node_allocation[fail_node_id][2] = "Failed"
			print "\n !!! Failed : "+str(fail_node_id)+" : "+str(self.tbl_node_allocation[fail_node_id][2])
			self.start_recovery(fail_node_id)
			self.tbl_node_allocation[fail_node_id][1] += self.storage_node[node].block_size
			return result
		elif(result == "Already Failed"):
			return result
	
	# make a specific node live
	def make_node_live(self, node):
		if(node >= 0 and node < self.max_nodes):
			if(self.storage_node[node].make_node_live() == "Live"):
				self.tbl_node_allocation[self.storage_node[node].node_id][2] = "Live"
	
	# do the recovery operation on node failure reporting in Replace mode.
	def start_recovery_replace_mode(self,node_id):
		#print "BufferNodes : "+str(self.buffernodes)
		if((self.buffernodes - self.buffernodes_used<= 0)):
			print "No buffer node Recovery not possible"
			return "Recovery Failure"
		elif(self.recovery_rule == "Replace"):
			print "\n Recovery Started"
			freenode = self.use_free_node()
			if(self.copy_data(node_id,freenode) == "Done"):
				print "Recovery Sucess"
			else:
				print "Recovery Failed"

		
	#find next free node
	def use_free_node(self):
		 freenode = next(self.list_free_nodes.iterkeys())
		 del(self.list_free_nodes[freenode])
		 self.buffernodes_used += 1
		 #self.buffernodes -= 1
		 return freenode

	# copy from node1 to node2
	#@profile
	def copy_data(self, src_node_id, destn_node_id):
		status = ""
		data = ""
		blk_list = []
		src_node_num = self.give_node_number(src_node_id) 
		#print "src_node_id : "+ str(src_node_id)+" src_node_num :" + str(src_node_num)
		#src_node_num = self.nodeid_nodenumber_map[src_node_id]
		destn_node_num = self.give_node_number(destn_node_id) # below line modified to avoid usage of nodeid_nodenumber_map
		#destn_node_num = self.nodeid_nodenumber_map[destn_node_id]
		#print "\n ^^^^^^^^^ In copy_data() ^^^^^^^^^^"
		if(self.storage_node[src_node_num].max_blocks > self.storage_node[destn_node_num].max_blocks):
			print "copy not possible"
			status = "NotDone"
		else:
			for nodeblocks in range(self.storage_node[src_node_num].max_blocks):
				print "nodeblocks  --> "+str(nodeblocks)
				if(self.storage_node[src_node_num].block_stat[nodeblocks] == "Free"):
					break
				data = self.storage_node[src_node_num].block_data[nodeblocks]
				print "data in "+str(nodeblocks)+" -- > "+ str(data)
				if(self.storage_node[destn_node_num].write_to_node_stripe(data,nodeblocks) == "Done"):# writing data to node block (Inside if )
						# update allocation table
						self.tbl_node_allocation[destn_node_id][1] = self.tbl_node_allocation[destn_node_id][1] - self.storage_node[destn_node_num].block_size # updating allocation table Tbl4
						#fileblk = self.node_fileblk_map[src_node_id][nodeblocks]
						fileblk = self.tbl_node_allocation[src_node_id][3][0]
						print "self.tbl_node_allocation[src_node_id][3] : "+str(self.tbl_node_allocation[src_node_id][3])
						print "\n@@@@fileblk : "+str(fileblk)
						#del(self.tbl_fileblk_nodeblk_map[self.node_fileblk_map[self.storage_node[src_node_num].node_id][nodeblocks]])
						print "self.tbl_fileblk_nodeblk_map[fileblk] : "+str(self.tbl_fileblk_nodeblk_map[fileblk])
						#update tbl2 fileblk-nodeblk map
						self.tbl_fileblk_nodeblk_map[fileblk] = [destn_node_id,nodeblocks]
						#update node fileblk map
						#self.node_fileblk_map[self.storage_node[destn_node_num].node_id].append(fileblk) Ojus removed as aprt of combining arrays
						self.tbl_node_allocation[destn_node_id][3].append(fileblk) # added by ojus to combine arrays
						self.tbl_node_allocation[src_node_id][3].remove(fileblk) # added by ojus to combine arrays
						#update file_stripe_map
						#print "self.storage_node[src_node_num].node_id,self.storage_node[destn_node_num].node_id,nodeblocks " +str(self.storage_node[src_node_num].node_id)+" ,"+str(self.storage_node[destn_node_num].node_id)+" ,"+str(nodeblocks)
						self.update_stripe_list_nodeid(src_node_id,destn_node_id,nodeblocks)
			status = "Done"
			#self.node_fileblk_map[src_node_id] = [] #Ojus removed as aprt of combining arrays
		return status
	
	# set recovery rule, return the new recovery rule if reset or old one if new recovery rule is same as old one.
	def set_recovery_rule(self, recovery_rule):
		if(self.recovery_rule == recovery_rule):
			print "\nCurrent Recovery rule is "+ str(recovery_rule) +" Need no reset"
		else:
			self.recovery_rule = recovery_rule
		return self.recovery_rule

	# return recovery rule
	def give_recovery_rule(self):
		return self.recovery_rule
	
	# do the recovery operation on node failure reporting in Replace mode.
	def start_recovery(self,node_id):
		print "\n ********* Starting Recovery ***********"
		if(self.buffernodes == 0):
			print "\n ********* No Free Nodes **********"
			self.set_recovery_rule("Adjust")
			self.start_recovery_adjust_mode(node_id)
		else:
			print "\n ******* Free Nodes Available ********"
			self.set_recovery_rule("Replace")
			self.start_recovery_replace_mode(node_id)

	# return the total available free bocks across all nodes
	def give_total_free_blocks_size(self):
		total_free_block_size = 0
		node_free_size_map = {}
		free_blocks = []
		for nodes in range(self.max_nodes):
			node_id = self.storage_node[nodes].node_id
			if(self.tbl_node_allocation[node_id][2] != "Failed"):
				print "\n self.tbl_node_allocation[self.storage_node[nodes].node_id] :" + str(self.tbl_node_allocation[node_id])
				if(self.tbl_node_allocation[node_id][1] != 0):
					node_free_size_map[node_id] = [self.tbl_node_allocation[node_id][1]]
		return node_free_size_map

	# returns the node number given the node id
	def give_node_number(self,node_id):
		nodeno = node_id.split('-')
		return int(nodeno[1])

	# return number of used blocks
	def give_number_of_used_blocks(self,node_id):
		number_used_blocks = 0
		node_number = self.give_node_number(node_id)
		#node_number = self.nodeid_nodenumber_map[node_id]
		max_blocks = self.storage_node[node_number].max_blocks
		for used_blocks in range(max_blocks):
			if(self.storage_node[node_number].block_stat[used_blocks] == "Allocated"):
				number_used_blocks += 1
		return number_used_blocks

	# start recovery in adjust mode
	# step 1 : estimate how much free space is available
	# step2  : estimate the total required space for recovery
	# step3  : find the number of blocks actually required for the recovery.
	# step4  : locate the next nodes having space
	# step5  : move the (recovered)data to the new locations 
	#@profile
	def start_recovery_adjust_mode(self, node_id):
		free_block_count = self.give_total_free_blocks_size()
		reqd_size = self.tbl_node_allocation[node_id][0]-self.tbl_node_allocation[node_id][1]
		nodenumber = self.give_node_number(node_id)
		reqd_no_blks = reqd_size/(self.storage_node[nodenumber].block_size)
		temp = 0
		available_space = 0
		nxtblk = []
		print "&&&&&& Node Failed :"+ str(node_id) + " free_block_count :" +str(free_block_count) + " reqd_size : "+str(reqd_size)
		for space in free_block_count:
			available_space += free_block_count[space][0]
		if(reqd_size > available_space):
			print "\nRecovery not Possible\n"
		else:
			count_bloks = 0 # used to track blks of the failed nodes getting copied.
			rqd_no_blks_actl = reqd_no_blks
			for nodes in free_block_count:
				data_blk = []
				#print "\n nodes : "+str(nodes) +"  self.nodeid_nodenumber_map[nodes] : "+str(self.nodeid_nodenumber_map[nodes])
				#print "\n free_block_count[nodes][0] "+str(free_block_count[nodes][0])
				temp_node_id = self.give_node_number(nodes)
				temp = int(math.floor(free_block_count[nodes][0]/self.storage_node[temp_node_id].block_size))
				print "\n nodes : "+str(nodes)+" temp : "+ str(temp) + " reqd_no_blks : "+str(reqd_no_blks) + " min(temp, reqd_no_blks) : "+str(min(temp, reqd_no_blks))
				for blk in range(min(temp, reqd_no_blks)):
					nxtblk = self.storage_node[temp_node_id].give_next_free_block()
					if(not nxtblk):
						break
					data_blk.append(nxtblk[1])
					data = self.storage_node[nodenumber].block_data[blk]
					print "\n nxtblk : "+ str(nxtblk) + " data = "+ str(data) +" nxtblk[1] : "+str(nxtblk[1])
					if(self.storage_node[temp_node_id].write_to_node_stripe(data,nxtblk[1]) == "Done"):
						#print "\n %%%%%%%%%%%%%%%%%Data Copied :" + str(self.storage_node[self.nodeid_nodenumber_map[nodes]].block_data[nxtblk[1]])
						#print "\n free_block_count[nodes][0] "+str(free_block_count[nodes][0])
						self.update_stripe_list_nodeid(node_id,nodes,nxtblk[1])
						reqd_no_blks -= 1
						free_block_count[nodes][0] -= self.storage_node[temp_node_id].block_size 
						self.tbl_node_allocation[nodes][1] = free_block_count[nodes][0]
						#print "\n ## free_block_count[nodes][0] "+str(free_block_count[nodes][0])
						if(reqd_no_blks == 0):
							break
				#fileblk = self.node_fileblk_map[node_id] # Ojus removed as aprt of combining arrays
				fileblk = self.tbl_node_allocation[node_id][3]
				#print " \n"+str(node_id)+" fileblk :" +str(fileblk)+ " data_blk : "+str(data_blk)
				count = 0
				len_cnt = len(data_blk)
				fileblk_list = []
				for fileblks in fileblk:
					#print " \n count : "+str(count) + "len_cnt : "+ str(len_cnt) +" fileblks : "+str(fileblks)
					if(count < len_cnt):
						#print "\n self.tbl_fileblk_nodeblk_map[fileblks] : "+ str(self.tbl_fileblk_nodeblk_map)
						del(self.tbl_fileblk_nodeblk_map[fileblks])
						fileblk_list.append(fileblks)
						#print "\n After Delete >> self.tbl_fileblk_nodeblk_map[fileblks] : "+ str(self.tbl_fileblk_nodeblk_map)
						self.tbl_fileblk_nodeblk_map[fileblk[count]]  = [nodes,data_blk[count]]
						count += 1
				for i in range(len(fileblk_list)):
					#self.node_fileblk_map[node_id].remove(fileblk_list[i])
					#print "############################### self.node_fileblk_map[node_id] : "+ str(self.node_fileblk_map[node_id])
					#print "fileblk_list["+ str(i)+"]"+str(fileblk_list[i])
					#print "~~~~~~~self.tbl_node_allocation[node_id][3]"+str(self.tbl_node_allocation[node_id][3])
					#print "(((( deleting : "+str(fileblk_list[i])
					self.tbl_node_allocation[node_id][3].remove(fileblk_list[i])# added by ojus for array combining
					self.tbl_node_allocation[nodes][3].append(fileblk_list[i])
					#self.node_fileblk_map[nodes].append(fileblk_list[i]) #Ojus removed as aprt of combining arrays
					#self.node_fileblk_map[node_id].remove(fileblk_list[i]) #Ojus removed as aprt of combining arrays
				#print "\n self.node_fileblk_map[node_id] : "+str(self.node_fileblk_map[node_id])
				if(reqd_no_blks == 0):
					break	

	# copy data in adjust mode
	#def copy_data_adjust_mode(self,src_node_id,destn_node_id,reqd_no_blks):

failed_nodes = []
start_time = time.time()
print "Start time : "+str(start_time)
#h = hpy()
print "%%%%%%% = "+str(sys.getsizeof({}))
ma = Master(14, 10, 4, 0)
#print "\n#######\n"
#ma.write_file_blocks_list("A",['A1','A2','PA1','PA2'])
#print "Blocks of A : "+ str(ma.give_file_blocks_list("A"))
#ma.make_node_fail(1)
#ma.write_file_blocks_list("B",['B1','B2','PB1','PB2'])
#ma.display_tables()
count = 0
'''for count in range (100):
	filename = "A"+str(count)
	raw_input("@@@@@ Pleas wait ...")	
	ma.write_file(filename,[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
	#if(count% == 0):
		#ma.make_node_fail(count/100)'''
exit = "T"	
#count = 0
while((exit=="T") or (exit == "t")):
	filename = "B"+str(count)
	count +=1
	raw_input("Please wait ...")	
	ma.write_file(filename,[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])  
	ma.display_tables()
	ma.display_nodes()
	answer = raw_input("\nFail a node ?...Y/N")
	if answer == "Y":
		node_to_fail = raw_input("Enter Node number to fail : ")
		if(ma.make_node_fail(int(node_to_fail),"No")== "Already Failed"):
			print "\n Can not fail an already failed node"
	exit = raw_input("DO you want to continue (T or F: ")
	if(exit=="F" or exit=="f"):
		break;
	#print "&&& "+str(count%1000)
	#if(count%1000 == 0):
	#	print "Failed\n"
	#	ma.make_node_fail(count)

print("--- %s seconds ---" % (time.time() - start_time))
#ma.write_file("A5",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.write_file("A6",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])																													
#ma.display_tables()
#ma.display_nodes()
ma.display_tables()
ma.display_nodes()
'''ma.make_node_fail(2)
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(3)
ma.display_tables()
ma.display_nodes()'
#raw_input("\nPress any key to continue ......")
#os.system('clear')
while (True):
	print "\n *********************************************"
	print "\n Enter 2 to add file"
	print "\n Enter 3 to delete file - not avaialble"
	print "\n Enter 4 to fail a node"
	print "\n Enter 5 to make a node live"
	print "\n Enter -1 to exit"
	print "\n************************************************"
	my_choice = int(raw_input("\n Enter your choice : "))
	print "\nYour choice : "+ str(my_choice)
	if my_choice == -1:
		print "My choice is : "+str()
		break
	elif my_choice == 2:
		print "\ngoing to add a file"
		filename = raw_input("Enter file name : ")
		ma.write_file(filename,[100,1001,102,103,104,105,106,107,108,109,110,111,112,113,114])
		#ma.display_tables()
	elif my_choice == 3:
		print "\ngoing to delete a file"
		filename = raw_input("Enter file name : ")
		ma.delete_file(filename)
	elif my_choice == 4:
		print "\ngoing to make a node to fail"
		node_to_fail = int(raw_input("\n Enter node to fail : "))
		ma.make_node_fail(node_to_fail)
	elif my_choice == 5:
		print "\n going to make a node live"
		node_to_live = int(raw_input("\n Enter node to fail : "))
		ma.make_node_live(node_to_live)
	else:
		print "\n Invalid Choice - please reenter your choice"
	ma.display_tables()
	ma.display_nodes()
	raw_input("\nPress any key to continue ......")	
	os.system('clear')'''
#ma.make_node_fail(4)
#ma.display_tables()
#ma.display_nodes()
print "recovery rule : "+str(ma.give_recovery_rule())
ma.set_recovery_rule("Adjust")
print "New recovery rule : "+str(ma.give_recovery_rule())


		
		
