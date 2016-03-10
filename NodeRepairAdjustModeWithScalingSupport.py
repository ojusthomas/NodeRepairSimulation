

import math
import time
#from guppy import hpy
import sys
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
		#print "inside write_to_node_stripe --- > "+ self.node_stat
		if(self.node_stat == "Live"):
			if(self.node_availability_status=="Free"):
				self.node_availability_status = "Used"
			if(self.block_stat[block_no] == "Free"):
				#print "self.block_count = " + str(self.block_count) 
				self.block_data[block_no] = data
				self.block_stat[block_no] = "Allocated"
				self.block_count += 1
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
		else:
			print "\n&&& &&& Node already failed\n"
		return self.node_stat
	
	#	make the node live	
	def make_node_live(self):
		if(self.node_stat == "Live"):
			print "\nNode Already Live\n"
		else:
			self.node_stat = "Live"
			print "\n node live now"
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
	tbl_fileblk_nodeblk_map = {} # Tbl2 File block to node block map eg : A1, [DN1,1]
	tbl_file_stripe_map = {} # Tbl3 File, stripe map eg : A ,[[DN1,1],[DN2,1], [PN3,1], [PN4,1]]
	tbl_node_allocation = {} # Tbl4 Node allocation map eg : DN1, [Fail], DN2,[50,20]
	tbl_file_blocks_list = [] # hold the file block list eg. [A1,A2,PA1,PA2]
	tbl_file_stripe_list = [] # hold the file stripe list eg. [[DN1,1],[DN2,1], [PN3,1], [PN4,1]]
	tbl_allocation_list = [] # [50,20]
	storage_node = [] # list of storage nodes
	file_list = [] # list of files stored
	replacement_list = {} #list of nodes with replacement node for each in case of failure. eg. ['DN1','BN1'] BN1-BufferNode1 is the replacement for DN1
	recovery_rule = "Adjust" # "Replace" - replace with a new node, "Adjust" - adjust with existing nodes
	list_free_nodes = {} # list of free nodes i.e having the node_availability_status = "Free"
	nodeid_nodenumber_map = {} # Keep track of node
	node_fileblk_map = {} # keep track of file blocks in each node

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
			self.storage_node.append(StorageNode(10, 100, "DN"+str(nodes),"DataNode"))# creating data nodes
			#print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [] # intializing allocation table Tbl4
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]#lists #populating allocation table Tbl4
			self.list_free_nodes[self.storage_node[nodes].node_id] = [] # intializing free node list e.g [DN0,'Free']
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status) # populating free node lsit
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes # creating nodeid nodenumber map
			self.node_fileblk_map[self.storage_node[nodes].node_id] = [] # initializing node fileblock map e.g {DN0,['A1','B1']}
			#print "**Nodes :"+str(nodes)+" datanodes = "+str(datanodes)+" datanodes+paritynodes = "+str(datanodes+paritynodes)
        	#if((nodes >= datanodes) or (nodes < datanodes+paritynodes)):  
		for nodes in range(datanodes,datanodes+paritynodes):
			#print"&&&&&&"
			self.storage_node.append(StorageNode(10, 100, "PN"+str((datanodes+paritynodes-1)-nodes),"ParityNode"))
			print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = []
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.list_free_nodes[self.storage_node[nodes].node_id] = []
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
			self.node_fileblk_map[self.storage_node[nodes].node_id] = []
		for nodes in range(datanodes+paritynodes,datanodes+paritynodes+buffernodes):
			#print"&&&&&&"
			self.storage_node.append(StorageNode(10, 100, "BN"+str((datanodes+paritynodes+buffernodes-1)-nodes),"BufferNode"))
			#print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = []
			#lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.list_free_nodes[self.storage_node[nodes].node_id] = []
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
			self.node_fileblk_map[self.storage_node[nodes].node_id] = []
	# node registration with master

	# Scaling - Full type where another (n,k) storage will be created, if partial  type then specify the number of nodes to be created.
	def scale(self,type,node_count = 1) :
		print "Scaling :"+ str(type)
		self.scaled += 1
		if(type == "Full"):
			print "Full Scaling : "+str(self.scaled)
			start_node = (self.scaled-1) * self.max_nodes + 2
			end_node =  start_node + self.datanodes
			#print " **** start_node "+ str(start_node)+ " end_node : "+str(end_node)
			for nodes in range(start_node, end_node):
				self.storage_node.append(StorageNode(10, 100, "DN"+str(nodes),"DataNode"))# creating data nodes
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = [] # intializing allocation table Tbl4
				#print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
				allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = [allocation, allocation, "Live"]#lists #populating allocation table Tbl4
				self.list_free_nodes[self.storage_node[nodes].node_id] = [] # intializing free node list e.g [DN0,'Free']
				self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status) # populating free node lsit
				self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes # creating nodeid nodenumber map
				self.node_fileblk_map[self.storage_node[nodes].node_id] = [] # initializing node fileblock map e.g {DN0,['A1','B1']}
			start_node = end_node
			end_node = start_node + self.paritynodes
			#print " $$$$ start_node "+ str(start_node)+ " end_node : "+str(end_node)
			for nodes in range(start_node ,end_node):
				#print "Nodes : "+ str(nodes)
				self.storage_node.append(StorageNode(10, 100, "PN"+str(nodes),"ParityNode"))
				print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = []
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
				self.list_free_nodes[self.storage_node[nodes].node_id] = []
				self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
				self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
				self.node_fileblk_map[self.storage_node[nodes].node_id] = []
			start_node = end_node
			end_node = start_node + self.buffernodes
			#print " $$$$ start_node "+ str(start_node)+ " end_node : "+str(end_node)
			for nodes in range(start_node,end_node):
				self.storage_node.append(StorageNode(10, 100, "BN"+str(nodes),"BufferNode"))
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = [] 
				allocation = self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size
				self.tbl_node_allocation[self.storage_node[nodes].node_id] = [allocation, allocation, "Live"]
				self.list_free_nodes[self.storage_node[nodes].node_id] = []
				self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
				self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
				self.node_fileblk_map[self.storage_node[nodes].node_id] = []

	#@profile
	def write_file(self, file_name, file_data=[]):  
		blk_list = []
		list_blk_list = []
		data_count = 0
		status = "No Duplicates"
		print "\n writing " + file_name
		#if(self.add_to_file_list(file_name) == "Duplicate"):  # removed by ojus since it was consuming too much of memory.
		if(status !="No Duplicates"):		 #  
			status = "Duplicate File Exist"  # No specific purpose, but to keep the 
		else:
			if not (self.storage_node[(self.scaled-1) * self.max_nodes + 2].give_list_free_blocks()):
				print "\n Nodes full,					 go for scaling \n"
				status = "Scaling Required"
				self.scale("Full")
				#return
			self.tbl_file_blocks_list = self.split_file(file_name)
			#print "LLL"+ str(self.tbl_file_blocks_list)
			self.tbl_file_blocks_map[file_name] = self.split_file(file_name) # append the file blocks list (Tbl1)
			#print "### self.tbl_file_blocks_map[file_name] = "+ str(self.tbl_file_blocks_map[file_name])
			#print self.storage_node[0].give_list_free_blocks()
			#print "self.scaled : "+ str(self.scaled)
			if(self.scaled == 1): # checks how many scaling has happened 0 - no scaling, 1 - one scaling , ...
				start_node = 0   # assign start node
				end_node = self.max_nodes-1 # end node
			elif(self.scaled > 1): # scaling has happened 
				start_node = (self.scaled-1) * self.max_nodes+2
				end_node = start_node + self.max_nodes - 1
			print "self scale : "+str(self.scaled) +" start : "+str(start_node)+" end_node : "+str(end_node)
			
				#return status 
			for nodes in range(start_node,end_node): # trying to write the data to the node blocks.
				#for blocks in range(storage_node[nodes].max_nodes)
					#print "Nodes : "+ str(nodes)
					if(self.storage_node[nodes].node_stat == "Failed"): # added to check if the node is a failed node and skip that node
						continue
					if(self.storage_node[nodes].node_availability_status == "Free"): # rChanging the availability status of the node from Free to Used on first use
						self.storage_node[nodes].node_availability_status == "Used"
						del(self.list_free_nodes[self.storage_node[nodes].node_id])
					blk_list = self.storage_node[nodes].give_next_free_block()
					#print "blk_list"+str(blk_list)
					#print "***** "+self.storage_node[nodes].node_stat
					#print "***** xxxx "+self.storage_node[nodes].write_to_node_stripe(file_data[data_count],blk_list[1])
					if(self.storage_node[nodes].write_to_node_stripe(file_data[data_count],blk_list[1]) == "Done"):# writing data to node block (Inside if )
						#print "##############\n" 
						self.tbl_node_allocation[self.storage_node[nodes].node_id][1] = self.tbl_node_allocation[self.storage_node[nodes].node_id][1] - self.storage_node[nodes].block_size # updating allocation table Tbl4
						#print "*self.storage_node[nodes].node_id-- > "+ self.storage_node[nodes].node_id
						#print "*@@@@self.tbl_file_blocks_map[file_name][nodes]-- > "+ self.tbl_file_blocks_map[file_name][data_count] # nodes- temp to get count from zero
						self.node_fileblk_map[self.storage_node[nodes].node_id].append(self.tbl_file_blocks_map[file_name][data_count])
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
			self.tbl_file_stripe_map[file_name] = list_blk_list
			#print "self.tbl_file_stripe_map[file_name] - "+ file_name+ " "+str(self.tbl_file_stripe_map[file_name])
			del self.tbl_file_blocks_list[:]
			del blk_list[:]
			del list_blk_list[:]
			#print "blk_list - "+str(blk_list)
		return status

	# display all blocks of all nodes
	def display_nodes(self):
		start_node = 0
		end_node = self.max_nodes+self.buffernodes
		for scale in range(0, self.scaled): # to handle scaled case
			print " \n "+str(scale)+" scale"
			print " **** start_node "+ str(start_node)+ " end_node : "+str(end_node) + " Scale : "+ str(scale)
			for nodes in range(start_node, end_node):
				print "\n***** Node "+str(self.storage_node[nodes].node_id)+ " "+str(self.storage_node[nodes].node_type)+" Scale : "+ str(scale)+" nodes : "+ str(nodes)+" ***** \n"
				self.storage_node[nodes].list_all_blocks()
			start_node = end_node
			end_node = start_node + (self.max_nodes+self.buffernodes -2)

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
		return self.tbl_file_stripe_map[filename]
		
	# update the stripe list given the file name
	def write_stripe_list(self, filename, stripe_list):
		#print "\nInside write_stripe_list Filename :"+str(filename) +" stripe_list : "+str(stripe_list)
		#print "\nself.tbl_file_stripe_map[filename].append(stripe_list) :"+str(self.tbl_file_stripe_map[filename])
		self.tbl_file_stripe_map[filename] = stripe_list
		#print "\n Moving out of write_stripe_list Filename :"+str(filename) +" stripe_list : "+str(stripe_list)

	# update node id with a new node id
	def update_stripe_list_nodeid(self,old_node_id,new_node_id,blknum):
		nodelist = ""
		newlist = ""
		flag = "false"
		#print "\nold_node_id :"+str(old_node_id)
		#print "\nnew_node_id :"+str(new_node_id)
		#print "\nblknum : "+str(blknum)
		for files in self.file_list:
			#print "File : "+str(files)
			nodelist = self.tbl_file_stripe_map[files]
			#print "\nnodelist :"+str(nodelist)
			for nodeslists in nodelist:
				#print "\n@@@@ nodeslist[0] :"+str(nodeslists[0]) + " old_node_id :"+str(old_node_id)
				if(nodeslists[0] == old_node_id):
					flag = "true"
					#print "\n&&&&&&&&&&&&&&"
					nodeslists[0] = new_node_id
					nodeslists[1] = blknum
					#print "\n### nodeslist :"+str(nodeslists) + " blkno :"+str(blknum)
					
			#newlist.append(nodeslists)
			#print "\nOut of For nodelist : "+str(nodelist)
			if(flag == "true"):
				#print "\n ***************** Break ******************"
				break
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
		print "\n nodeid_nodenumber_map : "+str(self.nodeid_nodenumber_map)
		print "\n node_fileblk_map : "+str(self.node_fileblk_map)
		print "\n*******************************************\n"
	
	# make a specific node fail
	def make_node_fail(self, node):
		#if(node >= 0 and node < self.max_nodes):
		#print "###"+str(self.tbl_node_allocation[self.storage_node[node].node_id][2])
		if(self.storage_node[node].make_node_fail()=="Failed"):
			self.tbl_node_allocation[self.storage_node[node].node_id][2] = "Failed"
			print "\n Failed : "+str(self.storage_node[node].node_id)+" : "+str(self.tbl_node_allocation[self.storage_node[node].node_id][2])
			self.start_recovery(self.storage_node[node].node_id)
	
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


			#self.storage_node.append(StorageNode(5, 10, "DN"+str(nodes),"DN"))
			#self.tbl_node_allocation[self.storage_node[nodes].node_id] = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size]
	
	#find next free node
	def use_free_node(self):
		 freenode = next(self.list_free_nodes.iterkeys())
		 del(self.list_free_nodes[freenode])
		 self.buffernodes_used += 1
		 #self.buffernodes -= 1
		 return freenode

	# copy from node1 to node2
	def copy_data(self, src_node_id, destn_node_id):
		status = ""
		data = ""
		blk_list = []
		src_node_num = self.nodeid_nodenumber_map[src_node_id]
		destn_node_num = self.nodeid_nodenumber_map[destn_node_id]
		if(self.storage_node[src_node_num].max_blocks > self.storage_node[destn_node_num].max_blocks):
			print "copy not possible"
			status = "NotDone"
		else:
			for nodeblocks in range(self.storage_node[src_node_num].max_blocks):
				#print "nodeblocks  --> "+str(nodeblocks)
				if(self.storage_node[src_node_num].block_stat[nodeblocks] == "Free"):
					break
				data = self.storage_node[src_node_num].block_data[nodeblocks]
				print "data in "+str(nodeblocks)+" -- > "+ str(data)
				if(self.storage_node[destn_node_num].write_to_node_stripe(data,nodeblocks) == "Done"):# writing data to node block (Inside if )
						# update allocation table
						self.tbl_node_allocation[self.storage_node[destn_node_num].node_id][1] = self.tbl_node_allocation[self.storage_node[destn_node_num].node_id][1] - self.storage_node[destn_node_num].block_size # updating allocation table Tbl4
						fileblk = self.node_fileblk_map[self.storage_node[src_node_num].node_id][nodeblocks]
						print "\nfileblk : "+str(fileblk)
						#del(self.tbl_fileblk_nodeblk_map[self.node_fileblk_map[self.storage_node[src_node_num].node_id][nodeblocks]])
						#print "self.tbl_fileblk_nodeblk_map[fileblk] : "+str(self.tbl_fileblk_nodeblk_map)
						#update tbl2 fileblk-nodeblk map
						self.tbl_fileblk_nodeblk_map[fileblk] = [self.storage_node[destn_node_num].node_id,nodeblocks]
						#update node fileblk map
						self.node_fileblk_map[self.storage_node[destn_node_num].node_id].append(fileblk)
						#update file_stripe_map
						#print "self.storage_node[src_node_num].node_id,self.storage_node[destn_node_num].node_id,nodeblocks " +str(self.storage_node[src_node_num].node_id)+" ,"+str(self.storage_node[destn_node_num].node_id)+" ,"+str(nodeblocks)
						self.update_stripe_list_nodeid(self.storage_node[src_node_num].node_id,self.storage_node[destn_node_num].node_id,nodeblocks)
			status = "Done"
			self.node_fileblk_map[self.storage_node[src_node_num].node_id] = []
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
		if(self.list_free_nodes == {}):
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
			if(self.tbl_node_allocation[self.storage_node[nodes].node_id][2] != "Failed"):
				#print "\n self.tbl_node_allocation[self.storage_node[nodes].node_id] :" + str(self.tbl_node_allocation[self.storage_node[nodes].node_id])
				if(self.tbl_node_allocation[self.storage_node[nodes].node_id][1] != 0):
					node_free_size_map[self.storage_node[nodes].node_id] = [self.tbl_node_allocation[self.storage_node[nodes].node_id][1]]
		return node_free_size_map

	# return number of used blocks
	def give_number_of_used_blocks(self,node_id):
		number_used_blocks = 0
		for used_blocks in range(self.storage_node[self.nodeid_nodenumber_map[node_id]].max_blocks):
			if(self.storage_node[self.nodeid_nodenumber_map[node_id]].block_stat[used_blocks] == "Allocated"):
				number_used_blocks += 1
		return number_used_blocks

	# start recovery in adjust mode
	def start_recovery_adjust_mode(self, node_id):
		free_block_count = self.give_total_free_blocks_size()
		reqd_size = self.tbl_node_allocation[node_id][0]-self.tbl_node_allocation[node_id][1]
		reqd_no_blks = reqd_size/(self.storage_node[self.nodeid_nodenumber_map[node_id]].block_size)
		temp = 0
		available_space = 0
		nxtblk = []
		for space in free_block_count:
			available_space += free_block_count[space][0]
		#print "\n free_block_count : "+str(free_block_count) +" reqd_size : "+str(reqd_size)+" reqd_no_blks : "+str(reqd_no_blks)
		#print "\n Available Space : "+str(available_space)
		if(reqd_size > available_space):
			print "\nRecovery not Possible\n"
		else:
			count_bloks = 0 # used to track blks of the failed nodes getting copied.
			rqd_no_blks_actl = reqd_no_blks
			for nodes in free_block_count:
				data_blk = []
				#print "\n nodes : "+str(nodes) +"  self.nodeid_nodenumber_map[nodes] : "+str(self.nodeid_nodenumber_map[nodes])
				#print "\n free_block_count[nodes][0] "+str(free_block_count[nodes][0])
				temp = int(math.floor(free_block_count[nodes][0]/self.storage_node[self.nodeid_nodenumber_map[nodes]].block_size))
				#print "\n nodes : "+str(nodes)+" temp : "+ str(temp) + " reqd_no_blks : "+str(reqd_no_blks) + " min(temp, reqd_no_blks) : "+str(min(temp, reqd_no_blks))
				for blk in range(min(temp, reqd_no_blks)):
					nxtblk = self.storage_node[self.nodeid_nodenumber_map[nodes]].give_next_free_block()
					if(not nxtblk):
						break
					data_blk.append(nxtblk[1])
					data = self.storage_node[self.nodeid_nodenumber_map[node_id]].block_data[blk]
					#print "\n nxtblk : "+ str(nxtblk) + " data = "+ str(data) +" nxtblk[1] : "+str(nxtblk[1])
					if(self.storage_node[self.nodeid_nodenumber_map[nodes]].write_to_node_stripe(data,nxtblk[1]) == "Done"):
						#print "Data Copied :" + str(self.storage_node[self.nodeid_nodenumber_map[nodes]].block_data[nxtblk[1]])
						#print "\n free_block_count[nodes][0] "+str(free_block_count[nodes][0])
						self.update_stripe_list_nodeid(node_id,nodes,nxtblk[1])
						reqd_no_blks -= 1
						free_block_count[nodes][0] -= self.storage_node[self.nodeid_nodenumber_map[nodes]].block_size 
						self.tbl_node_allocation[nodes][1] = free_block_count[nodes][0]
						#print "\n ## free_block_count[nodes][0] "+str(free_block_count[nodes][0])
						if(reqd_no_blks == 0):
							break
				fileblk = self.node_fileblk_map[node_id]
				#print " \n fileblk :" +str(fileblk)+ " data_blk : "+str(data_blk)
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
					self.node_fileblk_map[node_id].remove(fileblk_list[i])
					self.node_fileblk_map[nodes].append(fileblk_list[i])
				#print "\n self.node_fileblk_map[node_id] : "+str(self.node_fileblk_map[node_id])
				if(reqd_no_blks == 0):
					break	
	# copy data in adjust mode
	#def copy_data_adjust_mode(self,src_node_id,destn_node_id,reqd_no_blks):
start_time = time.time()
print "Start time : "+str(start_time)
#h = hpy()
print "%%%%%%% = "+str(sys.getsizeof({}))
ma = Master(6, 4, 2, 2)
#print "\n#######\n"
#ma.write_file_blocks_list("A",['A1','A2','PA1','PA2'])
#print "Blocks of A : "+ str(ma.give_file_blocks_list("A"))
#ma.make_node_fail(1)
#ma.write_file_blocks_list("B",['B1','B2','PB1','PB2'])
ma.display_tables()
'''if __name__ == '__main__':	
	ma.write_file("A",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
if __name__ == '__main__':	
	ma.write_file("B",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
if __name__ == '__main__':	
	ma.write_file("C",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
#ma.make_node_fail(4)
#ma.display_tables()
#ma.display_nodes()
#print h.heap()
print "%%%%%%% = "+str(sys.getsizeof({}))
print("--- %s seconds ---" % (time.time() - start_time))
if __name__ == '__main__':	
	ma.write_file("D",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])

if __name__ == '__main__':	
	ma.write_file("E",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.display_tables()
#ma.display_nodes()
print("--- %s seconds ---" % (time.time() - start_time))
if __name__ == '__main__':	
	ma.write_file("E",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.display_tables()
#ma.display_nodes()
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("F",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("G",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("H",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
print "%%%%%%% = "+str(sys.getsizeof({}))
#ma.make_node_fail(2)
#ma.display_tables()
#ma.display_nodes()
ma.write_file("I",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("J",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.make_node_fail(4)
#ma.display_tables()
#ma.display_nodes()
ma.write_file("K",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.make_node_fail(6)
ma.write_file("L",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.make_node_fail(10)
#ma.make_node_fail(11)
#ma.display_tables()
#ma.display_nodes()
#ma.write_file("M",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("N",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("O",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("P",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("Q",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("R",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])																						
#ma.display_tables()																																			
#ma.display_nodes()
ma.write_file("S",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("T",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()																																																									
#ma.display_nodes()
ma.write_file("U",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
ma.write_file("V",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()																																																																																																																																																																																																																																																																																																												
ma.write_file("W",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("X",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.display_tables()
#ma.display_nodes()
print "%%%%%%% = "+str(sys.getsizeof({}))																																																																																																														
ma.write_file("Y",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("Z",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A1",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
print "%%%%%%% = "+str(sys.getsizeof({}))
ma.write_file("A2",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A3",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A4",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("A5",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A6",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A7",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A8",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A9",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A10",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("A11",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A12",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A13",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A14",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A15",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A16",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
print("--- %s seconds ---" % (time.time() - start_time))
ma.write_file("A17",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A18",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A19",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A20",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A21",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
ma.write_file("A22",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])'''
for count in range (4):
	filename = "B"+str(count)
	ma.write_file(filename,[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
	print "&&& "+str(count%1000)
	if(count%1000 == 0):
		print "Failed\n"
		ma.make_node_fail(count)

print("--- %s seconds ---" % (time.time() - start_time))
#ma.write_file("A5",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])
#ma.write_file("A6",[10,20,21,22,23,24,25,30,10,31,32,33,34,35,36])																													
#ma.display_tables()
#ma.display_nodes()
'''ma.display_tables()
ma.display_nodes()
ma.make_node_fail(2)
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(3)
ma.display_tables()
ma.display_nodes()'''
ma.make_node_fail(4)
ma.display_tables()
ma.display_nodes()
print "recovery rule : "+str(ma.give_recovery_rule())
ma.set_recovery_rule("Adjust")
print "New recovery rule : "+str(ma.give_recovery_rule())


		
		
