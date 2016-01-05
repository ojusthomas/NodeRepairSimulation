# define the storage node.
class StorageNode:
	
	#Constructor takes maximum number of blocks in  the node, size of the block, nodeid, type of node - DN(DataNode), PN(ParityNode)
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
	def write_to_node_stripe(self, data, block_no):
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
		if(self.node_stat == "Live"):
			self.block_data[self.block_count] = data
			#self.block_count += self.block_count
			#return "Live"
		else:
			print "\nCannot write Node Failed\n"
			#return "Failed"
			
			
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
		print "\n ^^^^&&&^--__ Node"+str(self.node_stat)
		if(self.node_stat == "Live"):
			self.node_stat = "Failed"
			print "\n ^^^^&&&^^^^ Node "+str(self.node_stat)
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
	def give_next_free_block(self):
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
	recovery_rule = "Replace" # "Replace" - replace with a new node, "Adjust" - adjust with existing nodes
	list_free_nodes = {} # list of free nodes i.e having the node_availability_status = "Free"
	nodeid_nodenumber_map = {} # Keep track of node
	node_fileblk_map = {} # keep track of file blocks in each node

	#constructor
	def __init__(self, max_nodes, datanodes, paritynodes, buffernodes): # max_nodes = datanodes+paritynodes, buffernodes are for failure handling.
		self.max_nodes  = max_nodes
		self.datanodes = datanodes
		self.paritynodes = paritynodes
		self.buffernodes = buffernodes
		self.totalnodes = datanodes+paritynodes+buffernodes
		lists = []
		for nodes in range(self.datanodes):
			print "**@@ Nodes :"+str(nodes)+" datanodes = "+str(datanodes)+" datanodes+paritynodes = "+str(datanodes+paritynodes)
			#if (nodes < datanodes):
			self.storage_node.append(StorageNode(10, 10, "DN"+str(nodes),"DataNode"))# creating data nodes
			print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = [] # intializing allocation table Tbl4
			lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = lists #populating allocation table Tbl4
			self.list_free_nodes[self.storage_node[nodes].node_id] = [] # intializing free node list e.g [DN0,'Free']
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status) # populating free node lsit
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes # creating nodeid nodenumber map
			self.node_fileblk_map[self.storage_node[nodes].node_id] = [] # initializing node fileblock map e.g {DN0,['A1','B1']}
			print "**Nodes :"+str(nodes)+" datanodes = "+str(datanodes)+" datanodes+paritynodes = "+str(datanodes+paritynodes)
        	#if((nodes >= datanodes) or (nodes < datanodes+paritynodes)):  
		for nodes in range(datanodes,datanodes+paritynodes):
			print"&&&&&&"
			self.storage_node.append(StorageNode(10, 10, "PN"+str((datanodes+paritynodes-1)-nodes),"ParityNode"))
			print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = []
			lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = lists
			self.list_free_nodes[self.storage_node[nodes].node_id] = []
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
			self.node_fileblk_map[self.storage_node[nodes].node_id] = []
		for nodes in range(datanodes+paritynodes,datanodes+paritynodes+buffernodes):
			print"&&&&&&"
			self.storage_node.append(StorageNode(10, 10, "BN"+str((datanodes+paritynodes+buffernodes-1)-nodes),"BufferNode"))
			print "*Nodes :"+str(nodes)+" node_id " + str(self.storage_node[nodes].node_id)
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = []
			lists = [self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, self.storage_node[nodes].max_blocks*self.storage_node[nodes].block_size, "Live"]
			self.tbl_node_allocation[self.storage_node[nodes].node_id] = lists
			self.list_free_nodes[self.storage_node[nodes].node_id] = []
			self.list_free_nodes[self.storage_node[nodes].node_id].append(self.storage_node[nodes].node_availability_status)
			self.nodeid_nodenumber_map[self.storage_node[nodes].node_id] = nodes
			self.node_fileblk_map[self.storage_node[nodes].node_id] = []
	# node registration with master
	def write_file(self, file_name, file_data=[]):  
		blk_list = []
		list_blk_list = []
		data_count = 0
		status = "No Duplicates"
		if(self.add_to_file_list(file_name) == "Duplicate"):
			status = "Duplicate File Exist"
		else:
			self.tbl_file_blocks_list = self.split_file(file_name)
			#print "LLL"+ str(self.tbl_file_blocks_list)
			self.tbl_file_blocks_map[file_name] = self.tbl_file_blocks_list # append the file blocks list (Tbl1)
			#print "$$$$"
			#print self.tbl_file_blocks_map[file_name]
			for nodes in range(self.max_nodes): # trying to write the data to the node blocks.
				#for blocks in range(storage_node[nodes].max_nodes)
					#print nodes
					#print "DataCount : "+str(data_count)
					if(self.storage_node[nodes].node_availability_status == "Free"): # rChanging the availability status of the node from Free to Used on first use
						self.storage_node[nodes].node_availability_status == "Used"
						del(self.list_free_nodes[self.storage_node[nodes].node_id])
					blk_list = self.storage_node[nodes].give_next_free_block()
					#print "blk_list"+str(blk_list)
					if(self.storage_node[nodes].write_to_node_stripe(file_data[data_count],blk_list[1]) == "Done"):# writing data to node block (Inside if )
						self.tbl_node_allocation[self.storage_node[nodes].node_id][1] = self.tbl_node_allocation[self.storage_node[nodes].node_id][1] - self.storage_node[nodes].block_size # updating allocation table Tbl4
						self.node_fileblk_map[self.storage_node[nodes].node_id].append(self.tbl_file_blocks_list[nodes])
					#print "\n###$ "+str(self.tbl_file_blocks_list[data_count])
					self.tbl_fileblk_nodeblk_map[self.tbl_file_blocks_list[data_count]] = blk_list # populating Tbl2
					list_blk_list.append(blk_list)
					#print "list_blk_list"+str(list_blk_list)
					data_count += 1
			self.tbl_file_stripe_map[file_name] = list_blk_list
		return status

	# display all blocks of all nodes
	def display_nodes(self):
		for nodes in range(self.max_nodes+self.buffernodes):
			print "\n***** Node "+str(nodes)+ " "+str(self.storage_node[nodes].node_type)+" *****\n"
			self.storage_node[nodes].list_all_blocks()

	#split file into blocks given the file name
	def split_file(self, filename):
		file_blks = []
		for nodes in range(self.max_nodes-self.paritynodes):
			file_blks.append(filename+str(nodes))
		for nodes in range(self.datanodes,self.max_nodes):
			file_blks.append("P"+filename+str(nodes))
		#print file_blks
		return file_blks
	
	# check for duplicate files and add to file list
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
		self.tbl_file_blocks_list = self.tbl_file_blocks_map[filename]
		return self.tbl_file_blocks_list
	
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
		print "\nInside write_stripe_list Filename :"+str(filename) +" stripe_list : "+str(stripe_list)
		print "\nself.tbl_file_stripe_map[filename].append(stripe_list) :"+str(self.tbl_file_stripe_map[filename])
		self.tbl_file_stripe_map[filename] = stripe_list
		print "\n Moving out of write_stripe_list Filename :"+str(filename) +" stripe_list : "+str(stripe_list)

	# update node id with a new node id
	def update_stripe_list_nodeid(self,old_node_id,new_node_id,blknum):
		nodelist = ""
		newlist = ""
		flag = "false"
		print "\nold_node_id :"+str(old_node_id)
		print "\nnew_node_id :"+str(new_node_id)
		print "\nblknum : "+str(blknum)
		for files in self.file_list:
			print "File : "+str(files)
			nodelist = self.tbl_file_stripe_map[files]
			print "\nnodelist :"+str(nodelist)
			for nodeslists in nodelist:
				print "\n@@@@ nodeslist[0] :"+str(nodeslists[0]) + " old_node_id :"+str(old_node_id)
				if(nodeslists[0] == old_node_id):
					flag = "true"
					print "\n&&&&&&&&&&&&&&"
					nodeslists[0] = new_node_id
					nodeslists[1] = blknum
					print "\n### nodeslist :"+str(nodeslists) + " blkno :"+str(blknum)
					
			#newlist.append(nodeslists)
			print "\nOut of For nodelist : "+str(nodelist)
			if(flag == "true"):
				print "\n ***************** Break ******************"
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
		if(node >= 0 and node < self.max_nodes):
			print "###"+str(self.tbl_node_allocation[self.storage_node[node].node_id][2])
			if(self.storage_node[node].make_node_fail()=="Failed"):
				self.tbl_node_allocation[self.storage_node[node].node_id][2] = "Failed"
				print "###22222222222 : "+str(self.tbl_node_allocation[self.storage_node[node].node_id][2])
				self.start_recovery_replace_mode(self.storage_node[node].node_id)
	
	# make a specific node live
	def make_node_live(self, node):
		if(node >= 0 and node < self.max_nodes):
			if(self.storage_node[node].make_node_live() == "Live"):
				self.tbl_node_allocation[self.storage_node[node].node_id][2] = "Live"
	
	#do the recovery operation on node failure reporting.
	def start_recovery_replace_mode(self,node_id):
		print "BufferNodes : "+str(self.buffernodes)
		if(self.buffernodes <= 0):
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
		 self.buffernodes -= 1
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
				if(self.storage_node[src_node_num].block_stat[nodeblocks] == "Free"):
					break
				data = self.storage_node[src_node_num].block_data[nodeblocks]
				if(self.storage_node[destn_node_num].write_to_node_stripe(data,nodeblocks) == "Done"):# writing data to node block (Inside if )
						# update allocation table
						self.tbl_node_allocation[self.storage_node[destn_node_num].node_id][1] = self.tbl_node_allocation[self.storage_node[destn_node_num].node_id][1] - self.storage_node[destn_node_num].block_size # updating allocation table Tbl4
						#print "map "+str(self.node_fileblk_map[self.storage_node[src_node_num].node_id])
						fileblk = self.node_fileblk_map[self.storage_node[src_node_num].node_id][nodeblocks]
						#print "\nfileblk : "+str(fileblk)
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


	
			

ma = Master(9, 6, 3, 3)
print "\n#######\n"
#ma.write_file_blocks_list("A",['A1','A2','PA1','PA2'])
#print "Blocks of A : "+ str(ma.give_file_blocks_list("A"))
#ma.make_node_fail(1)
#ma.write_file_blocks_list("B",['B1','B2','PB1','PB2'])
ma.display_tables()
ma.write_file("A",[10,20,21,22,23,24,25,30,10])
ma.display_tables()
ma.write_file("B",[10,20,21,22,23,24,25,30,10])
ma.display_tables()
ma.write_file("C",[10,20,21,22,23,24,25,30,10])
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(1)
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(2)
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(3)
ma.display_tables()
ma.display_nodes()
ma.make_node_fail(4)
ma.display_tables()
ma.display_nodes()


		
		
