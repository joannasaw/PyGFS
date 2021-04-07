import rpyc
import uuid
import math
import random
import configparser
import signal
import sys
import json

from rpyc.utils.server import ThreadedServer

# This function basically stores the state of the master and its mapping to a local file when interrupted

# when signal is received for keyboard cancel, this function runs to save 
# TODO: create a backup server to integrate with this
def int_handler(signal, frame):
    content = MasterService.exposed_Master.file_table
    try:
        con = rpyc.connect("127.0.0.1", port=8100)
        back_up_server = con.root.BackUpServer()
        file_table_string = json.dumps(content)
        back_up_server.updateFileTable(file_table_string)
        con.close()
    except:
        print("\n -----Info: Primary backup Server not found !!! ------- ")
        print(" ----- Master Server memory lost ------- \n ")

    sys.exit(0)


def set_conf():
    # Retrieve IP address information in GFS config for Chunkservers
    conf = configparser.ConfigParser()
    conf.read_file(open('GFS.conf'))
    MasterService.exposed_Master.block_size = int(conf.get('master', 'block_size'))
    MasterService.exposed_Master.num_replica = int(conf.get('master', 'num_replica'))
    chunkServers = conf.get('master', 'chunkServers').split(',')
    chunkReplicas = conf.get('master','chunkReplicas').split(',')
    # print(chunkServers)
    
    for m in chunkServers:
        id, host, port = m.split(":")
        # print("set_conf in master:", str(id))
        MasterService.exposed_Master.chunkServers[id] = (host, port)  # set up chunkserver mappings

    for m in chunkReplicas:
        id, host, port = m.split(":")
        # print("set_conf in master:", str(id))
        MasterService.exposed_Master.chunkReplicas[id] = (host, port)  # set up chunkserver's replicas mappings

    # Check if NUMBER OF REPLICATIONS IS HIGHER THAN NUMBER OF CHUNKSERVERS
    if MasterService.exposed_Master.num_replica > (len(MasterService.exposed_Master.chunkServers)+len(MasterService.exposed_Master.chunkReplicas))/len(MasterService.exposed_Master.chunkServers) :
        print("WARNING: NUMBER OF REPLICATIONS IS HIGHER THAN NUMBER OF CHUNKSERVERS")

    # Attempt to connect to a primary master server if it is running (NOT IMPLEMENTED FOR NOW)
    try:
        con = rpyc.connect("127.0.0.1", port=8100)
        print(" ----- Connected to Primary back-up Server ------")
        back_up_server = con.root.BackUpServer()
        file_table_backup = back_up_server.getFileTable()
        MasterService.exposed_Master.file_table = json.loads(file_table_backup)
        con.close()
    except:
        print("\n -----Info: Primary backup Server not found !!! ------- ")
        print(" -----Start the primary_backup_server ------- \n \n ")


class MasterService(rpyc.Service):
    class exposed_Master():
        file_table = {}
        chunkServers = {}
        chunkReplicas = {}
        
        
        block_size = 0
        num_replica = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self, dest, size):
            if self.exists(dest):
                pass

            self.__class__.file_table[dest] = [] # overwrites?

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_write(dest, num_blocks)
            # master returns block to client
            return blocks

        def exposed_write_append(self, dest, size):
            if self.exists(dest):
                pass

            # self.__class__.file_table[dest] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_append(dest, num_blocks)
            # master returns block to client
            return blocks

        def exposed_delete(self, fname):
            if not self.exists(fname):
                return False
            mapping_to_be_deleted = self.__class__.file_table.pop(fname, None)
            return mapping_to_be_deleted

        def exposed_get_file_table_entry(self, fname):
            # print("file_table in master:", self.__class__.file_table)
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            else:
                return None

        def exposed_get_list_of_files(self):
            return list(self.__class__.file_table.keys())

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_num_replica(self):
            return self.__class__.num_replica

        def exposed_get_chunkServers(self):
            #print("master get_chunkServers:", self.__class__.chunkServers)
            return self.__class__.chunkServers

        def exposed_get_chunkReplicas(self):
            #print("master get_chunkReplicas:", self.__class__.chunkReplicas)
            return self.__class__.chunkReplicas

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_write(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                block_uuid = str(block_uuid)
                # Master is randomly assigning Chunkservers to each block
                nodes_id = random.choice(list(self.__class__.chunkServers.keys()))
                replicas_ids = []
                for i in range(self.__class__.num_replica-1):
                    replicas_ids.append(nodes_id+"."+str(i+1)) 
                blocks.append((block_uuid, nodes_id, replicas_ids))

                # append block_id , Chunk_server_id, Chunk_server's replicas_ids, index_of_block
                self.__class__.file_table[dest].append((block_uuid, nodes_id, replicas_ids, i))

            return blocks

        def alloc_blocks(self, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                block_uuid = str(block_uuid)
                # Master is randomly assigning Chunkservers to each block
                nodes_id = random.choice(list(self.__class__.chunkServers.keys()))
                blocks.append((block_uuid, nodes_id))

            return blocks

        def alloc_append(self, filename, num_append_blocks): # append blocks
            block_uuids = self.__class__.file_table[filename]
            append_block_uuids = self.alloc_blocks(num_append_blocks)
            block_uuids.extend(append_block_uuids)
            return append_block_uuids


if __name__ == "__main__":
    port = 2131
    set_conf()
    # signal.signal(): Allows defining custom handlers to be executed when a signal is received
    # signal.SIGINT: allows keyboard interrupt
    signal.signal(signal.SIGINT, int_handler)
    print("Master server running on port", port)
    t = ThreadedServer(MasterService, port=port)
    t.start()
