import rpyc
import uuid
import math
import random
import configparser
import signal
import sys
import json
from pprint import pprint
from rpyc.utils.server import ThreadedServer
from threading import Timer
import copy

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

def get_heartbeat(host, port):
    HEARTBEAT_INTERVAL = 5
    try:
        conn = rpyc.connect(host, port)
        # msg = conn.root.Chunks().get_heartbeat()
        # print(msg)
        conn.root.Chunks().get_heartbeat()
        # print("Heartbeat to chunkserver: {}, {} successful".format(host, port))
        # conn.close()

    except Exception as e:
        print("Heartbeat to chunkserver: {}, {} failed".format(host, port))

    heartbeat_timer = Timer(HEARTBEAT_INTERVAL, get_heartbeat, args=[host,port])
    heartbeat_timer.start()

def split_list(a_list, no_of_parts):
    length = len(a_list)
    if no_of_parts > length:
        raise ValueError("Number of primaries specified exceeds secondaries available!")
    return [a_list[i*length // no_of_parts: (i+1)*length // no_of_parts] 
             for i in range(no_of_parts) ]

class MasterService(rpyc.Service):
    class exposed_Master():
        print("start")
        file_table = {}
        # chunkServers = {}
        # chunkReplicas = {}
        allChunkServers = {} #e.g. {"1":("127.0.0.1","8888"), "2":("127.0.0.1","8887")}
        primary_secondary_table = {}    # primary_secondary_table[primary_id] = [secondary_id_1, secondary_id_2]
                                        # e.g. {"1":["2","4"], "5":["3","6"]}

        # Retrieve IP address information in GFS config for Chunkservers
        conf = configparser.ConfigParser()
        conf.read_file(open('GFS.conf'))
        block_size = int(conf.get('master', 'block_size'))
        num_primary = int(conf.get('master', 'num_primary'))
        allChunkServers_conf = conf.get('master', 'chunkServers').split(',')

        # Initialise primaryServers and seondaryServers through leasing
        # NOTE: primaryServers should be updated every time a new primary is chosen
        print("--------------------- Leasing to Primary Chunkservers ---------------------")
        for m in allChunkServers_conf:
            id, host, port = m.split(":")
            # print("set_conf in master:", str(id))
            allChunkServers[id] = (host, port)  # set up all chunkserver mappings

        primary_idxs = random.sample(list(allChunkServers.keys()), num_primary)
        secondary_chunkservers = copy.deepcopy(allChunkServers)
        for idx in primary_idxs:
            del secondary_chunkservers[idx]

        sec_chunkserver_grps = split_list(list(secondary_chunkservers.keys()), num_primary)
        for primary_idx, sec_chunkserver_grp in zip(primary_idxs, sec_chunkserver_grps):
            primary_secondary_table[primary_idx] = sec_chunkserver_grp
        print(primary_secondary_table)
        print("---- Leasing completed. Pri to Sec mapping: {} ----".format(primary_secondary_table))
        

        # # Check if NUMBER OF REPLICATIONS IS HIGHER THAN NUMBER OF CHUNKSERVERS
        # if num_replica > (len(chunkServers)+len(chunkReplicas))/len(chunkServers) :
        #     print("WARNING: NUMBER OF REPLICATIONS IS HIGHER THAN NUMBER OF CHUNKSERVERS")

        # Attempt to connect to a primary master server if it is running (NOT IMPLEMENTED FOR NOW)
        try:
            con = rpyc.connect("127.0.0.1", port=8100)
            print(" ----- Connected to Shadow Master ------")
            back_up_server = con.root.BackUpServer()
            file_table_backup = back_up_server.getFileTable()
            file_table = json.loads(file_table_backup)
            # con.close()
        except:
            print("\n -----Info: Shadow Master not found !!! ------- ")
            print(" -----Start the Shadow Master ------- \n \n ")

        for chunkServer_idx in allChunkServers:
            host, port = allChunkServers[chunkServer_idx]
            get_heartbeat(host, port)

######### MASTER FUNCTIONS #########
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

        def exposed_get_num_primary(self):
            return self.__class__.num_primary

        def exposed_get_primaryServers(self):
            primaryServers = {}
            for primary_id in list(self.__class__.primary_secondary_table.keys()):
                primaryServers[primary_id] = self.__class__.allChunkServers[primary_id]
            return primaryServers

        def exposed_get_secondaryServers(self, primary_id):
            secondaryServers = {}
            for secondary_id in self.__class__.primary_secondary_table[primary_id]:
                secondaryServers[secondary_id] = self.__class__.allChunkServers[secondary_id]
            return secondaryServers


        # def exposed_get_num_replica(self):
        #     return self.__class__.num_replica
        #
        # def exposed_get_chunkServers(self):
        #     #print("master get_chunkServers:", self.__class__.chunkServers)
        #     return self.__class__.chunkServers
        #
        # def exposed_get_chunkReplicas(self):
        #     #print("master get_chunkReplicas:", self.__class__.chunkReplicas)
        #     return self.__class__.chunkReplicas

        def num_filled_blocks(self, chunkserver_id):
            blocks_filled = 0
            for file in self.__class__.file_table:
                chunk_list = self.__class__.file_table[file]
                for chunk in chunk_list:
                    if chunk[1] == chunkserver_id:
                        blocks_filled += 1
            return blocks_filled
        def get_most_available_primary(self):
            primaryServers = self.exposed_get_primaryServers()
            blocks_filled_dict = {}
            for primary in primaryServers:
                blocks_filled_dict[primary] = self.num_filled_blocks(primary)

            v = list(blocks_filled_dict.values())
            k = list(blocks_filled_dict.keys())
            return k[v.index(max(v))]



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
<<<<<<< HEAD
                # nodes_id = random.choice(list(self.__class__.chunkServers.keys()))
                nodes_id = self.get_most_available_primary()
                replicas_ids = []
                for i in range(self.__class__.num_replica-1):
                    replicas_ids.append(str(nodes_id)+"."+str(i+1))
                blocks.append((block_uuid, nodes_id, replicas_ids))
=======
                primaryServers = {}
                for primary_id in self.__class__.primary_secondary_table:
                    primaryServers[primary_id] = self.__class__.allChunkServers
                primary_id = random.choice(list(primaryServers.keys()))
                print("primary_id:", primary_id)
                secondaryServers = {}
                for secondary_id in self.__class__.primary_secondary_table[primary_id]:
                    secondaryServers[secondary_id] = self.__class__.allChunkServers
                secondary_ids = list(secondaryServers.keys())
                # for i in range(self.__class__.num_replica-1):
                #     replicas_ids.append(str(nodes_id)+"."+str(i+1))
                blocks.append((block_uuid, primary_id, secondary_ids))
>>>>>>> c90fb022e016f5c2e0bde56fd0c18156cb4419cd

                # append block_id , Chunk_server_id, Chunk_server's replicas_ids, index_of_block
                self.__class__.file_table[dest].append((block_uuid, primary_id, secondary_ids, i))

            return blocks

        def alloc_blocks(self, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                block_uuid = str(block_uuid)
                # Master is randomly assigning Chunkservers to each block
<<<<<<< HEAD
                # nodes_id = random.choice(list(self.__class__.chunkServers.keys()))
                nodes_id = self.get_most_available_primary()
=======
                nodes_id = random.choice(list(self.exposed_get_primaryServers().keys())) #TODO
>>>>>>> c90fb022e016f5c2e0bde56fd0c18156cb4419cd
                blocks.append((block_uuid, nodes_id))

            return blocks

        def alloc_append(self, filename, num_append_blocks): # append blocks
            block_uuids = self.__class__.file_table[filename]
            append_block_uuids = self.alloc_blocks(num_append_blocks)
            block_uuids.extend(append_block_uuids)
            return append_block_uuids


if __name__ == "__main__":
    port = 2131
    # signal.signal(): Allows defining custom handlers to be executed when a signal is received
    # signal.SIGINT: allows keyboard interrupt
    signal.signal(signal.SIGINT, int_handler)

    t = ThreadedServer(MasterService, port=port)
    print("Master server running on port", t.port)
    t.start()
