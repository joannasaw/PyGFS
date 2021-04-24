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
    file_table = MasterService.exposed_Master.file_table
    allChunkServers = MasterService.exposed_Master.allChunkServers
    primary_secondary_table = MasterService.exposed_Master.primary_secondary_table
    try:
        con = rpyc.connect("127.0.0.1", port=8100)
        back_up_server = con.root.BackUpServer()

        file_table_string = json.dumps(file_table)
        back_up_server.updateFileTable(file_table_string)
        allChunkServers_string = json.dumps(allChunkServers)
        back_up_server.updateAllChunkServers(allChunkServers_string)
        primary_secondary_table_string = json.dumps(primary_secondary_table)
        back_up_server.updatePrimarySecondaryTable(primary_secondary_table_string)

        con.close()
    except:
        print("\n -----Info: Primary backup Server not found !!! ------- ")
        print(" ----- Master Server memory lost ------- \n ")

    sys.exit(0)

def get_heartbeat(chunkServerStatus, chunkServer_idx, host, port):
    HEARTBEAT_INTERVAL = 5
    status = []
    try:
        conn = rpyc.connect(host, port)
        # msg = conn.root.Chunks().get_heartbeat()
        # print(msg)
        conn.root.Chunks().get_heartbeat()

        # print("Heartbeat to chunkserver: {}, {} successful".format(host, port))
        # conn.close()
        status = "alive"
    except Exception as e:
        status = "dead"
        print("Heartbeat to chunkserver {}: {}, {} failed".format(chunkServer_idx, host, port))

    chunkServerStatus[chunkServer_idx] = status
    # print(chunkServerStatus)
    heartbeat_timer = Timer(HEARTBEAT_INTERVAL, get_heartbeat, args=[chunkServerStatus, chunkServer_idx, host, port])
    heartbeat_timer.start()
    return status

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

        allChunkServers = {} #e.g. {"1":("127.0.0.1","8888"), "2":("127.0.0.1","8887")}
        chunkServerStatus ={}
        primary_secondary_table = {}    # primary_secondary_table[primary_id] = [secondary_id_1, secondary_id_2]
                                        # e.g. {"1":["2","4"], "5":["3","6"]}

        # Attempt to connect to a primary master server if it is running
        try:
            con = rpyc.connect("127.0.0.1", port=8100)
            print(" ----- Connected to Shadow Master ------")
            back_up_server = con.root.BackUpServer()
            file_table_backup = back_up_server.getFileTable()
            file_table = json.loads(file_table_backup)
            allChunkServers_backup = back_up_server.getAllChunkServers()
            allChunkServers = json.loads(allChunkServers_backup)
            primary_secondary_table_backup = back_up_server.getPrimarySecondary()
            primary_secondary_table = json.loads(primary_secondary_table_backup)
            # con.close()
        except:
            print("\n -----Info: Shadow Master not found !!! ------- ")
            print(" -----Start the Shadow Master ------- \n \n ")


        # Retrieve IP address information in GFS config for Chunkservers
        conf = configparser.ConfigParser()
        conf.read_file(open('GFS.conf'))
        block_size = int(conf.get('master', 'block_size'))

        num_primary = int(conf.get('master', 'num_primary'))
        allChunkServers_conf = conf.get('master', 'chunkServers').split(',')

        for m in allChunkServers_conf:
            id, host, port = m.split(":")
            # print("set_conf in master:", str(id))
            allChunkServers[id] = (host, port)  # set up all chunkserver mappings

        for chunkServer_idx in allChunkServers:
            host, port = allChunkServers[chunkServer_idx]
            get_heartbeat(chunkServerStatus, chunkServer_idx, host, port)

        def lease(self):
            print("--------------------- Leasing to Primary Chunkservers ---------------------")
            alive_chunkservers = [idx for idx in self.__class__.chunkServerStatus if self.__class__.chunkServerStatus[idx] == 'alive']
            # primary_idxs = random.sample(list(allChunkServers.keys()), num_primary)
            primary_idxs = random.sample(alive_chunkservers, self.__class__.num_primary)
            secondary_chunkservers = copy.deepcopy(self.__class__.allChunkServers)

            for idx in primary_idxs:
                del secondary_chunkservers[idx]
            dead_idx = list(set(secondary_chunkservers.keys()) - set(alive_chunkservers))
            for idx in dead_idx:
                if idx in secondary_chunkservers.keys():
                    del secondary_chunkservers[idx]

            sec_chunkserver_grps = split_list(list(secondary_chunkservers.keys()), self.__class__.num_primary)
            self.__class__.primary_secondary_table = {}
            for primary_idx, sec_chunkserver_grp in zip(primary_idxs, sec_chunkserver_grps):
                self.__class__.primary_secondary_table[primary_idx] = sec_chunkserver_grp
            print("---- Leasing completed. Pri to Sec mapping: {} ----".format(self.__class__.primary_secondary_table))
            pass


######### MASTER FUNCTIONS #########
        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self, dest, size):

            self.lease()

            if self.exists(dest):
                pass

            self.__class__.file_table[dest] = []  # will append to this empty list later

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            # master returns block to client
            return blocks

        def exposed_write_append(self, dest, size):
            self.lease()

            if self.exists(dest):
                pass

            # self.__class__.file_table[dest] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            # master returns block to client
            return blocks

        def exposed_delete(self, fname):
            if not self.exists(fname):
                return False
            mapping_to_be_deleted = self.__class__.file_table.pop(fname, None)
            return mapping_to_be_deleted

        def exposed_get_file_table_entry(self, fname):
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

        def exposed_get_chunkServers(self):
            return self.__class__.allChunkServers
        
        def find_primary_with_least(self):
            primary_secondary_table = self.__class__.primary_secondary_table
            output = ""
            count = sys.maxsize
            for primary in primary_secondary_table:
                num_sec = len(primary_secondary_table[primary])
                if num_sec < count:
                    output = primary
                    count = num_sec
            return output


        def update_primary(self, old_primary, new_primary, new_secondarys):
            # Update primary_secondary_table
            primary_secondary_table = self.__class__.primary_secondary_table
            try:
                primary_secondary_table.pop(old_primary)
            except Exception as e:
                print(e)
                print("Old primary is not an actual primary")
                print(primary_secondary_table)
                return
            primary_secondary_table[new_primary] = new_secondarys
            # Update file_table
            file_table = self.__class__.file_table
            for file in file_table:
                chunk_list = file_table[file]
                for i in range(len(chunk_list)):
                    old_p = chunk_list[i][1]
                    if old_p == old_primary:
                        file_table[file][i][1] = new_primary
                        file_table[file][i][2] = new_secondarys
            self.__class__.file_table = file_table


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

            return k[v.index(min(v))]



        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                block_uuid = str(block_uuid)
                primary_id = self.get_most_available_primary()
                secondaryServers = self.exposed_get_secondaryServers(primary_id)
                secondary_ids = list(secondaryServers.keys())
                blocks.append((block_uuid, primary_id, secondary_ids))
                self.__class__.file_table[dest].append((block_uuid, primary_id, secondary_ids, i))

            return blocks

if __name__ == "__main__":
    port = 2131
    # signal.signal(): Allows defining custom handlers to be executed when a signal is received
    # signal.SIGINT: allows keyboard interrupt
    signal.signal(signal.SIGINT, int_handler)

    t = ThreadedServer(MasterService, port=port)
    print("Master server running on port", t.port)
    t.start()
