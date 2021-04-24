import rpyc
import os
import json
import sys
import signal

from rpyc.utils.server import ThreadedServer

file_name = "backup_json"

def int_handler(signal, frame):
    file_table = PrimaryBackUpService.exposed_BackUpServer.file_table

    # Putting the file_table on disk
    backup_json = open(file_name, 'w')
    json.dump(file_table, backup_json, ensure_ascii=False)

    sys.exit(0)

def read(fname):
    mapping = PrimaryBackUpService.exposed_BackUpServer.file_table[fname]
    return mapping

def loadFromFile():
    if os.path.isfile(file_name):
        print("local backup file found")
        PrimaryBackUpService.exposed_BackUpServer.file_table = json.load(open(file_name))

class PrimaryBackUpService(rpyc.Service):

    class exposed_BackUpServer():
        file_table = {}
        allChunkServers = {} #e.g. {"1":("127.0.0.1","8888"), "2":("127.0.0.1","8887")}
        primary_secondary_table = {}    # primary_secondary_table[primary_id] = [secondary_id_1, secondary_id_2]
                                        # e.g. {"1":["2","4"], "5":["3","6"]}


        def exposed_getFileTable(self):
            file_table_string = json.dumps(self.file_table)
            print("File table requested by MasterServer")
            return file_table_string
        def exposed_getAllChunkServers(self):
            allChunkServers_string = json.dumps(self.allChunkServers)
            print("allChunkServers requested by MasterServer")
            return allChunkServers_string
        def exposed_getPrimarySecondary(self):
            primary_secondary_table_string = json.dumps(self.primary_secondary_table)
            print("primary_secondary_table requested by MasterServer")
            return primary_secondary_table_string

        def exposed_updateFileTable(self, __file_table__):
            self.__class__.file_table = json.loads(__file_table__)
            print("File table is updated")
        def exposed_updateAllChunkServers(self, __allChunkServers__):
            self.__class__.allChunkServers = json.loads(__allChunkServers__)
            print("allChunkServers is updated")
        def exposed_updatePrimarySecondaryTable(self, __primary_secondary_table__):
            self.__class__.primary_secondary_table = json.loads(__primary_secondary_table__)
            print("primary_secondary_table is updated")

        ## master functions

        def exposed_get_file_table_entry(self, fname):
            # print("file_table in master:", self.__class__.file_table)
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            else:
                return None

        def exposed_get_list_of_files(self):
            return list(self.__class__.file_table.keys())

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


if __name__ == "__main__":
    port = 8100 # master is told to connect to this number in master_b.py
    loadFromFile()
    signal.signal(signal.SIGINT, int_handler)
    print("Shadow Master is running on port", port)
    t = ThreadedServer(PrimaryBackUpService, port=port)
    t.start()
