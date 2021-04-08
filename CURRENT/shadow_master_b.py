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


def loadFromFile():
    if os.path.isfile(file_name):
        print("local backup file found")
        PrimaryBackUpService.exposed_BackUpServer.file_table = json.load(open(file_name))

class PrimaryBackUpService(rpyc.Service):

    class exposed_BackUpServer():
        file_table = {}

        def exposed_getFileTable(self):
            file_table_string = json.dumps(self.file_table)
            print("File table requested by MasterServer")
            return file_table_string

        def exposed_updateFileTable(self, __file_table__):
            self.__class__.file_table = json.loads(__file_table__)
            print("File table is updated")


if __name__ == "__main__":
    port = 8100 # master is told to connect to this number in master_b.py
    loadFromFile()
    signal.signal(signal.SIGINT, int_handler)
    print("Primary BackUp Server is running on port", port)
    t = ThreadedServer(PrimaryBackUpService, port=port)
    t.start()
