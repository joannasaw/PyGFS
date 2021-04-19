import rpyc
import os
from pathlib import Path

from rpyc.utils.server import ThreadedServer

debug_Mode = True

# DATA_DIR = os.path.expanduser("~")
# DATA_DIR += "/gfs_root/"

dir_path = os.path.dirname(os.path.realpath(__file__))
BASE_PATH = str(Path(dir_path).parents[0])
# DATA_DIR = os.path.sep.join([BASE_PATH, "gfs_root"])

class ChunkServerService(rpyc.Service):
    class exposed_Chunks():
        blocks = {}

        def exposed_get_heartbeat(self): # this is an exposed method
            return "I'm ok"

        def exposed_put(self, block_uuid, data, secondaryServers):
            with open(os.path.sep.join([DATA_DIR, str(block_uuid)]), 'w') as f:
                f.write(data)
                print("WRITING:", data)
            if len(secondaryServers) > 0:
                self.forward(block_uuid, data, secondaryServers)

        def exposed_replicate(self, block_uuid, data):
            with open(os.path.sep.join([DATA_DIR, str(block_uuid)]), 'w') as f:
                f.write(data)
                print("WRITING:", data)


        def exposed_get(self, block_uuid):
            block_addr = os.path.sep.join([DATA_DIR, str(block_uuid)])
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def forward(self,block_uuid,data,secondaryServers):
            if debug_Mode:
                print("-: forwarding to:")
                print(block_uuid, secondaryServers)
            #chunkServer = chunkServers[0]
            #chunkServers = chunkServers[1:]
            for i in secondaryServers:
                host, port = i
                con = rpyc.connect(host, port=port)
                secondary = con.root.Chunks()
                secondary.replicate(block_uuid,data)

        def exposed_delete_block(self, block_uuid, secondaryServers):
            block_addr = os.path.sep.join([DATA_DIR, str(block_uuid)])
            if debug_Mode:
                print("deleting")
                print(block_addr)
            self.forward_delete(block_uuid,secondaryServers)
            if not os.path.isfile(block_addr):
                return None
            os.remove(block_addr)
            return True

        def forward_delete(self,block_uuid,secondaryServers):
            if debug_Mode:
                print("-: forwarding delete to:")
                print(block_uuid, secondaryServers)
            #chunkServer = chunkServers[0]
            #chunkServers = chunkServers[1:]
            #print(chunkReplicas)
            for i in secondaryServers:
                host, port = i
                con = rpyc.connect(host, port=port)
                chunkServer = con.root.Chunks()
                chunkServer.delete_for_replica(block_uuid)

        def exposed_delete_for_replica(self, block_uuid):
            block_addr = os.path.sep.join([DATA_DIR, str(block_uuid)])
            if debug_Mode:
                print("deleting")
                print(block_addr)
            if not os.path.isfile(block_addr):
                print("file to delete not in chunk")
            os.remove(block_addr)


if __name__ == "__main__":
    port = input("Enter the server port [Default = 8888]:")
    if port:
        port = int(port)
    else:
        port = 8888

    DATA_DIR = os.path.sep.join([BASE_PATH, "gfs_root", str(port)])
    # print(DATA_DIR)
    # print(os.path.exists(DATA_DIR))
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    t = ThreadedServer(ChunkServerService, port=port)
    print("Starting chunkserver service on port", port)
    t.start()
