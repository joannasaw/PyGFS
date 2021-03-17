import rpyc
import os
from pathlib import Path

from rpyc.utils.server import ThreadedServer

debug_Mode = False

# DATA_DIR = os.path.expanduser("~")
# DATA_DIR += "/gfs_root/"

dir_path = os.path.dirname(os.path.realpath(__file__))
BASE_PATH = str(Path(dir_path).parents[0])
DATA_DIR = os.path.sep.join([BASE_PATH, "gfs_root"])

class ChunkServerService(rpyc.Service):
    class exposed_Chunks():
        blocks = {}

        def exposed_put(self, block_uuid, data, chunkServers):
            with open(os.path.sep.join([DATA_DIR, str(block_uuid)]), 'w') as f:
                f.write(data)
                print("WRITING:", data)
            if len(chunkServers) > 0:
                self.forward(block_uuid, data, chunkServers)

        def exposed_get(self, block_uuid):
            block_addr = os.path.sep.join([DATA_DIR, str(block_uuid)])
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def forward(self,block_uuid,data,chunkServers):
            if debug_Mode:
                print("-: forwarding to:")
                print(block_uuid, chunkServers)
            chunkServer = chunkServers[0]
            chunkServers = chunkServers[1:]
            host, port = chunkServer

            con = rpyc.connect(host, port=port)
            chunkServer = con.root.Chunks()
            chunkServer.put(block_uuid,data,chunkServers)

        def exposed_delete_block(self,block_uuid):
            block_addr = os.path.sep.join([DATA_DIR, str(block_uuid)])
            if debug_Mode:
                print("deleting")
                print(block_addr)
            if not os.path.isfile(block_addr):
                return None
            os.remove(block_addr)
            return True


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    port = input("Enter the server port [Default = 8888]:")
    if port:
        port = int(port)
    else:
        port = 8888

    t = ThreadedServer(ChunkServerService, port=port)
    print("Starting chunkserver service on port", port)
    t.start()
