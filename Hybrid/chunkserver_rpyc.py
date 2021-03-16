import math
import uuid
import os
import time
import operator
import rpyc
from rpyc.utils.server import ThreadedServer
DATA_DIR = "./data"

class GFSChunkserver(rpyc.Service):

    def __init__(self, chunkloc):
        self.chunkloc = chunkloc
        self.chunktable = {}
        self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(chunkloc)
        # Create the directory if there isn't one
        if not os.access(self.local_filesystem_root, os.W_OK):
            os.makedirs(self.local_filesystem_root)

    def exposed_echo(self, text):
        print(text)

    def exposed_write(self, chunkuuid, chunk):
        local_filename = self.chunk_filename(chunkuuid)
        print("local file name is:", local_filename)
        with open(local_filename, "w") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename

    def exposed_read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "r") as f:
            data = f.read()
        return data

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + "/" \
            + str(chunkuuid) + '.gfs'
        return local_filename

if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    port = input("Enter the server port [Default = 3435]:")   # Allows user to enter a port number for Server
    if port:
         port = int(port)
    else:
        port = 3435       
    t = ThreadedServer(GFSChunkserver, port=port)
    print("Chunk service starting...")
    t.start()