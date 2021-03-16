import math
import uuid
import os
import time
import operator
import rpyc
from chunkserver import GFSChunkserver
from rpyc.utils.server import ThreadedServer
from rpyc.lib import setup_logger

# def int_handler(signal, frame):
#   pickle.dump((MasterService.exposed_Master.file_table,MasterService.exposed_Master.block_mapping),open('fs.img','wb'))
#   sys.exit(0)


# Wrap entire class in Rpyc class
class GFSMaster(rpyc.Service):
    
    
    def __init__(self):
        self.num_chunkservers = 4 
        self.max_chunkservers = 10 # NEVER USED
        self.max_chunksperfile = 100 # NEVER USED
        self.chunksize = 10
        self.chunkrobin = 0
        self.filetable = {} # file to chunk mapping
        self.chunktable = {} # chunkuuid to chunkloc mapping
        self.chunkservers = {} # loc id to chunkserver mapping
        # self.init_chunkservers() # NO LONGER USED

    def exposed_echo(self, text):
        print(text)

    def init_chunkservers(self): 
        for i in range(0, self.num_chunkservers):
            # i here stands for chunkloc
            chunkserver = GFSChunkserver(i)
            self.chunkservers[i] = chunkserver

    def exposed_get_chunkservers(self):
        return self.chunkservers

    # Allocates memory from chunkservers to the new file
    def alloc(self, filename, num_chunks): # return ordered chunkuuid list
        chunkuuids = self.alloc_chunks(num_chunks) # the chunkuuids that belong to the file
        self.filetable[filename] = chunkuuids # add the mapping of the filename:chunkuuids
        return chunkuuids 

    def alloc_chunks(self, num_chunks):
        chunkuuids = []
        for i in range(0, num_chunks):
            chunkuuid = uuid.uuid1() # just creates a random UUID
            chunkloc = self.chunkrobin # chunkrobin is the current index of the chunkserver with empty chunks 
            self.chunktable[chunkuuid] = chunkloc # update chunktable of the new index for specific chunk
            chunkuuids.append(chunkuuid) 
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers # update chunkrobin
        return chunkuuids

    def alloc_append(self, filename, num_append_chunks): # append chunks
        chunkuuids = self.filetable[filename]
        append_chunkuuids = self.alloc_chunks(num_append_chunks)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def exposed_get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def exposed_get_chunkuuids(self, filename):
        return self.filetable[filename]

    def exposed_exists(self, filename):
        return True if filename in self.filetable else False

    def exposed_delete(self, filename): # rename for later garbage collection
        chunkuuids = self.filetable[filename] # find the chunks that belong to the filename
        del self.filetable[filename] 
        timestamp = repr(time.time())
        
        # rename the file and store it back into the filetable as another name that can be used for recovery
        deleted_filename = "/hidden/deleted/" + timestamp + filename
        self.filetable[deleted_filename] = chunkuuids
        print("deleted file: " + filename + " renamed to " + \
            deleted_filename + " ready for gc")

    def dump_metadata(self):
        print("Filetable:"),
        for filename, chunkuuids in self.filetable.items():
            print (filename, "with", len(chunkuuids),"chunks")
        print("Chunkservers: ", len(self.chunkservers))
        print("Chunkserver Data:")
        for chunkuuid, chunkloc in sorted(self.chunktable.items(), key=operator.itemgetter(1)):
            chunk = self.chunkservers[chunkloc].read(chunkuuid)
            print(chunkloc, chunkuuid, chunk)

if __name__ == "__main__":
    host = "127.0.0.1"
    port = input("Enter the server port [Default = 18812]:")   # Allows user to enter a port number for Server
    if port:
         port = int(port)
    else:
        port = 18812                            # Sets default value as 18812 if no port number is specified by user 
    t = ThreadedServer(GFSMaster, hostname=host, port=port,protocol_config={'allow_public_attrs': True})
    """ 'allow_public_attrs' creates the Threaded RPyc server to allow the attributes to be accessible to users 
                                                   i.e. allow normal actions on dict, dataframes """
    setup_logger(quiet=False, logfile=None)   
    print("Master service started...")  # Start logging online
    t.start()       