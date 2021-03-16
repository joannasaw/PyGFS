import math
import uuid
import os
import time
import operator
import functools 
import rpyc


class GFSClient:
    def __init__(self, master):
        self.master = master

    def write(self, filename, data): # filename is full namespace path
        if self.exists(filename): # if already exists, overwrite
            self.delete(filename)
        num_chunks = self.num_chunks(len(data))
        print("number of chunks:", num_chunks)
        chunkuuids = self.master.alloc(filename, num_chunks)
        print("chunkuuids", chunkuuids)
        self.write_chunks(chunkuuids, data)
    
    def write_chunks(self, chunkuuids, data):
        chunks = [ data[x:x+self.master.chunksize] \
            for x in range(0, len(data), self.master.chunksize) ]
        chunkservers = self.master.get_chunkservers()
        print("chunkservers are", chunkservers)
        for i in range(0, len(chunkuuids)): # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

    def num_chunks(self, size):
        return (size // self.master.chunksize) \
            + (1 if size % self.master.chunksize > 0 else 0)

    def write_append(self, filename, data):
        if not self.exists(filename):
            raise Exception("append error, file does not exist: " \
                + filename)
        num_append_chunks = self.num_chunks(len(data))
        append_chunkuuids = self.master.alloc_append(filename, \
            num_append_chunks)
        self.write_chunks(append_chunkuuids, data)

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename): # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: " \
                + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkservers = self.master.get_chunkservers()
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)
        data = functools.reduce(lambda x, y: x + y, chunks) # reassemble in order
        return data

    def delete(self, filename):
        self.master.delete(filename) 


def main():

    # connect to master
    try:
        c = rpyc.connect("localhost", port=8888)
        master = c.root
        master.echo("hello ms from client")
    except:
        print("Master server not found")



    # master.echo("hello from client")
    client = GFSClient(master)

    while True:
        try:
            request = input("TYPE 'write', 'read', 'append' or 'delete': (w/r/a/d) ")

            if request == "write" or request == "w":
                file_name = input("FILE NAME: ")
                content = input("WRITE: ")
                client.write(file_name, content)

            elif request == "read" or request == "r":
                file_name = input("FILE NAME: ")
                client.read(file_name)

            elif request == "append" or request == "a":
                file_name = input("FILE NAME: ")
                content = input("APPEND: ")
                client.write_append(file_name, content)

            elif request == "delete" or request == "d":
                file_name = input("FILE NAME: ")
                client.delete(file_name)
            else:
                print("Invalid action entered! Try again.")
                break
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()
    