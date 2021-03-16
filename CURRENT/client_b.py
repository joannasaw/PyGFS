import rpyc
import sys
import os

debug_Mode = False


def send_to_minion(block_uuid,data,minions):
    if debug_Mode:
        print("sending: " + str(block_uuid) + str(minions))
        print("before minions:", minions)
    minion = minions[0]
    minions = minions[1:] # so far we think this will always be empty, should return multiple minions when we implement replication
    if debug_Mode:
        print("after minion", minion)
        print("after minions:", minions)
    host, port = minion
    try:
        con = rpyc.connect(host, port=port)
        minion = con.root.Chunks()
        minion.put(block_uuid, data, minions)
    except:
        print("\n----Chunk Server not found -------")
        print("client: send_to_minion")
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)


def read_from_minion(block_uuid,minion):
    host, port = minion

    try:
        con = rpyc.connect(host, port=port)
        minion = con.root.Chunks()
    except:
        print("\n----Chunk Server not found -------")
        print("client: read_from_minion")
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)

    return minion.get(block_uuid)


def delete_from_chunks(block_uuid, minion):
    host, port = minion
    try:
        con = rpyc.connect(host, port=port)
        minion = con.root.Chunks()
    except:
        print("\n----Chunk Server not found -------")
        print("client: delete_from_chunks")
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)

    return minion.delete_block(block_uuid)


def get(master, fname):
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        print("File is not in the list. \n  Check list of files first")
        return

    for block in file_table:
        if debug_Mode:
            print(block)
        for m in [master.get_minions()[_] for _ in block[1]]:
            data = read_from_minion(block[0], m)
            if data:
                sys.stdout.write(data)
                break
        else:
            print("Err: Block file missed ")


def delete(master, fname):
    file_table = master.delete(fname)
    if not file_table:
        print("File is not in the list. \n  Check list of files first")
        return
    print("File entry deleted from Master server table")

    for block in file_table:
        for m in [master.get_minions()[_] for _ in block[1]]:
            condition = delete_from_chunks(block[0], m)
            if not condition:
                print("Error: File not found in chunk servers")
                return
    print("File deleted from chunk servers")


def put(master, source, dest): # will overwrite existing file with same name/dest
    size = os.path.getsize(source)  # gets the size of the file we are trying to put
    print(size)
    # obtain address and id of available chunkservers that client can write to
    blocks = master.write(dest, size) # gets the blocks of from master 
    # TODO: this adds file name to file table, if machine fails to upload, file name still exists but does not reference to any blocks i.e. will still be seen in list
    with open(source) as f:
        # loop through each of the blocks given by master and write to it
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid=b[0] # b[0] is the unique ID of each block
            minions = [master.get_minions()[_] for _ in b[1]] # gets chunkserver details for the chunk
            send_to_minion(block_uuid,data,minions) # tells chunkserver we want to write the data to specific block
            if debug_Mode:
                print(data)
                print("put master.get_minions:", master.get_minions())
                print("put b:", b)
                print("put b[1]:", b[1])
                print("put minions:", minions)
    print("File is hosted across chunk servers successfully!")

def create(master, string_data, dest):
    size = len(string_data.encode('utf-8')) 
    # size = os.path.getsize(source)  # returns the size of file in integer
    print(size)
    blocks = master.write(dest, size) # gets the blocks details from master
    total_data = string_data
    #each block is like a level
    for b in blocks:
        data = total_data[:master.get_block_size()] # select first n elements to be stored
        total_data = total_data[master.get_block_size():] # remove first n elements
        block_uuid=b[0] #b[0] is the unique ID of each block
        minions = [master.get_minions()[_] for _ in b[1]] # getting chunkserver details for the block
        send_to_minion(block_uuid,data,minions)
        if debug_Mode:
            print(data)
            print("put master.get_minions:", master.get_minions())
            print("put b:", b)
            print("put b[1]:", b[1])
            print("put minions:", minions)
    print("File is hosted across chunk servers successfully!")


def list_files(master):
    files = master.get_list_of_files()
    print(files)


def main(args):
    try:
        con = rpyc.connect("localhost", port=2131)
        master = con.root.Master()
    except:
        print("Master Server not found ")
        print("launch Master Server and try again")
        return

    while True:
        try:
            request = input("\nTYPE 'list', 'upload', 'write', 'read', 'append' or 'delete': (l/w/r/a/d) ")

            if request == "list" or request == "l":
                list_files(master)

            elif request == "upload" or request == "u":
                original_file_name = input("SOURCE FILE NAME: ")
                dest = input("DFS FILE NAME: ")
                # client.write(file_name, content)
                put(master, original_file_name, dest) #TODO: currently uploads an existing file, what about creating a new file?

            elif request == "write" or request == "w":
                dest = input("DFS FILE NAME: ")
                data = input("CONTENT:")
                # client.write(file_name, content)
                create(master, data, dest)

            elif request == "read" or request == "r":
                file_name = input("DFS FILE NAME: ")
                # client.read(file_name)
                get(master, file_name)

            elif request == "append" or request == "a":
                file_name = input("FILE NAME: ")
                content = input("APPEND: ")
                # client.write_append(file_name, content)
                print("NOT DONE YET LOL") #TODO: append

            elif request == "delete" or request == "d":
                file_name = input("DFS FILE NAME: ")
                # client.delete(file_name)
                delete(master, file_name)
            else:
                print("Invalid action entered! Try again.")

        except Exception as e:
            print(e)

    # if len(args) == 0:
    #     print "------ Help on Usage -------"
    #     print "To upload : Client.py put Destination/to/the/src/file  Name_of_the_file_in_the_GFS "
    #     print "To download: Client.py get Name_of_the_file_in_the_GFS"
    #     print "To delete: Client.py delete Name_of_the_file_in_the_GFS"
    #     print "To overwite: Client.py put Destination/to/the/src/file Name_of_the_file_in_the_GFS"
    #     return

    if args[0] == "get":
      get(master, args[1])
    elif args[0] == "put":
      put(master, args[1], args[2])
    elif args[0] == "delete":
      delete(master, args[1])
    elif args[0] == "list":
      list_files(master)
    # else:
    #   print "Incorrect command \n"
    #   print "------ Help on Usage -------"
    #   print "To upload : Client.py put Destination/to/the/src/file  Name_of_the_file_in_the_GFS "
    #   print "To download: Client.py get Name_of_the_file_in_the_GFS"
    #   print "To delete: Client.py delete Name_of_the_file_in_the_GFS"
    #   print "To overwite: Client.py put Destination/to/the/src/file Name_of_the_file_in_the_GFS"


if __name__ == "__main__":
  main(sys.argv[1:])
    # main()
