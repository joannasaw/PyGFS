import rpyc
import sys
import os

debug_Mode = True


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
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)


def read_from_minion(block_uuid,minion):
    host, port = minion

    try:
        con = rpyc.connect(host, port=port)
        minion = con.root.Chunks()
    except:
        print("\n----Chunk Server not found -------")
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
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)

    return minion.delete_block(block_uuid)


def get(master, fname):
    # print("fname in get of client:", fname)
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


def put(master, source, dest):
    size = os.path.getsize(source)  # returns the size of file in integer
    blocks = master.write(dest, size) # gets the blocks of from master
    with open(source) as f:
        #each block is like a level
        for b in blocks:
            data = f.read(master.get_block_size())
            if debug_Mode: print(data)  # debugging statement
            block_uuid=b[0] #b[0] is the unique ID of each block
            print("put master.get_minions:", master.get_minions())
            minions = [master.get_minions()[_] for _ in b[1]] # getting chunkserver details for the block
            print("put b:", b)
            print("put b[1]:", b[1])
            print("put minions:", minions)
            send_to_minion(block_uuid,data,minions)
    print("File is hosted across chunk servers successfully")


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

    # while True:
    #     try:
    #         request = input("TYPE 'write', 'read', 'append' or 'delete': (w/r/a/d) ")

    #         if request == "write" or request == "w":
    #             file_name = input("FILE NAME: ")
    #             # content = input("WRITE: ")
    #             # client.write(file_name, content)
    #             put(file_name, content)

    #         elif request == "read" or request == "r":
    #             file_name = input("FILE NAME: ")
    #             # client.read(file_name)
    #             get(file_name)

    #         elif request == "append" or request == "a":
    #             file_name = input("FILE NAME: ")
    #             content = input("APPEND: ")
    #             # client.write_append(file_name, content)
    #             print("NOT DONE YET LOL")

    #         elif request == "delete" or request == "d":
    #             file_name = input("FILE NAME: ")
    #             # client.delete(file_name)
    #             delete(file_name)
    #         else:
    #             print("Invalid action entered! Try again.")
    #             break
    #     except Exception as e:
    #         print(e)

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