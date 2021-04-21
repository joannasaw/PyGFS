import rpyc
import sys
import os

debug_Mode = False



def send_to_chunkServer(block_uuid, data, primaryServer, secondaryServers):
    if debug_Mode:
        print("send_to_chunksServer primaryServer:", primaryServer)
        print("send_to_chunksServer secondaryServers:", secondaryServers)
    host, port = primaryServer
    try:
        con = rpyc.connect(host, port=port)
        primaryService = con.root.Chunks()
        primaryService.put(block_uuid, data, secondaryServers)

    except Exception as e:
        print("\n----Chunk Server not found -------"+str(host)+":"+str(port))
        print("client: send_to_chunkServer")
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)


def read_from_chunkServer(block_uuid, chunkServer):
    host, port = chunkServer

    try:
        con = rpyc.connect(host, port=port)
        chunkServer = con.root.Chunks()
    except:
        print("\n----Chunk Server not found -------")
        print("client: read_from_chunkServer")
        print("----Start Chunks.py then try again ------ \n \n ")
        return False

    return chunkServer.get(block_uuid)


def delete_from_chunks(block_uuid, primaryServer, secondaryServers):
    host, port = primaryServer
    try:
        con = rpyc.connect(host, port=port)
        primaryService = con.root.Chunks()
    except:
        print("\n----Chunk Server not found -------")
        print("client: delete_from_chunks")
        print("----Start Chunks.py then try again ------ \n \n ")
        sys.exit(1)

    return primaryService.delete_block(block_uuid, secondaryServers)


def get(master, fname):
    full_data = ""
    file_table = master.get_file_table_entry(fname)
    chunkServers = master.get_chunkServers()
    print(file_table)
    if not file_table:
        print("File is not in the list. \n  Check list of files first")
        return

    for block in file_table:
        if debug_Mode:
            print(block)
        primary_idx =  block[1]
        data = read_from_chunkServer(block[0], chunkServers[primary_idx])
        if data:
            # sys.stdout.write(data)
            print("Found in Primary")
            full_data += data
            

        else:
            print("Err: Primary not responding")
            for secondaryServer in [chunkServers[sec_idx] for sec_idx in block[2]]:
                data = read_from_chunkServer(block[0], secondaryServer)
                if data:
                    # sys.stdout.write(data)
                    full_data += data
                    print("Found in Secondary")
                    
                else:
                    print("Err: Secondaries also not responding")

    return full_data


def delete(master, fname):
    file_table = master.get_file_table_entry(fname)
    chunkServers = master.get_chunkServers()
    if not file_table:
        print("File is not in the list. \n  Check list of files first")
        return False
    print("File entry deleted from Master server table")

    for block in file_table:
        # seems to have unnecessary overparsing to get primary ID '2' from e.g. block = ('8a1fdfdc-9fee-11eb-b262-00155d7a8699', '2', ['3', '4'], 0)
        secondaryServers = [chunkServers[sec_idx] for sec_idx in block[2]]
        primaryServer = chunkServers[block[1]]
        condition = delete_from_chunks(block[0], primaryServer, secondaryServers)
        if not condition:
            print("Error: File not found in chunk servers")
            return False

    file_table = master.delete(fname)
    print("File deleted from chunk servers")
    return True


def write_b(master, b, data):
    block_uuid = b[0]  # b[0] is the unique ID of each block
    # getting chunkserver details for the block

    primaryServer = [master.get_primaryServers()[_] for _ in b[1]][0]
    secondaryServers = [master.get_secondaryServers(b[1])[_] for _ in b[2]]

    send_to_chunkServer(block_uuid, data, primaryServer, secondaryServers)

    if debug_Mode:
        print("write_b data:", data)

def put(master, source, dest):  # will overwrite existing file with same name/dest
    size = os.path.getsize(source)  # returns the size of file in integer
    # print(size)
    # gets the blocks of from  TODO: this adds file name to file table, if machine fails to upload, file name still exists but does not reference to any blocks i.e. will still be seen in list
    blocks = master.write(dest, size)
    with open(source) as f:
        # loop through each of the blocks given by master and write to it
        for b in blocks:
            data = f.read(master.get_block_size())
            write_b(master, b, data)
    print("File is hosted across chunk servers successfully!")
    return True


def create(master, string_data, dest):
    size = len(string_data.encode('utf-8'))
    # size = os.path.getsize(source)  # returns the size of file in integer
    blocks = master.write(dest, size)  # gets the blocks details from master
    total_data = string_data
    # each block is like a level
    for b in blocks:
        # select first n elements to be stored
        data = total_data[:master.get_block_size()]
        # remove first n elements
        total_data = total_data[master.get_block_size():]
        write_b(master, b, data)
    print("File is hosted across chunk servers successfully!")
    return True


def write_append(master, string_data, dest):
    if master.get_file_table_entry(dest) == None:
        raise Exception("append error, file does not exist: "
                        + dest)
    size = len(string_data.encode('utf-8'))
    blocks = master.write_append(dest, size)
    total_data = string_data
    for b in blocks:
        # select first n elements to be stored
        data = total_data[:master.get_block_size()]
        # remove first n elements
        total_data = total_data[master.get_block_size():]
        write_b(master, b, data)
    print("Successfully appended to file!")
    return True


def list_files(master):
    files = master.get_list_of_files()
    print(files)


def connect_to_master():
    try:
        con = rpyc.connect("127.0.0.1", port=2131)
        master = con.root.Master()
        print("Connected to master")
        return master
    except Exception as e:
        print(e)
        print("Master Server not found: launch Master Server and try again")
        return

def connect_to_shadow():
    try:
        con = rpyc.connect("127.0.0.1", port=8100)
        master = con.root.BackUpServer()
        print("Connected to shadow")
        return master
    except Exception as e:
        print(e)
        print("Shadow Master Server not found: launch Shadow Master Server and try again")
        return



def main(args):
    print("Starting client...")


    while True:
        try:

            master = connect_to_master()
            if master is not None:
                request = input(
                    "\nTYPE 'list', 'upload', 'create', 'read', 'append' or 'delete': (l/u/c/r/a/d) ")

                if request == "list" or request == "l":
                    list_files(master)

                elif request == "upload" or request == "u":
                    original_file_name = input("SOURCE FILE NAME: ")
                    dest = input("DFS FILE NAME: ")
                    # client.write(file_name, content)
                    # TODO: currently uploads an existing file, what about creating a new file?
                    put(master, original_file_name, dest)

                # write here is a function to create a new file + upload
                elif request == "create" or request == "c":
                    dest = input("DFS FILE NAME: ")
                    data = input("CONTENT:")
                    # client.write(file_name, content)
                    create(master, data, dest)

                elif request == "read" or request == "r":
                    file_name = input("DFS FILE NAME: ")
                    # client.read(file_name)
                    data = get(master, file_name)
                    print(data)


                elif request == "append" or request == "a":
                    dest = input("FILE NAME: ")
                    data = input("APPEND: ")
                    # client.write_append(file_name, content)
                    write_append(master, data, dest)

                elif request == "delete" or request == "d":
                    file_name = input("DFS FILE NAME: ")
                    # client.delete(file_name)
                    delete(master, file_name)
                elif request == "reset":
                    new_master = connect_to_master()
                    if new_master is not None:
                        print("Client has reset")
                    else:
                        print("Unable to reset client")

                else:
                    print("Invalid action entered! Try again.")
            else:
                print("Unable to connect to Master, only READ and LIST operations allowed")
                request = input(
                    "\nTYPE 'list' or 'read': (l/r) ")
                if request == "read" or request == "r":
                    file_name = input("DFS FILE NAME: ")
                    print("\nAttempting to read from shadow master")
                    shadow_master = connect_to_shadow()
                    if shadow_master is not None:
                        data = get(shadow_master, file_name)
                        print(data)
                elif request == "list" or request == "l":
                    print("\nAttempting to list from shadow master")
                    shadow_master = connect_to_shadow()
                    if shadow_master is not None:
                        list_files(shadow_master)

        except Exception as e:
            print(e)
            if isinstance(e, EOFError):
                print("Reconnecting to master")
                new_master = connect_to_master()
                if new_master is not None:
                    master = new_master
                    print("RECONNECTED!")
                else:
                    if request == "read" or request == "r":
                        print("\nAttempting to read from shadow master")
                        shadow_master = connect_to_shadow()
                        if shadow_master is not None:
                            data = get(shadow_master, file_name)
                            print(data)
                    elif request == "list" or request == "l":
                        print("\nAttempting to list from shadow master")
                        shadow_master = connect_to_shadow()
                        if shadow_master is not None:
                            list_files(shadow_master)
    # while master is None:
    #     try:
            # print("Unable to connect to Master, only READ and LIST operations allowed")
            # request = input(
            #     "\nTYPE 'list' or 'read': (l/r) ")
            # if request == "read" or request == "r":
            #     file_name = input("DFS FILE NAME: ")
            #     print("\nAttempting to read from shadow master")
            #     shadow_master = connect_to_shadow()
            #     if shadow_master is not None:
            #         data = get(shadow_master, file_name)
            #         print(data)
            # elif request == "list" or request == "l":
            #     print("\nAttempting to list from shadow master")
            #     shadow_master = connect_to_shadow()
            #     if shadow_master is not None:
            #         list_files(shadow_master)
    #     except Exception as e:
    #         print(e)
    #         print("Unable to use shadow master")




    # if len(args) == 0:
    #     print "------ Help on Usage -------"
    #     print "To upload : Client.py put Destination/to/the/src/file  Name_of_the_file_in_the_GFS "
    #     print "To download: Client.py get Name_of_the_file_in_the_GFS"
    #     print "To delete: Client.py delete Name_of_the_file_in_the_GFS"
    #     print "To overwite: Client.py put Destination/to/the/src/file Name_of_the_file_in_the_GFS"
    #     return

    # if args[0] == "get":
    #     get(master, args[1])
    # elif args[0] == "put":
    #     put(master, args[1], args[2])
    # elif args[0] == "delete":
    #     delete(master, args[1])
    # elif args[0] == "list":
    #     list_files(master)
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
