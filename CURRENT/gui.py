from tkinter import *
from tkinter import filedialog
from client_b import *

import rpyc
import sys
import os

# Attempt connectiong with Master Server as a Client GUI
try:
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()
    print("Successfully connected to Master Server")
except:
    print("Master Server not found ")
    print("launch Master Server and try again")

# Displays the list of available filenames hosted on the server


def refreshAllFiles():
    for label in list(frame1.children.values()):
        label.destroy()

    files = master.get_list_of_files()
    print(files)
    for each_file in files:
        label = Label(frame1, text=each_file)
        label.pack(side=LEFT, expand=True, fill='x')

# Opens local directory to import file into GUI for preview before upload


def openFile():
    tf = filedialog.askopenfilename(
        initialdir="C:/Users/MainFrame/Desktop/",
        title="Open Text file",
        filetypes=(("Text Files", "*.txt"),)
    )
    pathh.delete(0, 'end')
    pathh.insert(END, tf)
    tf = open(tf)

    # set global filename variable for uploadFile() function to use
    global filename
    filename = os.path.split(tf.name)[1]
    file_cont = tf.read()
    txtarea.delete("1.0", "end")
    txtarea.insert(END, file_cont)
    tf.close()


def readFile():
    file_name = pathh.get()
    full_data = get(master, file_name)
    print(full_data)
    txtarea.delete("1.0", "end")
    txtarea.insert(END, full_data)


def uploadFile():
    print(filename)
    success = put(master, filename, filename)

    if success:
        notice.config(text="File successfully uploaded to Server")
        refreshAllFiles()
    else:
        notice.config(text="Error in uploading file to Server")


def saveFile():
    tf = filedialog.asksaveasfile(
        mode='w',

        title="Save file",
        defaultextension=".txt"
    )
    tf.config(mode='w')

    pathh.insert(END, tf)
    data = str(txtarea.get(1.0, END))
    tf.write(data)

    tf.close()


def deleteFile():
    file_name = pathh.get()
    success = delete(master, file_name)
    if success:
        notice.config(text="File successfully deleted from Server")
        refreshAllFiles()
    else:
        notice.config(text="Error deleting file from Server")


def createFile():
    file_name = pathh.get()
    file_content = txtarea.get("1.0", END)

    success = create(master, file_content, file_name)
    if success:
        notice.config(text="File successfully created on Server")
        refreshAllFiles()


ws = Tk()
ws.title("Budget GFS")
ws.geometry("1000x500")
ws['bg'] = '#2a636e'

# adding frames
frame = Frame(ws)
frame.pack(pady=20)

frame1 = Frame(frame, relief="ridge", width=100)
frame1.pack(side=LEFT)

# adding scrollbars
ver_sb = Scrollbar(frame, orient=VERTICAL)
ver_sb.pack(side=RIGHT, fill=BOTH)

hor_sb = Scrollbar(frame, orient=HORIZONTAL)
hor_sb.pack(side=BOTTOM, fill=BOTH)


# adding writing space
txtarea = Text(frame, width=60, height=20)
txtarea.pack(side=LEFT)

# binding scrollbar with text area
txtarea.config(yscrollcommand=ver_sb.set)
ver_sb.config(command=txtarea.yview)

txtarea.config(xscrollcommand=hor_sb.set)
hor_sb.config(command=txtarea.xview)

# adding path showing box
pathh = Entry(ws)
pathh.pack(expand=True, fill=X, padx=10)

# adding notice text below to show status messages
notice = Label(ws, text="Welcome to low budget GFS!")
notice.pack()

# Fetch all the available files on the server once at the start
refreshAllFiles()

# adding all the buttons
Button(
    ws,
    text="Open File",
    command=openFile
).pack(side=LEFT, expand=True, fill=X, padx=20)

Button(
    ws,
    text="Save File",
    command=saveFile
).pack(side=LEFT, expand=True, fill=X, padx=20)

Button(
    ws,
    text="Upload File",
    command=uploadFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Read File",
    command=readFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Delete File",
    command=deleteFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Create File",
    command=createFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Exit",
    command=lambda: ws.destroy()
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)


ws.mainloop()
