from tkinter import *
from tkinter import filedialog
from client_b import *

import rpyc
import sys
import os
# functions
try:
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()
    print("Successfully connected to Master Server")
    # refreshAllFiles()
except:
    print("Master Server not found ")
    print("launch Master Server and try again")


def refreshAllFiles():
    files = master.get_list_of_files()
    print(files)
    for each_file in files:
        label = Label(frame, text=each_file)
        label.pack(side=LEFT, expand=True, fill='x')


def openFile():
    tf = filedialog.askopenfilename(
        initialdir="C:/Users/MainFrame/Desktop/",
        title="Open Text file",
        filetypes=(("Text Files", "*.txt"),)
    )
    pathh.insert(END, tf)
    tf = open(tf)
    global filename
    filename = os.path.split(tf.name)[1]
    file_cont = tf.read()
    # print(file_cont)
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
        notice.config(text="File successfully uploaded")
    else:
        notice.config(text="Error in uploading file")


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


ws = Tk()
ws.title("PythonGuides")
ws.geometry("400x500")
ws['bg'] = '#2a636e'

# adding frame
frame = Frame(ws)
frame.pack(pady=20)

# adding scrollbars
ver_sb = Scrollbar(frame, orient=VERTICAL)
ver_sb.pack(side=RIGHT, fill=BOTH)

hor_sb = Scrollbar(frame, orient=HORIZONTAL)
hor_sb.pack(side=BOTTOM, fill=BOTH)

# list of files
# listfiles = Label(frame, width=20, text="Empty List")
# listfiles.pack(side=LEFT)

refreshAllFiles()
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

notice = Label(ws, text="Welcome!")
notice.pack()

# adding buttons
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
    text="Exit",
    command=lambda: ws.destroy()
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)


ws.mainloop()
