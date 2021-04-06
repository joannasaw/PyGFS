from tkinter import *
from tkinter import filedialog, messagebox
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
    for i, each_file in enumerate(files):
        label = Button(frame1,
                       text=each_file,
                       command=lambda file_name=each_file: fillPath(file_name)).pack(side=TOP, fill=BOTH)


def fillPath(file_name):
    print(file_name)
    pathh.delete(0, 'end')
    pathh.insert(END, file_name)
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
        messagebox.showinfo(
            'Upload Success', "File successfully uplaoded to Server")
        refreshAllFiles()
    else:
        messagebox.showinfo('Upload Fail', "Error uploading file to Server")


def saveFile():
    tf = filedialog.asksaveasfilename(
        title="Save file",
        defaultextension=".txt"
    )

    f = open(tf, 'w')
    data = str(txtarea.get(1.0, END))
    f.write(data)
    f.close()
    messagebox.showinfo('Success', 'File Saved')


def deleteFile():
    file_name = pathh.get()
    success = delete(master, file_name)
    if success:
        messagebox.showinfo(
            'Delete Success', 'File Successfully deleted from Server')
        refreshAllFiles()
    else:
        messagebox.showinfo(
            'Delete Failure', 'Error deleting file from Server')


def createFile():
    file_name = pathh.get()
    file_content = txtarea.get("1.0", END)

    success = create(master, file_content, file_name)
    if success:
        messagebox.showinfo(
            'Create Success', 'File successfully created on Server')
        refreshAllFiles()
    else:
        notice.config(text="Error creating file on Server")


def appendFile():
    file_name = pathh.get()
    content_to_append = str(txtarea.get("1.0", END))
    print("content:", content_to_append)
    success = write_append(master, content_to_append, file_name)
    if success:
        messagebox.showinfo('Append Success', 'Successfully Appended to File')
        refreshAllFiles()
    else:
        messagebox.showinfo('Append Failure', 'Error Appending to File')


ws = Tk()
ws.title("Budget GFS")
ws.geometry("1000x500")
ws['bg'] = '#2a636e'

# adding frames
frame = Frame(ws)
frame.pack(pady=20)

frame1 = Frame(frame, width=100)
frame1.pack(expand=True, fill='x', side=LEFT)

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

# Fetch all the available files on the server once at the start
refreshAllFiles()

Button(
    ws,
    text="Refresh",
    command=refreshAllFiles


).pack(padx=20)
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
    text="Create File",
    command=createFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Read File",
    command=readFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Upload File",
    command=uploadFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)


Button(
    ws,
    text="Delete File",
    command=deleteFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)


Button(
    ws,
    text="Append File",
    command=appendFile
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    ws,
    text="Exit",
    command=lambda: ws.destroy()
).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

ws.mainloop()
