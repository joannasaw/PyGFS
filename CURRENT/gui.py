from tkinter import *
from tkinter import filedialog, messagebox
from client_b import *

import rpyc
import sys
import os

# Attempt connection with Master Server as a Client GUI
try:
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()
    print("Successfully connected to Master Server")

except:
    print("Master Server not found ")
    print("launch Master Server and try again")
# Displays the list of available filenames hosted on the server


def clearDisplay():
    txtarea.delete("1.0", "end")


def clearFileEntry():
    filenameEntry.delete(0, 'end')


def refreshAllFiles():
    for label in list(frame1.children.values()):
        label.destroy()

    files = master.get_list_of_files()
    print(files)
    for i, each_file in enumerate(files):
        label = Button(frame1,
                       text=each_file,
                       command=lambda file_name=each_file: readFile(file_name)).pack(side=TOP, fill=BOTH)
    Button(
        frame1,
        text="Refresh",
        command=refreshAllFiles,
    ).pack(side=BOTTOM)

# Opens local directory to import file into GUI for preview before upload


def openFile():
    tf = filedialog.askopenfilename(
        initialdir="C:/Users/MainFrame/Desktop/",
        title="Open Text file",
        filetypes=(("Text Files", "*.txt"),)
    )
    filenameEntry.delete(0, 'end')
    filenameEntry.insert(END, tf)
    tf = open(tf)

    # set global filename variable for uploadFile() function to use
    global filename
    filename = os.path.split(tf.name)[1]
    file_cont = tf.read()
    txtarea.config(state=NORMAL)
    txtarea.delete("1.0", "end")
    txtarea.insert(END, file_cont)
    txtarea.config(state=DISABLED)
    tf.close()


def readFile(file_name):
    full_data = get(master, file_name)
    # print(full_data)
    txtarea.config(state=NORMAL)
    txtarea.delete("1.0", "end")
    txtarea.insert(END, full_data)
    txtarea.config(state=DISABLED)


def uploadFile():
    # print(filename)
    success = put(master, filename, filename)

    if success:
        messagebox.showinfo(
            'Upload Success', "File successfully uploaded to Server")
        refreshAllFiles()
        clearDisplay()
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
    file_name = filenameEntry.get()
    success = delete(master, file_name)
    if success:
        messagebox.showinfo(
            'Delete Success', 'File Successfully deleted from Server')
        refreshAllFiles()
        clearDisplay()
    else:
        messagebox.showinfo(
            'Delete Failure', 'Error deleting file from Server')


def createFile():
    file_name = filenameEntry.get()
    file_content = txtarea.get("1.0", END)

    success = create(master, file_content, file_name)
    if success:
        messagebox.showinfo(
            'Create Success', 'File successfully created on Server')
        refreshAllFiles()
        clearDisplay()
    else:
        notice.config(text="Error creating file on Server")


def appendFile():
    file_name = filenameEntry.get()
    content_to_append = str(contentEntry.get())
    print("content:", content_to_append)
    success = write_append(master, content_to_append, file_name)
    if success:
        messagebox.showinfo('Append Success', 'Successfully Appended to File')
        refreshAllFiles()
        clearDisplay()
        contentEntry.delete(0, "end")
        readFile(file_name)
    else:
        messagebox.showinfo('Append Failure', 'Error Appending to File')


ws = Tk()
ws.title("Budget GFS")
ws.geometry("700x530")
ws['bg'] = '#2a636e'

# adding frames
frame = Frame(ws, width=400, borderwidth=3, relief=RAISED)
frame.pack(pady=20, padx=20,)
txtlabel = Label(frame, text="File content")
txtlabel.pack(side=TOP)

frame1 = Frame(frame, width=100, borderwidth=3, relief=RAISED)
frame1.pack(side=LEFT, fill=Y)
Button(
    frame1,
    text="Refresh",
    command=refreshAllFiles,
).pack(side=BOTTOM)
# adding scrollbars
# ver_sb = Scrollbar(frame, orient=VERTICAL)
# ver_sb.pack(side=RIGHT, fill=BOTH)

# hor_sb = Scrollbar(frame, orient=HORIZONTAL)
# hor_sb.pack(side=BOTTOM, fill=BOTH)


# adding display space
txtarea = Text(frame, width=60, height=20)
txtarea.pack(side=LEFT)
# txtarea.config(state=DISABLED)


# binding scrollbar with text area
# txtarea.config(yscrollcommand=ver_sb.set)
# ver_sb.config(command=txtarea.yview)

# txtarea.config(xscrollcommand=hor_sb.set)
# hor_sb.config(command=txtarea.xview)
frame2 = Frame(ws)
frame2.pack(fill=X, ipady=10)
# adding path showing box
filenameLabel = Label(frame2, text="File Name", width=10)
filenameLabel.pack(side=LEFT, padx=5, pady=5)
filenameEntry = Entry(frame2)
filenameEntry.pack(expand=True, fill=X, padx=5)

frame3 = Frame(ws)
frame3.pack(fill=X)

contentLabel = Label(frame3, text="Edit", width=10)
contentLabel.pack(side=LEFT, padx=5, pady=5)
contentEntry = Entry(frame3)
contentEntry.pack(expand=True, fill=X, padx=5)

# Fetch all the available files on the server once at the start
refreshAllFiles()

frame4 = Frame(ws)
frame4.pack(fill=X, ipady=20)

# adding all the buttons
Button(
    frame4,
    text="Open",
    command=openFile
).pack(side=LEFT, expand=True, fill=X, padx=10)

Button(
    frame4,
    text="Download",
    command=saveFile
).pack(side=LEFT, expand=True, fill=X, padx=10)

Button(
    frame4,
    text="Create",
    command=createFile
).pack(side=LEFT, expand=True, fill=X, padx=10)

# Button(
#     ws,
#     text="Read",
#     command=readFile
# ).pack(side=LEFT, expand=True, fill=X, padx=20, pady=20)

Button(
    frame4,
    text="Upload",
    command=uploadFile
).pack(side=LEFT, expand=True, fill=X, padx=10)


Button(
    frame4,
    text="Delete",
    command=deleteFile
).pack(side=LEFT, expand=True, fill=X, padx=10)


Button(
    frame4,
    text="Append",
    command=appendFile
).pack(side=LEFT, expand=True, fill=X, padx=10)

frame5 = Frame(ws)
frame5.pack(fill=X, ipady=10)
Button(
    frame5,
    text="Exit",
    command=lambda: ws.destroy()
).pack(side=LEFT, expand=True, fill=X, padx=300)

# ws.resizable(width=False, height=False)
ws.mainloop()
