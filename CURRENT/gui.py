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
    txtarea.config(state=NORMAL)
    txtarea.delete("1.0", END)
    txtarea.config(state=DISABLED)


def clearFileEntry():
    filenameEntry.delete(0, 'end')


def clearEditEntry():
    contentEntry.delete(0, 'end')


def refreshAllFiles():
    for label in list(frame1.children.values()):
        label.destroy()

    global files
    files = master.get_list_of_files()
    for i, each_file in enumerate(files):
        label = Button(frame1,
                       text=os.path.splitext(each_file)[0],
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

    tf = open(tf)

    # set global filename variable for uploadFile() function to use
    global filename
    filename = os.path.split(tf.name)[1]
    filename = os.path.splitext(filename)[0]
    filenameEntry.insert(END, filename)
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
    clearFileEntry()
    filenameEntry.insert(0, file_name)
    clearEditEntry()


def uploadFile():
    print(filename)
    file_name = filenameEntry.get()
    file_content = txtarea.get("1.0", END)
    if len(str(file_name)) == 0:
        messagebox.showinfo('Error', "Please enter filename")
    elif len(str(file_name)) > 20:
        messagebox.showinfo(
            'Error', "File name cannot exceed 20 characters")
    elif file_name in list(files):
        messagebox.showinfo(
            'Error', "File name already exist on server!")
    elif len(str(file_content)) == 0:
        messagebox.showinfo('Error', "Please enter content of file")
    else:
        success = create(master, file_content, file_name)

        if success:
            messagebox.showinfo(
                'Upload Success', "File successfully uploaded to Server")
            refreshAllFiles()
            clearDisplay()
            clearFileEntry()
            clearEditEntry()
        else:
            messagebox.showinfo(
                'Upload Fail', "Error uploading file to Server")


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
    if len(str(file_name)) == 0:
        messagebox.showinfo('Error', "Please enter filename")
    success = delete(master, file_name)
    if success:
        messagebox.showinfo(
            'Delete Success', 'File Successfully deleted from Server')
        refreshAllFiles()
        clearDisplay()
        clearFileEntry()
        clearEditEntry()
    else:
        messagebox.showinfo(
            'Delete Failure', 'Error deleting file from Server')


def createFile():
    file_name = filenameEntry.get()
    if len(str(file_name)) == 0:
        messagebox.showinfo('Error', "Please enter filename")
    file_content = txtarea.get("1.0", END)
    if len(str(file_content)) == 0:
        messagebox.showinfo('Error', "Please enter content of file")
    success = create(master, file_content, file_name)
    if success:
        messagebox.showinfo(
            'Create Success', 'File successfully created on Server')
        refreshAllFiles()
        clearDisplay()
        clearFileEntry()
        clearEditEntry()
    else:
        notice.config(text="Error creating file on Server")


def appendFile():
    file_name = filenameEntry.get()
    if len(str(file_name)) == 0:
        messagebox.showinfo('Error', "Please enter filename")
    content_to_append = str(contentEntry.get())
    print("content:", content_to_append)
    success = write_append(master, content_to_append, file_name)
    if success:
        messagebox.showinfo('Append Success', 'Successfully Appended to File')
        refreshAllFiles()
        clearDisplay()
        contentEntry.delete(0, "end")
        readFile(file_name)
        clearEditEntry()
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

# adding display space
txtarea = Text(frame, width=60, height=20)
txtarea.pack(side=LEFT)

frame2 = Frame(ws)
frame2.pack(fill=X, ipady=10)
# adding path showing box
filenameLabel = Label(frame2, text="File Name", width=10)
filenameLabel.pack(side=LEFT, padx=5, pady=5)
frame5 = Frame(frame2)
frame5.pack(expand=True, fill=X)
filenameEntry = Entry(frame5, borderwidth=1)
filenameEntry.pack(expand=True, fill=X, side=LEFT, padx=5)
Button(
    frame5,
    text="Clear",
    command=clearFileEntry
).pack(padx=5)
frame3 = Frame(ws)
frame3.pack(fill=X)


contentLabel = Label(frame3, text="Edit", width=10)
contentLabel.pack(side=LEFT, padx=5, pady=5)
frame6 = Frame(frame3)
frame6.pack(expand=True, fill=X)
contentEntry = Entry(frame6, borderwidth=1)
contentEntry.pack(expand=True, fill=X, side=LEFT, padx=5)
Button(
    frame6,
    text="Clear",
    command=clearEditEntry
).pack(padx=5)

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
