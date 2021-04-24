# PyGFS
This README.md documents the files and work for 50.041 Distributed Systems project taken in Spring 2021.
```
├── MAIN
|   ├── chunkserver.py
|   ├── client.py
|   ├── gui.py
|   ├── master.py
|   ├── shadow_master.py
│   ├── letters.txt
│   ├── numbers.txt
│   └── GFS.conf
|
├── gfs_root (will be ignored)
│  
├── .gitignore
└── README.md
```

* Note that the gfs_root folder will be ignored, and will be created locally upon running

INSTRUCTIONS:
1. Run shadow_master.pu in a terminal
2. Run master.py in another terminal
3. Run chunkserver.py in 6 other terminals
- When requested, input port values: 8888, 8887, 8886, 8885, 8884, 8883
4. Run client.py in 2 other terminals
- Input commands: 'list', 'upload', 'create', 'read', 'append' or 'delete (l/u/c/r/a/d)
OR
  Run gui.py in 2 other terminals
- Use tkinter interface to execute above functions
