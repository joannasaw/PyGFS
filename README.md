# PyGFS
This README.md documents the files and work for 50.041 Distributed Systems project taken in Spring 2021.
```
├── CURRENT
|   ├── client_b.py
|   ├── master_b.py
|   ├── chunkserver_b.py
│   ├── letters.txt
│   ├── numbers.txt
│   └── GFS.conf
|
├── Hybrid
|
├── gfs_root (will be ignored)
│  
├── .gitignore
└── README.md
```

* Note that the gfs_root folder will be ignored, and will be created locally upon running

INSTRUCTIONS:
1. Run master_b.py in a terminal
2. Run chunkserver_b.py in 4 other terminals
- When requested, input DIFFERENT port values (e.g. 8888, 7777, 6666, 5555)
3. Run client_b.py in 2 other terminals
- Input commands: 'list', 'upload', 'write', 'read', 'append' or 'delete (l/w/r/a/d)
