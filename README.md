# filescan
Filesystem scanner in Golang

Notes:

Supports three modes:

1. Scan a specified file system using a pathname on the command line and save the
metadata in the db
1. Create a sqlite db
1. Run the reports on the saved file system metadata

Usage of the filescan cli:

```
  -dbName string
        database name
  -makedb
        initiallize the db
  -path string
        root path name
  -report
        run usage report
```

Examples:

```
./filescan --makedb=true --dbName="files.db"
./filescan --dbName="files.db" --path="/home/ubuntu"
./filescan --dbName="files.db" --report=true"
```

Supported Reports:

- file count report
- file size report
- files by owner (uid and gid)
- file aging report
