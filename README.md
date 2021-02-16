# zerodb
**zerodb** is a NoSQL, storage-supported, fast embedded database. It is like NoSQL version of sqlite. It currently support only Python language. It is uitable for Python applications that need sqlite-like database but much faster and with storage support. The database can store any json data. All the data is maintained in the memory. The data is also backed to a file if the user needs storage support. The database gurantees safety of the storage file against application crash.

## Installation
You can install zerodb by cloning the repo.
```
> git clone git@github.com:justcli/zerodb.git
```
Then running the following commands
```
> cd zerodb
> ./install.sh
```

## How to use
You can just import it and use the zerodb. You do it by creating an instance of the class ZeroDB.
```
from zerodb import ZeroDB
mydb = ZeroDB("./mydir/mydb.zdb")
```
In the above code, a ZeroDB instance was created by loading the mydb.zdb database file. If the file is not present, it is created.
You can instantiate without passing any database filename. In that case, the database is maintained only in memory.
```
from zerodb import ZeroDB
mydb = ZeroDB()
```
The ZeroDB object implements four methods.
**insert** : to insert a json data against a user given key.
```
d = {"name": "Amaing Name", "Grade":5}
mydb.insert("students", d)
d = {"name": "Funny Name", "Grade":6}
mydb.insert("students", d)
```
Note above that two pieces of json data are inserted against the same key. zerodb allows multiple inserts against the same key. All the data is appended to a list.

**query** : to get the data from the database
```
students = mydb.query("studets")
for student in students:
  print(student)
```
The query always returns a list which can have one or many entries.

**remove** : to remove a key and all it's data
```
mydb.remove("students")
```

**flush** : to force the dabase ti immediately flush the data to the database file
```
mydb.flush()
```

**tidyup** : to tidyup stale or remoed entires from the database file
*zerodb* database file is a journal where nothing is actually deleted. Keys are just marked as deleted. It means that the database file always grow even if keys are deleted. To take care of this, *zerodb* provides tidyup method. When called, this methos shrinks the database file by removing all deleted entries.

You can also tidyup your database file using commandline.
```
> zerodb -tidyup ./mydir/mydb.zdb mydb_new.zdb
or
> zerodb -tidyup ./mydir/mydb.zdb > mydb_new.zdb
> mv mydb_new.zdb mydb.zdb
```

## Benchmark
You can benchmark *zerodb* by running the following command
```
> zerodb -benchmark
```
On my Macbook (Core-i5, 8GB RAM running MacOS Mojave), I get the following results.
```
> zerodb -benchmark
In-memory : 276480 inserts / sec
Storage   : 84142 inserts / sec
```

