# Crunch

[![Build Status](https://travis-ci.org/Shelnutt2/crunch.svg?branch=master)](https://travis-ci.org/Shelnutt2/crunch)

MariaDB storage engine based on cap'n proto storage

## Requirements

Requires MariaDB 10.2 or newer. It is untested on older versions.

## Installation

There are two methods to building crunch.

### Inside MariaDB Source Tree (Recommended)
The first is inside a MariaDB source tree (recommended).

```bash
git clone git@github.com:MariaDB/server.git
cd server
git submodule add git@github.com:Shelnutt2/crunch.git storage/crunch
mkdir build && cd build
cmake ..
make -j4
```

### Standalone compilation

The second method is building it standalone and
installing the library to the mariadb plugin folder.
This method is not recommended, as the compiler and
compiler flags must match for mariadb to load the plugin.

```bash
git clone git@github.com:Shelnutt2/crunch.git
cd crunch
mkdir build && cd build
cmake .. -DCRUNCH_COMPILE_STANDALONE=ON
make -j4
```

## Features

### Inplace Alter Table

In place alter tables are supported for column renames, column additions and
some column data type changes.
Adding of columns is supported at any position (LAST or AFTER supported).

When altering tables, the on disk data format is not changed. A new schema is
produced and when reading data it is converted on read to the new schema.

If a default value is set for a column and the column is also set to null
then the default value is not used, but the column will return null for
existing data. This is a limitation that will be addressed in the future.

For column data type changes, if the underlying type is similar, int16->int32
or float64 -> float32, the change can be made online. There is no protection
against decreased percision or overflow. Changing from a int64 to a int16
is allowed, but could result in data loss if any values are larger than an
int16. This is how mariadb normally works, so we've allowed it with online
alters.

## Architecture

### On Disk Format

The ondisk format is based on cap'n proto. Each table gets a cap'n proto schema
file which represent a row in the table.
See [examples/t1.capnp](examples/t1.capnp) for a sample cap'n proto schema
representing the following table:

```sql
CREATE TABLE t1 (
  column1 integer,
  column2 varchar(64)
) ENGINE=crunch;
```

### On Disk File Hierarchy

Below is a hierarchy of the ondisk structure,
assuming the database is test and table is t1.
```
mysql_datadir
└── test
    ├── t1
    │   ├── 1515252170775990244-e8e7a69e-923d-471f-b0ca-8e54568a3fef.capnpd
    │   ├── t1.capnp
    │   ├── t1.capnpd
    │   ├── t1.deleted.capnpd
    │   └── transactions
    │       ├── 1515251992233237213-b0929f33-a67c-4d7e-83e1-594b983ce299.capnpd
    │       └── 1515251992233237213-b0929f33-a67c-4d7e-83e1-594b983ce299.deleted.capnpd
    └── t1.frm
```

### Transactions

Transactions are supported and handled with the crunchTxn class.
Each transaction will perform it's operations in a "transactions" folder,
with dedicated files per table/transactions.
This allows for ondisk isolation for of transactions before commit.
It also removes the immediate need for an undo log.

A rollback simply deletes the transaction files and its done.
When a commit is made, the transaction files are renamed and moved to the
main table folder. The rename function call is atomic according to the
[ISO c standard](http://pubs.opengroup.org/onlinepubs/9699919799/functions/rename.html)
assuming the transaction folder lies on the same filesystem as the main
table folder.

The transaction folder is a subdirectory in the table directory.
The transaction files are in the format of
**unix_timestamp_nanoseconds**-**uuid**.ext

The use of unix timestamp is to order the files on disk so full table scans
can sequentially read all files and keep the ordering the same as original
inserts.

A consequence of using independent files for each transaction is that the
number of data files will grow without bounds. During a full table scan
each data file must be opened and mmaped one at a time. Performance
degrades in a linear manor. To combat this a consolidation of datafiles
has been implemented. Currently this only acts on table close.
The long term goal is to implement a daemon plugin which will implement
consolidation in the background independent of the mysql server. See #44.

Consolidation can also be manually forced by running `OPTIMIZE TABLE tbl_name`

## System and Table Variables

Below are a list of server and table configuration variables.

| Variable Bame | description | Data Type | Default Value | Range | System Variable | Table Variable |
| ------------- | ----------- | --------- | ------------- | ----- | --------------- | -------------- |
| consolidation_threshold | Threshold for number of data files to start consolidation | Integer | 100 | 0 to MAX_INT | Yes | Yes |
