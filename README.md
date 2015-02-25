# mysql_rsync
Incremental Dumps of MySQL databases

## Description
This tool performs mysql dumps from a source to a destination database. It does
incremental dumps, so if the process stops it will continue from where it stopped.

Example usages can be:

- Transfer data between databases
- Migrate tables to a new host
- Periodic dump of information

## Usage

  mysql_rsync.pl [--help] [--verbose] [--mysqldump=/usr/bin/mysqldump] [--src-host=localhost] --src-db=DB [--src-user=username] [--src-pass=pass] [--dst-host=HOST] [--dst-db=DB] [--dst-user=username] [--dst-pass=pass] [table1 ... tablen]

## Requirements

It requires perl and some modules, it should run fine on any platform.

- Run::IPC
- DBI

### Debian based dists
Perl should be in your linux distro, you can install the requirements doing:

  # apt-get install libdbi-perl librun-ipc-perl


