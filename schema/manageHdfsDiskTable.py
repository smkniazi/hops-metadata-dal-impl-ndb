#!/usr/bin/python

import os
import sys

#DATA_DISKS = ["/media/tmpfs/"]
#DATA_DISKS = ["/var/lib/mysql-cluster/ndb/ndb_data/disk_data/"]
DATA_DISKS = [""]
DATAFILE_SIZE = "1024M"
DATAFILES_PER_DISK = 2

#LOG_DISKS = ["/media/tmpfs"]
#LOG_DISKS = ["/var/lib/mysql-cluster/ndb/ndb_data/disk_data/"]
LOG_DISKS = [""]
LOGFILE_SIZE = "1024M"
LOGFILES_PER_DISK = 2

CONNECT_STRING = "mysql -uhop -phop -P3306 -h bbc2 hop_salman_sf -e "

HDFS_FILE_INODE_DATA_SIZE=4096

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def printStage(message):
   message=("\n*** %s ***\n"% (message))
   print(bcolors.OKBLUE+bcolors.BOLD+message+bcolors.ENDC)

def executeSQLCommand(subCmd):
  command = ('%s"%s"' % (CONNECT_STRING, subCmd))
  print(command)
  os.system(command)


def create():
  # create log files
  logGroupCreated = False
  for fileIndex in range(0, LOGFILES_PER_DISK):
    for disk in LOG_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/" 

      if logGroupCreated == False:
        printStage("Creating Log Group")
        logGroupCreated = True
        subCommand = (
          "CREATE LOGFILE GROUP lg_1 ADD UNDOFILE '%s undo_log_%d.log' INITIAL_SIZE = %s ENGINE ndbcluster" % (
            disk, fileIndex, LOGFILE_SIZE))
      else:
        subCommand = (
          "ALTER LOGFILE GROUP lg_1 ADD UNDOFILE '%s undo_log_%d.log' INITIAL_SIZE = %s ENGINE ndbcluster" % (
            disk, fileIndex, LOGFILE_SIZE))
      executeSQLCommand(subCommand)

  # Create Data Files
  tableSpaceCreated = False
  for fileIndex in range(0, DATAFILES_PER_DISK):
    for disk in DATA_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/" 

      if tableSpaceCreated == False:
        printStage("Creating Table Space")
        tableSpaceCreated = True
        subCommand = ("CREATE TABLESPACE ts_1 ADD datafile '%s data_file_%d.dat' use LOGFILE GROUP lg_1 INITIAL_SIZE = %s  ENGINE ndbcluster" % (disk, fileIndex, DATAFILE_SIZE))
      else:
        subCommand = ("ALTER TABLESPACE ts_1 ADD datafile '%s data_file_%d.dat' INITIAL_SIZE = %s  ENGINE ndbcluster" % (disk, fileIndex, DATAFILE_SIZE))
      executeSQLCommand(subCommand)

  #Create Table
  printStage("Creating Tables")
  subCommand = ("CREATE table hdfs_file_inode_data ( inode_id int(11) PRIMARY KEY, data varbinary(%d) not null ) TABLESPACE ts_1 STORAGE DISK ENGINE ndbcluster partition by key (`inode_id`)"% (HDFS_FILE_INODE_DATA_SIZE))
  executeSQLCommand(subCommand)



def drop():
  #Drop table 
  printStage("Dropping Tables")
  executeSQLCommand("DROP TABLE hdfs_file_inode_data")

  # Drop Table Space
  printStage("Dropping Table Space")
  for fileIndex in range(0, DATAFILES_PER_DISK):
    for disk in DATA_DISKS:
      if disk and not disk.endswith('/'):
        disk+="/"

      subCommand = ("ALTER TABLESPACE ts_1 drop datafile '%s data_file_%d.dat' ENGINE ndbcluster" % (disk, fileIndex))
      executeSQLCommand(subCommand)

  subCommand = "DROP TABLESPACE ts_1 ENGINE ndbcluster"
  executeSQLCommand(subCommand)

  # drop log group
  printStage("Dropping Log File Group")
  subCommand = "DROP LOGFILE GROUP lg_1 ENGINE ndbcluster"
  executeSQLCommand(subCommand)


if __name__ == "__main__":
    if len(sys.argv) == 2:
      if sys.argv[1] == "create":
        create()
      elif sys.argv[1] == "drop":
        drop();
    else:
      print("%s [create|drop]" % (sys.argv[0]))

