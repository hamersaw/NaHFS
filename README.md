# NaHFS
## DESCRIPTION
Needle and Hand File System (NaHFS) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## COMMANDS
#### HDFS
    ./bin/hdfs dfs -mkdir -p /user/hamersaw
    ./bin/hdfs storagepolicies -setStoragePolicy -path /user/hamersaw -policy INDEXED
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-imputed/8z6_2014_DECEMBER.csv /user/hamersaw

## TODO
#### datanode
- process indexed and not indexed blocks
- zero-copy block reads
- actually send block report and heartbeat messages (currently blank)
- bound BlockProcessor channels to alleviate memory usage
#### namenode
- pass IpcConnectionContext(user, etc) to populate owner/group on file creation
- compute file length in HdfsFileStatusProto creation
- parameterize unnecessarily hardcoded values!
- implement getBlockLocations()!
