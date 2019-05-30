# NaHFS
## DESCRIPTION
Needle and Hand File System (NaHFS) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## COMMANDS
#### HDFS
    ./bin/hdfs dfs -mkdir -p /user/hamersaw
    ./bin/hdfs storagepolicies -setStoragePolicy -path /user/hamersaw -policy INDEXED
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-imputed/8z6_2014_DECEMBER.csv /user/hamersaw
    ./bin/hdfs dfs -copyToLocal ~/user/hamersaw/8z6_2014_DECEMBER.csv+prefix=8bce .

## TODO
- optimize blockReport and indexReport to not send entire dataset every iteration
#### datanode
- handle data which doesn't fall on boundaries (currently removing first and last observations)
- replicate blocks
- parameterize hardcoded values
#### namenode
- parameterize unnecessarily hardcoded values
