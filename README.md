# NaHFS
## DESCRIPTION
Needle and Hand File System (NaHFS) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## COMMANDS
#### HDFS
    ./bin/hdfs dfs -mkdir -p /user/hamersaw
    ./bin/hdfs storagepolicies -setStoragePolicy -path /user/hamersaw -policy INDEXED
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-imputed/8z6_2014_DECEMBER.csv /user/hamersaw

## TODO
- parse filename with embedded query (ex. blah.csv?geohash=9fa)
#### datanode
- handle data which doesn't fall on boundaries (currently removing first and last observations)
- send IndexReportProto with "com.bushpath.nahfs.protocol.DatanodeProtocol";
- replicate blocks
- parameterize hardcoded values
#### namenode
- set complete on HdfsFileStateProto when all blocks are available
- compute file length in HdfsFileStatusProto creation
- persist file system on disk for restarts
- parameterize unnecessarily hardcoded values
