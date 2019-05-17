# NaHFS
## DESCRIPTION
Needle and Hand File System (NaHFS) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## COMMANDS
#### HDFS
    ./bin/hdfs dfs -mkdir -p /user/hamersaw
    ./bin/hdfs storagepolicies -setStoragePolicy -path /user/hamersaw -policy INDEXED
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-imputed/8z6_2014_DECEMBER.csv /user/hamersaw

## TODO
- persist system on disk
- replicate blocks
#### datanode
- implement read_block(id: &u64) to block mod (to use in protocol/namenode.rs)
- integrate BlockOutputStream into protocol/transfer.rs

- handle data which doesn't fall on boundaries (currently removing first and last observations)
- send IndexReportProto with "com.bushpath.nahfs.protocol.DatanodeProtocol";
- bound BlockProcessor channels to alleviate memory usage
#### namenode
- persist file system on disk for restarts
- pass IpcConnectionContext(user, etc) to populate owner/group on file creation
- compute file length in HdfsFileStatusProto creation
- parameterize unnecessarily hardcoded values
