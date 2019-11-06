# NahFS
## DESCRIPTION
NahFS (Needle and Hand File System) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## FILE FORMATS
- CsvPoint(timestamp_index:3, latitude_index:0, longitude_index:1)
- Wkt(spatial_index:1)

## COMMANDS
#### HDFS
    ./bin/hdfs dfs -mkdir -p /user/hamersaw
    ./bin/hdfs storagepolicies -setStoragePolicy -path /user/hamersaw -policy \
        "CsvPoint(timestamp_index:3,latitude_index:0,longitude_index:1)"
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-imputed/8z6_2014_DECEMBER.csv /user/hamersaw
    ./bin/hdfs dfs -copyToLocal ~/user/hamersaw/8z6_2014_DECEMBER.csv+g=8bce&t>123 .

## TODO
#### datanode
- handle data which doesn't fall on boundaries (currently removing first and last observations)
- parameterize hardcoded values
#### namenode
- parameterize unnecessarily hardcoded values
