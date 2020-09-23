# NahFS
## DESCRIPTION
NahFS (Needle and Hand File System) is a distributed, spatio-temporal file system presenting an HDFS compatible interface.

## FILE FORMATS
- CsvPoint(timestamp_index:x, latitude_index:y, longitude_index:z)
- Wkt(spatial_index:x)

## COMMANDS
#### start / stop cluster
    ./sbin/start-all.sh
    ./sbin/stop-all.sh
#### create 'indexed' directory
    ./bin/hdfs dfs -mkdir -p /noaa-1-hour/csv/2013
    ./bin/hdfs storagepolicies -setStoragePolicy -path /noaa-1-hour/csv -policy "CsvPoint(timestamp_index:3,latitude_index:0,longitude_index:1)"

    ./bin/hdfs dfs -mkdir -p /noaa-1-hour/csv-mod/2013
    ./bin/hdfs storagepolicies -setStoragePolicy -path /noaa-1-hour/csv-mod -policy "CsvPoint(timestamp_index:2,latitude_index:1,longitude_index:0)"
#### upload files
    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-1-hour/csv/2013/d7m_2013_DECEMBER.csv /noaa-1-hour/csv/2013

    ./bin/hdfs dfs -copyFromLocal ~/Downloads/noaa-1-hour/csv-mod/2013/d7m_2013_DECEMBER.csv /noaa-1-hour/csv-mod/2013
#### download files (with spatiotemporal filter)
    ./bin/hdfs dfs -copyToLocal "/noaa-1-hour/csv/2013/d7m_2013_DECEMBER.csv+g=92d9&t>123" .

    ./bin/hdfs dfs -copyToLocal "/noaa-1-hour/csv-mod/2013/d7m_2013_DECEMBER.csv+g=92d9&t>123" .

## TODO
#### datanode
- handle data which doesn't fall on boundaries (currently removing first and last observations)
- parameterize hardcoded values
#### namenode
- enable temporal distribution of datasets
- include temporal attributes in block distribution
- parameterize unnecessarily hardcoded values
