```
$ sftp -v -i ./id_rsa -o StrictHostKeyChecking=no -P 2222 vendor@0.0.0.0
```
```
$ clickhouse-client -d <database> -m -n -q "CREATE TABLE movies (
    title String,
    duration_mins String,
    original_language String,
    size_mb UInt32
)
ENGINE=HDFS('hdfs://namenode:9000/internal/movies/*', 'CSVWithNames')"
```
