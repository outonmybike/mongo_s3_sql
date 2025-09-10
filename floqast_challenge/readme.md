Pseudocode


Step 1: Get latest pull date from file names in S3 - python
step 2: Get all records in MondoDB greater than lastest pull date (truncated by 1 minute) -pyspark
    note: set up truncated time frame to be flexible to day, hour, minute, second
        pyspark steps:
            unnesting of data, 1 row per entry for entries tables
            calculate total debit/credit. check for sum = 0, validation, etc
            add partition columns year, month
Step 3: write to S3 with Iceberg formatting. Use incremental updates with Iceberg Merge INTO CDC

Step 4: Optimize S3 files with partitions and enable compaction, enable snapshots


https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/

https://www.bmc.com/blogs/mongodb-docker-container/
