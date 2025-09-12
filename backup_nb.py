from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergTableCreation") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .getOrCreate()




sql_1 = "CREATE DATABASE IF NOT EXISTS climate;"
# sql_2 = """CREATE TABLE IF NOT EXISTS climate.weather (
#     datetime              timestamp,
#     temp                  double,
#     lat                   double,
#     long                  double,
#     cloud_coverage        string,
#     precip                double,
#     wind_speed            double
# )
# USING iceberg
# PARTITIONED BY (days(datetime))
# ;"""

# result_1 = spark.sql(sql_1)

# result_2 = spark.sql(sql_2)


# schema = spark.table("climate.weather").schema

# data = [
# (datetime(2023,8,16), 76.2, 40.951908, -74.075272, "Partially sunny", 0.0, 3.5),
# (datetime(2023,8,17), 82.5, 40.951908, -74.075272, "Sunny", 0.0, 1.2),
# (datetime(2023,8,18), 70.9, 40.951908, -74.075272, "Cloudy", .5, 5.2)
# ]

# df = spark.createDataFrame(data, schema)
# df.writeTo("climate.weather").append()