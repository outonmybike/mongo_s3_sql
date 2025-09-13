# Import required libraries
from pyspark.sql import SparkSession


def generate_spark():
    # Configure Spark session
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
    return spark

accounts_cols = [
    'account_id BIGINT',
    'name string',
    'type string',
    'x_loaded_at timestamp',
]

journal_entries_lines_cols = [
    'line_id string',
    'entry_id string',
    'date date',
    'account_id BIGINT',
    'debit float',
    'credit float',
    'description string',
]

close_tasks_cols = [
    'task_id string',
    'name string',
    'assigned_to string',
    'status string',
    'due_date date',
]





table_list = [
    {
        "table_name":"accounts",
        "partition":"",
        "table_cols": accounts_cols,
        "load_freq":"all",
    },
    {
        "table_name":"journal_entries_lines",
        "partition":"months(date)",
        "table_cols": journal_entries_lines_cols,
        "load_freq":"all",
    },    
    {
        "table_name":"close_tasks",
        "partition":"",
        "table_cols": close_tasks_cols,
        "load_freq":"all",
    },
    ]


def table_gen_sql(database, table_name, table_cols, partition_col):
    partition_txt = '' if partition_col == '' else f"PARTITIONED BY ({partition_col})"
    col_text = ", ".join(table_cols)
    sql = f'create or replace table {database}.{table_name} ({col_text}) USING iceberg {partition_txt}'
    return sql