import config

def main():
    print('hello')

    SPARK = config.generate_spark()
    TABLE_LIST = config.table_list
    DATABASE = 'mongo_imports'


    # # Create a database
    SPARK.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    
    # Create Iceberg tables
    for table in TABLE_LIST:
        name = table['table_name']
        cols = table['table_cols']
        partition_col = table['partition']
        table_sql = config.table_gen_sql(DATABASE,name,cols,partition_col)
        print(table_sql)
        # SPARK.sql(table_sql)


    # # Insert sample data
    #### LOOK UP DF insertion
    # SPARK.sql("""
    # INSERT INTO demo.nyc.taxis
    # VALUES 
    #     (1, 1000371, 1.8, 15.32, 'N'),
    #     (2, 1000372, 2.5, 22.15, 'N'),
    #     (2, 1000373, 0.9, 9.01, 'N'),
    #     (1, 1000374, 8.4, 42.13, 'Y')
    # """)
    # # Verify the data
    # SPARK.sql("SELECT * FROM demo.nyc.taxis").show()
    # # Stop the SPARK session
    # SPARK.stop()