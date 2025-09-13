from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


def main():
    print('starting')
    try:
        client = MongoClient('mongodb://admin:password@localhost:27017/admin')
        # client = MongoClient('localhost:27017')
        # client = MongoClient(
        #     'localhost:27017',
        #     username='admin',
        #     password='password',
        #     authSource='admin' # The database where the user is defined
        # )
        # print(client.list_database_names())
        # client.admin.command('ismaster')
        print("Connection to MongoDB successful!")
        mydb = client['accounting_db']
        mycollection = mydb["humans"]
        mycollection.insert_one({"name": "John Doe", "age": 30})

        # # You can now interact with your database
        # db = client.mydatabase
        # collection = db.mycollection
        # collection.insert_one({"message": "Hello from PyMongo!"})
        # print("Data inserted successfully.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()

main()