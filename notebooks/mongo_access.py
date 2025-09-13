from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


env = 'vsc' if __name__ == '__main__' else 'nb'

def main():
    print('new logic')
    path = 'localhost:27017' if env == 'vsc' else 'mongo'
    client = MongoClient(f'mongodb://root:example@{path}/admin')
    print(client.list_database_names())
    # client.admin.command('ismaster')
    print("Connection to MongoDB successful!")
    mydb = client['accounting_db']
    mycollection = mydb["humans"]
    mycollection.insert_one({"name": "Dan Doe", "age": 30})
    print('data added')
if __name__ == '__main__':
    main()