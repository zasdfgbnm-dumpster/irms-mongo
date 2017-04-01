from pymongo import MongoClient
client = MongoClient()
db = client.test
collection = db.test
infile = '/home/gaoxiang/irms/irms-mongo/001_test.txt'
docs = [{ '_id':i.strip().split()[0], 'str':i.strip().split()[1] } for i in open(infile) if len(i.strip().split())==2 ]
result = collection.insert_many(docs)
print(result.inserted_ids)
