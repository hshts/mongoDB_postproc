import pymongo

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find().sort('_id', pymongo.ASCENDING).limit(100):
        try:
            density = doc['metadata']['_Springer']['geninfo']['Density']
            print doc['key'], density
            print float(density.split()[2])
        except Exception as e:
            print 'Exception at {}'.format(doc['key'])
            print 'Density given as ' + doc['metadata']['_Springer']['geninfo']['Density']