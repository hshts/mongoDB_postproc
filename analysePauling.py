import pymongo


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']

if __name__ == '__main__':
    # print coll.index_information()
    # print 'Number of unparsable structures = {}'.format(coll.count({'structure': {'$exists': False}}))
    # print 'Number of unparsed structures so far = {}'.format(coll.count({'metadata': {'$exists': False}}))
    # print 'Number of docs with "rt" = {}'.format(coll.find({'$text': {'$search': 'rt'}}).count())
    # print 'Number of docs with "ht" = {}'.format(coll.find({'$text': {'$search': 'ht'}}).count())
    # print 'Number of docs with "hp" = {}'.format(coll.find({'$text': {'$search': 'hp'}}).count())
    # print 'Number of docs with "hp2" = {}'.format(coll.find({'$text': {'$search': 'hp2'}}).count())
    # print coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).count()
    # for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).limit(20):
    #     print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    for doc in coll.find({'$text': {'$search': 'hp'}}).limit(20):
        print doc['key'], doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
