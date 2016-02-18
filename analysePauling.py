import pymongo


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']

if __name__ == '__main__':
    # print coll.index_information()
    # print 'Number of unparsable structures = {}'.format(coll.count({'structure': {'$exists': False}}))
    # print 'Number of unparsed structures so far = {}'.format(coll.count({'metadata': {'$exists': False}}))
    print 'Number of docs with "rt" = {}'.format(coll.find({'$text': {'$search': 'rt'}}).count())
    print 'Number of docs with "ht" = {}'.format(coll.find({'$text': {'$search': 'ht'}}).count())
    print 'Number of docs with "hp" = {}'.format(coll.find({'$text': {'$search': 'hp'}}).count())
    print 'Number of docs with "hp2" = {}'.format(coll.find({'$text': {'$search': 'hp2'}}).count())