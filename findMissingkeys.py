import pymongo

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    print db['pauling_file'].count()
    print db['pauling_file_unique'].count()

    unique_sds = []
    duplicate_sds = set()

    for doc in db['pauling_file_unique'].find():
        if doc['key'] not in unique_sds:
            unique_sds.append(doc['key'])
        else:
            duplicate_sds.add(doc['key'])

    print len(unique_sds)
    print len(duplicate_sds)

    i = 0
    for key in duplicate_sds:
        i += 1
        if i < 3:
            matches = db['pauling_file_unique'].find({'key': key})
            for match in matches:
                print match
                print match.keys()