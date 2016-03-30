import pymongo
from pymatgen.matproj.rest import MPRester

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


if __name__ == '__main__':
    missing_t_field = []
    present_t_field = []
    x = 0
    for doc in db['pauling_file_tags'].find({'structure': {'$exists': True}}).batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        title = doc['metadata']['_Springer']['title']
        if 'T =' in title:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                missing_t_field.append(doc['key'])
            else:
                present_t_field.append(doc['key'])
    print 'Number of records missin temp field = {}'.format(len(missing_t_field))
    print missing_t_field
    print 'Number of records present with temp field = {}'.format(len(present_t_field))