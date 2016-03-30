import pymongo
from pymatgen.matproj.rest import MPRester

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


if __name__ == '__main__':
    missing_t_field = []
    httitle_missing_t_field = []
    htphase_missing_t_field = []
    present_t_field = []
    x = 0
    for doc in db['pauling_file_tags'].find({'structure': {'$exists': True}}).batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        title = doc['metadata']['_Springer']['title']
        phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        if 'T =' in title:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                missing_t_field.append(doc['key'])
            else:
                present_t_field.append(doc['key'])
        if ' ht' in title:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                httitle_missing_t_field.append(doc['key'])
        if ' ht' in phase:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                htphase_missing_t_field.append(doc['key'])
    print 'Number of records missing temp field = {}'.format(len(missing_t_field))
    print missing_t_field
    print 'Number of records present with temp field = {}'.format(len(present_t_field))
    print 'Number of HT title records missing temp field = {}'.format(len(httitle_missing_t_field))
    print httitle_missing_t_field
    print 'Number of HT phase records missing temp field = {}'.format(len(htphase_missing_t_field))
    print htphase_missing_t_field
