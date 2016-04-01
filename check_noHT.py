import pymongo
from pymatgen.matproj.rest import MPRester
import re

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


if __name__ == '__main__':
    ids_to_check = []
    x = 0
    for doc in db['pauling_file_tags'].find({'structure': {'$exists': True}}).batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        title = doc['metadata']['_Springer']['title']
        phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        if ' ht' not in title and 'T =' not in title and ' ht' not in phase:
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
                try:
                    temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
                    temp_exp = float(re.sub('\(.*\)', '', temp_str))
                    if temp_exp > 450:
                        ids_to_check.append(doc['key'])
                except:
                    pass
    print 'Number of ids to check = {}'.format(len(ids_to_check))
    print ids_to_check[:15]


