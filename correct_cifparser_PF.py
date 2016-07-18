import pymongo
from pymatgen.io.cif import CifParser
from pymatgen import Structure
from compositionMatcher import get_meta_from_structure

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    x = 0
    for doc in db['pauling_file'].find({'structure': {'$exists': True}}).batch_size(75):
        if doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436', 'sd_1500010']:
            continue
        x += 1
        if x % 1000 == 0:
            print x
        try:
            new_struct = CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
        except Exception as e:
            print e
            continue
        db['pauling_file'].update({'key': doc['key']}, {'$set': {'structure': new_struct}})
    for doc in db['pauling_file'].find({'structure': {'$exists': True}}).batch_size(75):
        if doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436', 'sd_1500010']:
            continue
        x += 1
        if x % 1000 == 0:
            print x
        meta_dict = get_meta_from_structure(Structure.from_dict(doc['structure']))
        db['pauling_file'].update({'key': doc['key']}, {'$set': {'metadata._structure': meta_dict}})
