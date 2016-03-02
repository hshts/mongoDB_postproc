import pymongo
from pymatgen import Structure
from pymatgen.analysis.structure_matcher import StructureMatcher

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']

if __name__ == '__main__':
    for doc in coll.find({'key': 'sd_1223808'}):
        struc1 = Structure.from_dict(doc['structure'])
    for doc in coll.find({'key': 'sd_0458111'}):
        struc2 = Structure.from_dict(doc['structure'])
    for doc in coll.find({'key': 'sd_1933177'}):
        struc3 = Structure.from_dict(doc['structure'])
    for doc in coll.find({'key': 'sd_1010018'}):
        struc4 = Structure.from_dict(doc['structure'])
        print struc4
        print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        # print Structure.from_dict(doc['structure'])
        # print doc['structure']
    for doc in coll.find({'key': 'sd_0529813'}):
        struc5 = Structure.from_dict(doc['structure'])
    matcher = StructureMatcher()
    print matcher.fit(struc1, struc2), '8.18, 8.26', matcher.get_rms_dist(struc1, struc2)
    print matcher.fit(struc2, struc3), '8.26 8.25', matcher.get_rms_dist(struc2, struc3)
    print matcher.fit(struc3, struc1), '8.25 8.18', matcher.get_rms_dist(struc3, struc1)
    print matcher.fit(struc1, struc4), matcher.get_rms_dist(struc1, struc4)
    print matcher.fit(struc1, struc5), matcher.get_rms_dist(struc1, struc5)
    print matcher.fit(struc2, struc5), matcher.get_rms_dist(struc2, struc5)

