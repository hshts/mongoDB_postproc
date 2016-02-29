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
    matcher = StructureMatcher()
    print matcher.fit(struc1, struc2)
    print matcher.get_rms_dist(struc1, struc2)

