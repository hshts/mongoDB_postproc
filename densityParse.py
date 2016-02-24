import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find().sort('_id', pymongo.ASCENDING).limit(100):
        density = doc['metadata']['_Springer']['geninfo']['Density']
        print doc['key'], density
        print float(density.split()[2])
