import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json
from pymatgen import Structure, Composition

# from pymatgen.analysis.structure_matcher import

client = pymongo.MongoClient()
db = client.springer

db['incorrect_structs'].drop()


if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find().batch_size(75).sort('_id', pymongo.ASCENDING).skip(d):
        d += 1
        print '#######'
        print 'On record # {}'.format(d)
        if 'structure' in doc:
            struct_comp = Structure.from_dict(doc['structure']).composition.reduced_formula
            print 'Key = {}'.format(doc['key'])
            struct = Structure.from_dict(doc['structure'])
            print 'Structure composition = {}'.format(struct_comp)
            try:
                formula_comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula']).get_el_amt_dict()
                print 'Formula composition = {}'.format(formula_comp)
                for element in formula_comp:
                    if element not in struct_comp:
                        print 'NO MATCH! - Element {} not in structure'.format(element)
                        db['incorrect_structs'].insert({'key': doc['key']})
            except Exception as e:
                print e
                continue
    print 'FINISHED!'
    print 'Number of incorrect parsed structures = {}'.format(db['incorrect_structs'].count())