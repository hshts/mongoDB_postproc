import pymongo
from pymatgen.io.cif import CifParser, CifFile
from pymatgen import Structure


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']
newcoll = db['incorrect_labels']


if __name__ == '__main__':
    d = 0
    remove_keys = []
    for doc in coll.find({'key': 'sd_1903187'}).batch_size(75).sort('_id', pymongo.ASCENDING).skip(d).limit(500):
        d += 1
        print '#########################'
        print 'On record # {} and key {}'.format(d, doc['key'])
        # new_cif_string = fix_incorrectlyparsedstructures_symbols(doc['cif_string'])
        if 'structure' in doc:
            print Structure.from_dict(doc['structure']).composition
            cif = CifFile.from_string(doc['cif_string']).data
            for block in cif:
                if 'standardized' in block:
                    cif_stdblock = cif[block]
                    break
            # print cif_stdblock['_atom_site_label']
            # print cif_stdblock['_atom_site_type_symbol']
            incorrect_symbol = False
            for i, sym in enumerate(cif_stdblock['_atom_site_type_symbol']):
                if sym not in cif_stdblock['_atom_site_label'][i] and ' + ' not in sym:
                    # print sym, cif_stdblock['_atom_site_label'][i]
                    cif_stdblock['_atom_site_label'][i] = sym
                    incorrect_symbol = True
            if incorrect_symbol:
                cif_string_new = ''
                for key in cif:
                    cif_string_new += str(cif[key]) + '\n'
                    cif_string_new += '\n'
                # print cif_string_new
                print CifParser.from_string(cif_string_new).get_structures()[0].composition
                # newcoll.insert({'key': doc['key']})
    print 'DONE!'
    print newcoll.count()

