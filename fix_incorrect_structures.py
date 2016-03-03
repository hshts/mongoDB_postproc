import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json
from pymatgen import Structure, Element, DummySpecie

client = pymongo.MongoClient()
db = client.springer


def fix_incorrectlyparsedstructures(cif_string):
    """
    Fixes already parsed CIF files with structures that have sites with partial occupancies, but unlike other CIF
    files with partial occupancies, have atomic labels with atomic symbols separated by commas, which makes them
    parsable by CifParser but only uses the first element in the structure, ignoring other elements on this site.

    :param cif_string: (str) cif file
    :return: corrected cif string
    """
    cif_lines = json.loads(json.dumps(cif_string)).splitlines()
    cif_string_new = ''
    # for line in cif_lines:

if __name__ == '__main__':
    d = 0
    for incorrect_doc in db['incorrect_structs'].find({'key': 'sd_1011081'}).batch_size(75).sort('_id', pymongo.ASCENDING).skip(d):
        d += 1
        print '#######'
        print 'On record # {} and key {}'.format(d, incorrect_doc['key'])
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': incorrect_doc['key']}):
            doc = parsed_doc
        print Structure.from_dict(doc['structure'])

