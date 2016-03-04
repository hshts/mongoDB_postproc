import pymongo
from pymatgen.io.cif import CifParser
import re
import json
from pymatgen import Composition, Structure

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
    for line in cif_lines:
        if ' + ' in line:
            try:
                newline = '#' + line
                cif_string_new += newline + '\n'
                matching_list = re.findall(r"\'(.+?)\'", line)
                elemocc = matching_list[0].split('+')
                elems = []
                occupancies = []
                for i in range(len(elemocc)):
                    occupancies.append('0' + re.findall('\.?\d+', elemocc[i].strip())[1])
                    c = re.findall('\D+', elemocc[i].strip())
                    elems.append(c[1])
                occ_sum = 0
                for i in range(len(occupancies)):
                    occ_sum += float(occupancies[i])
                if occ_sum != 1:
                    sum_exc_last = 0
                    for i in range(len(occupancies) - 1):
                        sum_exc_last += float(occupancies[i])
                    occupancies[:-1] = str(1 - sum_exc_last)
                for i in range(len(elems)):
                    oldline = line
                    old_elemline = oldline.replace("'" + matching_list[0] + "'", "'" + elems[i] + "'")
                    new_elemline_list = old_elemline.split()
                    new_elemline_list[0] = elems[i]
                    new_elemline_list[7] = occupancies[i]
                    new_elemline_list.append('\n')
                    new_elemline = ' '.join(new_elemline_list)
                    cif_string_new += new_elemline
            except:
                break
        else:
            cif_string_new += line + '\n'
    return cif_string_new


if __name__ == '__main__':
    d = 0
    remove_keys = []
    for incorrect_doc in db['incorrect_structs'].find().batch_size(75).sort('_id', pymongo.ASCENDING).skip(d).limit(10):
        d += 1
        print '#######'
        print 'On record # {} and key {}'.format(d, incorrect_doc['key'])
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': incorrect_doc['key']}):
            doc = parsed_doc
        if 'cif_string_old' in doc['metadata']['_Springer']:
            new_cif_string = fix_incorrectlyparsedstructures(doc['metadata']['_Springer']['cif_string_old'])
            try:
                struct_comp = CifParser.from_string(new_cif_string).get_structures()[0].composition.reduced_formula
            except Exception as e:
                print e
                print 'ERROR parsing NEW structure!'
                continue
            print 'Structure composition = {}'.format(struct_comp)
            try:
                formula_comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula']).get_el_amt_dict()
            except Exception as e:
                print e
                continue
            print 'Formula composition = {}'.format(formula_comp)
            missing_element = False
            for element in formula_comp:
                if element not in struct_comp:
                    missing_element = True
                    break
            if missing_element:
                print 'NO MATCH! - Element {} not in structure'.format(element)
                continue
            print 'SUCCESS'
            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'structure': CifParser.from_string(
            new_cif_string).get_structures()[0].as_dict()}}, upsert=False)
            # db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$rename': {'cif_string':
            # 'metadata._Springer.cif_string_old'}})
            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string': new_cif_string}})
            remove_keys.append(doc['key'])
    print 'FINISHED!'
    print remove_keys
    print len(remove_keys)
    for key in remove_keys:
        db['incorrect_structs'].remove({'key': key})
