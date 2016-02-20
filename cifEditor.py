import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find({'key': 'sd_0540486'}):
        try:
            print doc['cif_string']
            print CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
        except:
            print 'Error in parsing..'
            try:
                lines = (json.loads(json.dumps(doc['cif_string']))).splitlines()
                noElemData = True
                for i, line in enumerate(lines):
                    if '_sm_atomic_environment_type' in line:
                        print i, line
                        print lines[i+1]
                        if '? ?' not in lines[i+1]:
                            noElemData = False
                            break
                if noElemData:
                    print 'THIS IS IT!'
                    # db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'errors': ['cif missing element data']}})
                else:
                    print 'Some other error in cif'
                    # db['unparsable_sds'].insert({'key': doc['key']})
            except Exception as e:
                print e
    '''
    for doc in db['pauling_file_StructParse'].find().batch_size(75):
        d += 1
        print 'On record # {}'.format(d)
        print 'Checking structure for ' + doc['key']
        if 'structure' not in doc:
            print '"structure" key not in this doc'
            try:
                doc['structure'] = CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
            except:
                print('! Could not parse structure for: {}'.format(doc['key']))
                print(traceback.format_exc())
                print 'Now trying to modify cif string'
                cif_string_new = ''
                try:
                    for line in (json.loads(json.dumps(doc['cif_string']))).splitlines():
                        if ' + ' in line:
                            matching_list = re.findall(r'\'(.+?)\'', line)
                            elemocc = matching_list[0].split('+')
                            elems = []
                            occupancies = []
                            for i in range(len(elemocc)):
                                occupancies.append('0' + re.findall('\.?\d+', elemocc[i].strip())[1])
                                c = re.findall('\D+', elemocc[i].strip())
                                elems.append(c[1])
                            newline = '#' + line
                            cif_string_new += newline + '\n'
                            for i in range(len(elems)):
                                oldline = line
                                old_elemline = oldline.replace("'" + matching_list[0] + "'", "'" + elems[i] + "'")
                                new_elemline_list = old_elemline.split()
                                new_elemline_list[7] = occupancies[i]
                                new_elemline_list.append('\n')
                                new_elemline = ' '.join(new_elemline_list)
                                cif_string_new += new_elemline
                        else:
                            cif_string_new += line + '\n'
                except:
                    print 'UNABLE TO PARSE THIS STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    db['unparsable_sds'].insert({'key': doc['key']})
                    continue
                try:
                    db['pauling_file_StructParse'].update({'key': doc['key']}, {
                        '$set': {'cif_string_new': cif_string_new,
                                 'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                          upsert=False)
                    print 'DONE!'
                except:
                    print 'STILL COULD NOT PARSE STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    print(traceback.format_exc())
                    db['unparsable_sds'].insert({'key': doc['key']})
                    continue
                print '#####################################'
        else:
            print 'Structure already parsed for ' + doc['key']
            print doc['structure']
            print '#####################################'
    print 'Total number of unparsable SD_IDs are: ' + str(db['unparsable_sds'].find().count())
    '''