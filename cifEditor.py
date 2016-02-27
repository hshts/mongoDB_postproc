import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json

client = pymongo.MongoClient()
db = client.springer


def handle_insufficientpowderdata(cif_string):
    """
    Handles CIF errors arising from too few or too many values in data loop for diffraction data.

    :param cif_string: (str) cif file
    :return: corrected cif string
    """
    smlineno = 0
    looplineno = 0
    cif_lines = json.loads(json.dumps(cif_string)).splitlines()
    powderdataline = False
    loopdataline = False
    cif_string_new = ''
    for lineno, line in enumerate(cif_lines):
        if '_sm_powderpattern_remark' in line:
            powderdataline = True
            smlineno = lineno + 1
            break
        else:
            cif_string_new += line + '\n'
    if powderdataline:
        for line in cif_lines[smlineno:]:
            line_list = line.split()
            if len(line_list) > 10:
                cif_string_new += line + '\n'
            elif 1 < len(line_list) < 11:
                cif_string_new += '#' + line + '\n'
    try:
        print CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
        return cif_string_new
    except AssertionError:
        print 'UNSUCCESSFUL 1st attempt (uncommenting lines with insufficient or too many values for powder pattern)'
        cif_string_new = ''
        for lineno, line in enumerate(cif_lines):
            if 'loop_' in line:
                if '_sm_powderpattern' in cif_lines[lineno + 1]:
                    cif_string_new += '#' + line + '\n'
                    loopdataline = True
                    looplineno = lineno + 1
                    break
                else:
                    cif_string_new += line + '\n'
            else:
                cif_string_new += line + '\n'
        if loopdataline:
            for line in cif_lines[looplineno:]:
                cif_string_new += '#' + line + '\n'
        try:
            print CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
            return cif_string_new
        except AssertionError:
            print 'UNSUCCESSFUL 2nd attempt (uncommenting all lines with insufficient or too many values for powder ' \
                  'pattern)'


if __name__ == '__main__':
    d = 0
    for unparsable_doc in db['unparsable_sds'].find({'key': 'sd_1704003'}).sort('_id', pymongo.ASCENDING).limit(10):
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': unparsable_doc['key']}):
            doc = parsed_doc
        try:
            print CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
        except AssertionError:
            print 'Error in parsing doc with key: {}'.format(doc['key'])
            print(traceback.format_exc())

            '''
            for line in (json.loads(json.dumps(doc['cif_string']))).splitlines():
                if ' + ' in line:
                    # print line
                    newline = '#' + line
                    cif_string_new += newline + '\n'
                    matching_list = re.findall(r'\'(.+?)\'', line)
                    elemocc_brackets = matching_list[0].split('+')
                    # print elemocc_brackets
                    elemocc_list = []
                    for i in elemocc_brackets:
                         elemocc_list.append(re.sub('\([0-9]\)', '', i.strip()))
                    elems = []
                    occupancies = []
                    for i in range(len(elemocc_list)):
                        occupancies.append('0' + re.findall('\.?\d+', elemocc_list[i].strip())[1])
                        c = re.findall('\D+', elemocc_list[i].strip())
                        elems.append(c[1])
                    # print elems
                    # print occupancies
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
            # print cif_string_new
            try:
                print CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
            except:
                print 'THIS IS NOT WORKING!!!'
    ########
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
                    # db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'errors': ['cif missing
                    element data']}})
                else:
                    print 'Some other error in cif'
                    # db['unparsable_sds'].insert({'key': doc['key']})
            except Exception as e:
                print e
    #######
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
