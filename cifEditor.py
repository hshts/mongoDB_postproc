import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json

client = pymongo.MongoClient()
db = client.springer


def handle_missingdata(cif_string):
    """
    Handles CIF parsing errors arising from missing elemental data.

    :param cif_string: (str) cif file
    :return: corrected cif string
    """
    cif_lines = json.loads(json.dumps(cif_string)).splitlines()
    noelemdata = True
    for i, line in enumerate(cif_lines):
        if '_sm_atomic_environment_type' in line:
            if '? ?' not in cif_lines[i + 1]:
                noelemdata = False
                break
    if noelemdata:
        print 'Sucessful'
        db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                               {'$addToSet': {'errors': ['cif missing element data']}})
    else:
        print 'Some other error in cif'


def handle_insufficientpowderdata(cif_string):
    """
    Handles CIF parsing errors arising from too few or too many values in data loop for diffraction data.

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
    except:
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
        except:
            print 'UNSUCCESSFUL 2nd attempt (uncommenting all powder diffraction lines)'
            return cif_string_new


def handle_partialocclables(cif_string):
    """
    Handles CIF parsing errors arising from atom labels containing partial occupancy numbers

    :param cif_string: (str) cif file
    :return: corrected cif string
    """
    cif_lines = json.loads(json.dumps(cif_string)).splitlines()
    cif_string_new = ''
    for line in cif_lines:
        if ' + ' in line:
            try:
                matching_list = re.findall(r'\'(.+?)\'', line)
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
                # occupancies[1] = 0.455
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
            except:
                break
        else:
            cif_string_new += line + '\n'
    # print cif_string_new
    try:
        print CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
        return cif_string_new
    except:
        print 'UNSUCCESSFUL - Could not correct partial occupancy numbers (w/o brackets)'


def handle_partialoccbracketlables(cif_string):
    """
    Handles CIF parsing errors arising from atom labels containing partial occupancy numbers and brackets

    :param cif_string: (str) cif file
    :return: corrected cif string
    """
    cif_lines = json.loads(json.dumps(cif_string)).splitlines()
    cif_string_new = ''
    for line in cif_lines:
        if ' + ' in line:
            newline = '#' + line
            cif_string_new += newline + '\n'
            matching_list = re.findall(r'\'(.+?)\'', line)
            try:
                if '<sup>' in matching_list[0]:
                    elemocc_brackets = matching_list[0].split(' + ')
                else:
                    elemocc_brackets = matching_list[0].split('+')
                elemocc_list = []
                for i in elemocc_brackets:
                    elemocc_list.append(re.sub('\([0-9]*\)', '', i.strip()))
                elems = []
                occupancies = []
                for i in range(len(elemocc_list)):
                    occupancies.append('0' + re.findall('\.?\d+', elemocc_list[i].strip())[1])
                    c = re.findall('\D+', elemocc_list[i].strip())
                    elems.append(str(c[1]).replace('<sup>', ''))
            except:
                break
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
        return cif_string_new
    except:
        print 'UNSUCCESSFUL - Could not correct partial occupancy numbers (with brackets)'


if __name__ == '__main__':
    d = 0
    remove_keys = []
    for unparsable_doc in db['unparsable_sds'].find().sort('_id', pymongo.ASCENDING).skip(d):
        if unparsable_doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436']:
            continue
        d += 1
        print '#######'
        print 'On record # {} and key: {}'.format(d, unparsable_doc['key'])
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': unparsable_doc['key']}):
            doc = parsed_doc
        if 'structure' in doc:
            remove_keys.append(doc['key'])
            print doc['structure']
        else:
            try:
                structure = CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
                print structure
            except:
                print 'Error in parsing'
                cif_string_new = None
                # cif_string_new = handle_insufficientpowderdata(doc['cif_string'])
                cif_string_new = handle_partialocclables(doc['cif_string'])
                if cif_string_new is not None:
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                    db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                               {'$rename': {'cif_string':
                                                               'metadata._Springer.cif_string_old'}})
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string':
                    cif_string_new}})
                    remove_keys.append(doc['key'])
                    print 'DONE!'
    print remove_keys
    for key in remove_keys:
        db['unparsable_sds'].remove({'key': key})
##########
'''
                try:
                except:
                    print 'STILL COULD NOT PARSE STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    print(traceback.format_exc())
                print '-----------------------------'
                    print unparsable_sds_removal
'''
