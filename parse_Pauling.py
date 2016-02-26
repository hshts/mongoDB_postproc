import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json
from bs4 import BeautifulSoup

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    unparsable_sds_removal = []
    for unparsable_doc in db['unparsable_sds'].find({'key': 'sd_1521603'}).sort('_id', pymongo.ASCENDING).skip(d):
        if unparsable_doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436']:
            continue
        d += 1
        print '#######'
        print 'On record # {} and key: {}'.format(d, unparsable_doc['key'])
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': unparsable_doc['key']}):
            doc = parsed_doc
        if 'structure' in doc:
            unparsable_sds_removal.append(doc['key'])
        else:
            try:
                structure = CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
                db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                    '$set': {'structure': CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()}},
                                                       upsert=False)
            except:
                print(traceback.format_exc())
                print 'Error in parsing'
                ##########
                cif_string_new = ''
                for line in (json.loads(json.dumps(doc['cif_string']))).splitlines():
                    if ' + ' in line:
                        print line
                        matching_list = re.findall(r'\'(.+?)\'', line)
                        elemocc = matching_list[0].split('+')
                        print elemocc
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
                print cif_string_new
                try:
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                    db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                               {'$rename': {'cif_string': 'metadata._Springer.cif_string_old'}})
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string': cif_string_new}})
                    unparsable_sds_removal.append(doc['key'])
                    print 'DONE!'
                except:
                    print 'STILL COULD NOT PARSE STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    print(traceback.format_exc())
                print '-----------------------------'
    print unparsable_sds_removal
    for key in unparsable_sds_removal:
        db['unparsable_sds'].remove({'key': key})
        # else:
        #     print 'Structure already parsed for ' + doc['key']
        # print '#####################################'
        ##############
        '''
                cif_string_new = ''
                cif_lines = json.loads(json.dumps(doc['cif_string'])).splitlines()
                powdpattInCif = False
                for lineno, line in enumerate(cif_lines):
                    if '_sm_powderpattern_remark' in line:
                        powdpattInCif = True
                        smlineno = lineno + 1
                        break
                    else:
                        cif_string_new += line + '\n'
                if powdpattInCif:
                    for line in cif_lines[smlineno:]:
                        line_list = line.split()
                        if len(line_list) > 10:
                            cif_string_new += line + '\n'
                        elif 1 < len(line_list) < 11:
                            cif_string_new += '#' + line + '\n'
                try:
                    structure = CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                    db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                           {'$rename': {'cif_string': 'metadata._Springer.cif_string_old'}})
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string': cif_string_new}})
                    print '1st SUCCESS!!'
                except:
                    print '1st NOOO!'
                    print(traceback.format_exc())
                    cif_string_new = ''
                    for x, line in enumerate(cif_lines):
                        if 'loop_' in line:
                            if '_sm_powderpattern' in cif_lines[x + 1]:
                                cif_string_new += '#' + line + '\n'
                                break
                            else:
                                cif_string_new += line + '\n'
                        else:
                            cif_string_new += line + '\n'
                    for line in cif_lines[x + 1:]:
                        cif_string_new += '#' + line + '\n'
                    # print cif_string_new
                    try:
                        structure = CifParser.from_string(cif_string_new).get_structures()[0].as_dict()
                        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                            '$set': {'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                               upsert=False)
                        db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                               {'$rename': {
                                                                   'cif_string': 'metadata._Springer.cif_string_old'}})
                        db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                               {'$set': {'cif_string': cif_string_new}})
                        unparsable_sds_removal.append(doc['key'])
                        print '2nd SUCCESS!!'
                    except:
                        print '2nd NOOO!'
                        print(traceback.format_exc())
                        second_cif_string_new = cif_string_new
                        new_cif_string = ''
                        for line in second_cif_string_new.splitlines():
                            if ' + ' in line:
                                # print line
                                newline = '#' + line
                                new_cif_string += newline + '\n'
                                matching_list = re.findall(r'\'(.+?)\'', line)
                                if '<sup>' in matching_list[0]:
                                    elemocc_brackets = matching_list[0].split(' + ')
                                else:
                                    elemocc_brackets = matching_list[0].split('+')
                                # print elemocc_brackets
                                elemocc_list = []
                                for f in elemocc_brackets:
                                     elemocc_list.append(re.sub('\([0-9]*\)', '', f.strip()))
                                # print elemocc_list
                                elems = []
                                occupancies = []
                                for g in range(len(elemocc_list)):
                                    occupancies.append('0' + re.findall('\.?\d+', elemocc_list[g].strip())[1])
                                    c = re.findall('\D+', elemocc_list[g].strip())
                                    elems.append(c[1])
                                for u, el in enumerate(elems):
                                    if '<sup>' in el:
                                        elems[u] = el.strip('<sup>')
                                # print elems
                                # print occupancies
                                for h in range(len(elems)):
                                    oldline = line
                                    old_elemline = oldline.replace("'" + matching_list[0] + "'", "'" + elems[h] + "'")
                                    new_elemline_list = old_elemline.split()
                                    new_elemline_list[7] = occupancies[h]
                                    new_elemline_list.append('\n')
                                    new_elemline = ' '.join(new_elemline_list)
                                    new_cif_string += new_elemline
                            else:
                                new_cif_string += line + '\n'
                        # print cif_string_new
                        try:
                            structure = CifParser.from_string(new_cif_string).get_structures()[0].as_dict()
                            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                                    '$set': {'structure': structure}},
                                                                       upsert=False)
                            db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                               {'$rename': {'cif_string': 'metadata._Springer.cif_string_old'}})
                            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string': new_cif_string}})
                            unparsable_sds_removal.append(doc['key'])
                            print 'SUCCESS!!'
                        except:
                            print 'THIS IS NOT WORKING!!!'
                            print '######'
    print unparsable_sds_removal
    for key in unparsable_sds_removal:
        db['unparsable_sds'].remove({'key': key})
    ###########
    for doc in db['pauling_file_unique_Parse'].find().skip(d).batch_size(75):
        d += 1
        print 'On record # {}'.format(d)
        ###########
        if 'structure' not in doc:
            try:
                db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                    '$set': {'structure': CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()}},
                                                       upsert=False)
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
                        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'errors': ['cif missing
                        element data']}})
                    else:
                        print 'Some other error in cif'
                        db['unparsable_sds'].insert({'key': doc['key']})
                except Exception as e:
                    print e
                    print 'THIS ERROR SHOULD NOT OCCUR!'
        print '###########'
        #########
        soup = BeautifulSoup(doc['webpage_str'], 'lxml')
        geninfo = soup.find('div', {'id': 'general_information'})
        geninfo_text = geninfo.get_text()
        lines = (line.strip() for line in geninfo_text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        geninfo_text = '\n'.join(chunk for chunk in chunks if chunk)
        geninfo_dict = {}
        geninfo_text_list = geninfo_text.split('\n')
        for i in range(len(geninfo_text_list)):
            if 'General Information' not in geninfo_text_list[i] and 'Substance Summary' not in geninfo_text_list[i]:
                if geninfo_text_list[i].endswith(':'):
                    geninfo_dict[geninfo_text_list[i][:-1]] = geninfo_text_list[i + 1]
        refsoup = soup.find('div', {'id': 'globalReference'}).find('div', 'accordion__bd')
        reference_dict = {'html': refsoup.prettify(),
                          'text': ''.join([(str(item.encode('utf-8'))).strip() for item in refsoup.contents])}
        geninfo_dict['ref'] = reference_dict
        ############
        if 'cif_string_new' in doc:
            db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                   {'$rename': {'cif_string': 'metadata._Springer.cif_string_old'}})
            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$rename': {'cif_string_new': 'cif_string'}})
            print 'MOVING cif_string field DONE!'
        ############
        expdetails = soup.find('div', {'id': 'experimentalDetails'}).find('div', 'accordion__bd')
        exptables = expdetails.findAll('table')
        exptables_dict = {}
        for table in exptables:
            trs = table.findAll('tr')
            expfields = {tr.findAll('td')[0].string.strip(): tr.findAll('td')[1].find('ul').find('li').text.strip()
                         for tr in trs}
            exptables_dict.update(expfields)
        ############
        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
            '$set': {'metadata._Springer.geninfo': geninfo_dict, 'metadata._Springer.expdetails': exptables_dict}},
                                               upsert=False)
        print 'Checking structure for ' + doc['key']
        if 'structure' not in doc:
            print '"structure" key not in this doc'
            try:
                db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                    '$set': {'structure': CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()}},
                                                       upsert=False)
            except:
                print('! Could not parse structure for: {}'.format(doc['key']))
                print(traceback.format_exc())
                print 'Now trying to modify cif string'
                '''
        '''
    # print 'FINISHED! Total number of unparsable SD_IDs are: ' + str(db['unparsable_sds'].find().count())
'''