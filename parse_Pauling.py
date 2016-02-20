import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json
from bs4 import BeautifulSoup

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 1112
    x = 0
    unparsable_sds_removal = []
    for unparsable_doc in db['unparsable_sds'].find().sort('_id', pymongo.ASCENDING).skip(d):
        d += 1
        print '#######'
        print 'On record # {}'.format(d)
        for parsed_doc in db['pauling_file_unique_Parse'].find({'key': unparsable_doc['key']}):
            doc = parsed_doc
        try:
            db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                    '$set': {'structure': CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()}},
                                                       upsert=False)
        except:
            print 'Error in parsing doc with key: {}'.format(doc['key'])
            cif_string_new = ''
            try:
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
            except:
                print 'STILL UNPARSABLE!'
                x += 1
                continue
            try:
                db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                db['pauling_file_unique_Parse'].update({'key': doc['key']},
                                                   {'$rename': {'cif_string': 'metadata._Springer.cif_string_old'}})
                db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'cif_string': cif_string_new}})
                unparsable_sds_removal.append(doc['key'])
                print 'SUCCESS!!'
            except:
                print 'THIS IS NOT WORKING!!!'
                print '######'
    print 'FINISHED! Total number of documents not parsed in this round = {}'.format(x)
    print unparsable_sds_removal
    for key in unparsable_sds_removal:
        db['unparsable_sds'].remove({'key': key})
    '''
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
                        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'errors': ['cif missing element data']}})
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
                    # db['unparsable_sds'].insert({'key': doc['key']})
                    continue
                try:
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'cif_string_new': cif_string_new,
                                 'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                    print 'DONE!'
                except:
                    print 'STILL COULD NOT PARSE STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    print(traceback.format_exc())
                    # db['unparsable_sds'].insert({'key': doc['key']})
                    continue
                print '-----------------------------'
        else:
            print 'Structure already parsed for ' + doc['key']
        print '#####################################'
        ##############
        '''
    # print 'FINISHED! Total number of unparsable SD_IDs are: ' + str(db['unparsable_sds'].find().count())
