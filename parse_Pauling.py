import pymongo
from pymatgen.io.cif import CifParser
import re
import traceback
import json
from bs4 import BeautifulSoup

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    # db['pauling_file_unique_Parse'].drop()
    # db['unparsable_sds'].drop()
    # for doc in db['pauling_file_unique'].find().limit(1000):
    #     db['pauling_file_unique_Parse'].insert(doc)
    # db['pauling_file_unique'].aggregate([{'$out': 'pauling_file_unique_Parse'}])
    # db['pauling_file_unique_Parse'].ensure_index("key", unique=True)
    d = 0
    for doc in db['pauling_file_unique_Parse'].find().batch_size(75):
        d += 1
        print 'On record # {}'.format(d)
        ###########
        soup = BeautifulSoup(doc['webpage_str'], 'lxml')
        '''
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
        '''
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
        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {'$set': {'metadata._Springer.expdetails': exptables_dict}}, upsert=False)
        '''
        db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
            '$set': {'metadata._Springer.geninfo': geninfo_dict, 'metadata._Springer.expdetails': exptables_dict}},
                                               upsert=False)
        '''
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
                    db['pauling_file_unique_Parse'].update({'key': doc['key']}, {
                        '$set': {'cif_string_new': cif_string_new,
                                 'structure': CifParser.from_string(cif_string_new).get_structures()[0].as_dict()}},
                                                           upsert=False)
                    print 'DONE!'
                except:
                    print 'STILL COULD NOT PARSE STRUCTURE. ADDING TO LIST OF UNPARSABLE_SDS'
                    print(traceback.format_exc())
                    db['unparsable_sds'].insert({'key': doc['key']})
                    continue
                print '-----------------------------'
        else:
            print 'Structure already parsed for ' + doc['key']
        print '#####################################'
        ##############
    print 'FINISHED! Total number of unparsable SD_IDs are: ' + str(db['unparsable_sds'].find().count())