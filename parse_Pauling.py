import pymongo
from bs4 import BeautifulSoup

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find().sort('_id', pymongo.ASCENDING).batch_size(75).skip(d):
        if doc['key'] in ['sd_1301665', 'sd_0456987']:
            continue
        d += 1
        print '#######'
        print 'On record # {} and key: {}'.format(d, doc['key'])
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
