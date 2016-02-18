import json
from bs4 import BeautifulSoup
import pymongo

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    db['pauling_file_WebParse'].drop()
    for doc in db['pauling_file_unique'].find({'key': 'sd_1213731'}).limit(1):
        db['pauling_file_WebParse'].insert(doc)
    for doc in db['pauling_file_WebParse'].find():
        soup = BeautifulSoup(doc['webpage_str'], 'lxml')
        geninfo = soup.find('div', {'id': 'experimentalDetails'}).find('div', 'accordion__bd')
        tables = geninfo.findAll('table')
        for table in tables:
            trs = table.findAll('tr')
            results = {tr.findAll('td')[0].string.strip(): tr.findAll('td')[1].find('ul').find('li') for tr in trs}
            print results
            # for tr in trs:
            #     tds = tr.findAll('td')
                # print tds[0].string.strip()
                # vals = tds[1].find('ul').find('li')
                # print vals
        # tds = [row.findAll('tr') for row in soup.findAll('table')]
        # print tds
        # results = {td[0].string: td[1].string for td in tds}
        # print results
        # geninfo = soup.find('div', {'id': 'experimentalDetails'}).find('div', 'accordion__bd')
        # text = geninfo.get_text()
        # lines = (line.strip() for line in text.splitlines())
        # chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # text = '\n'.join(chunk for chunk in chunks if chunk)
        # print text
        # geninfo = soup.find('div', {'id': 'general_information'})
        # text = geninfo.get_text()
        # lines = (line.strip() for line in text.splitlines())
        # chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # text = '\n'.join(chunk for chunk in chunks if chunk)
        # data_dict = {}
        # text_list = text.split('\n')
        # for i in range(len(text_list)):
        #     if 'General Information' not in text_list[i] and 'Substance Summary' not in text_list[i]:
        #         if text_list[i].endswith(':'):
        #             data_dict[text_list[i][:-1]] = text_list[i+1]
        # ref = soup.find('div', {'id': 'globalReference'}).find('div', 'accordion__bd')
        # print ''.join([(str(item.encode('utf-8'))).strip() for item in ref.contents])
        # # print ref.prettify()
        # print doc['key']
        # print data_dict

                        # for i in soup.findAll('li', 'data-list__item'):
                        #     print i.contents[0].strip()
                        # print ''.join([(str(item)).strip() for item in geninfo.contents])
                        # buyers = parsed_body.xpath('//li[@class="data-list__item"]/text()')
                        # sellers = parsed_body.xpath('//li[@class="data-list__item"]/span/text()')
                        # print buyers
                        # print sellers
                        # geninfo = soup.findAll('li', 'data-list__item')
                        # print geninfo.contents
                        # ref = soup.find('div', {'id': 'globalReference'}).find('div', 'accordion__bd')
                        # data_dict = {'_globalReference': ''.join([(str(item)).strip() for item in ref.contents]),
                        #              '_entireWebpage': soup.get_text(), '_cif': res.content}
                        # print StructureNL(cif_struct, data=data_dict,
                        #                   authors=['Saurabh Bajaj <sbajaj@lbl.gov>', 'Anubhav Jain <ajain@lbl.gov>'])

                # soup = BeautifulSoup(record['webpage_str'], 'lxml')
                # print soup.find('div', {'id': 'globalReference'}).find('div', 'accordion__bd')
                # print CifParser.from_string(record['cif_string']).get_structures()[0]
                # print pymatgen.Structure.from_dict(record['structure'])
