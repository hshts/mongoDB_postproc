from bs4 import BeautifulSoup
import pymongo
import requests
from lxml import html

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    db['pauling_file_WebParse'].drop()
    for doc in db['pauling_file_unique'].find({'key': 'sd_1213731'}).limit(1):
        db['pauling_file_WebParse'].insert(doc)
    for doc in db['pauling_file_WebParse'].find():
        # soup = BeautifulSoup(doc['webpage_str'], 'lxml')
        # geninfo = soup.find('div', {'id': 'experimentalDetails'}).find('div', 'accordion__bd')
        # tables = geninfo.findAll('table')
        # for table in tables:
        #     trs = table.findAll('tr')
        #     results = {tr.findAll('td')[0].string.strip(): tr.findAll('td')[1].find('ul').find('li') for tr in trs}
        #     print results

        # print ''.join([(str(item.encode('utf-8'))).strip() for item in ref.contents])

        # buyers = parsed_body.xpath('//li[@class="data-list__item"]/text()')
        # sellers = parsed_body.xpath('//li[@class="data-list__item"]/span/text()')
        # print buyers
        # print sellers
        # print StructureNL(cif_struct, data=data_dict,
        #                   authors=['Saurabh Bajaj <sbajaj@lbl.gov>', 'Anubhav Jain <ajain@lbl.gov>'])//div[@id="general_information"]

        parsed_body = html.fromstring(doc['webpage_str'])
        for i in parsed_body.xpath('//strong[@class="data-list__item-key"]/text()'):
            field = i.strip()
            if field.endswith(':'):
                print field[:-1]
            else:
                print field

        # for a_link in parsed_body.xpath('//a/@href'):
        #     if '.cif' in a_link:
        #         print a_link
