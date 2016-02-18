import traceback
import requests
from lxml import html
from pymatgen.io.cif import CifParser
from matminer.pyCookieCheat import chrome_cookies
import pymongo
import time

if __name__ == '__main__':
    starting_page = 1
    sleep_time = 0.01
    page_sleep_time = 0.05

    print("Connecting to database...")
    client = pymongo.MongoClient()
    db = client['springer']

    unique_sd = []

    print("Getting user token...")
    sim_user_token = chrome_cookies('http://materials.springer.com')['sim-user-token']

    print("Starting data collection")
    for page_no in range(starting_page, 11252):
        print("Starting page: {}".format(page_no))
        time.sleep(page_sleep_time)
        url = 'http://materials.springer.com/search?searchTerm=&pageNumber={}&propertyFacet=' \
              'crystal%20structure&datasourceFacet=sm_isp&substanceId='.format(page_no)
        result_page = requests.get(url)
        parsed_resbody = html.fromstring(result_page.content)
        for link in parsed_resbody.xpath('//a/@href'):
            if 'sd_' in link:
                sd_id = link[-10:]
                # unique_sd.append(sd_id)
                db['springer_sds'].insert({'key': sd_id, 'page_no': page_no})

    print db['springer_sds'].find().count()
    x = db['pauling_file_unique'].distinct('key')
    print len(x)
    y = db['springer_sds'].distinct('key')
    print len(y)
    diff = list(set(y) - set(x))
    print diff
    print len(diff)

    print("FINISHED!")
