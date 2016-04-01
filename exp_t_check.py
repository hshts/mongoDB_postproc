import pymongo
from pymatgen.matproj.rest import MPRester
import re

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


if __name__ == '__main__':
    missing_t_field = []
    httitle_missing_t_field = []
    htphase_missing_t_field = []
    no_of_ht_paran = []
    present_t_field = []
    htparan_t_missing = []
    incorrect_ht_title = []
    incorrect_ht_phase = []
    incorrect_ht_paran = []
    y = []
    x = 0
    for doc in db['pauling_file_tags'].find({'structure': {'$exists': True}}).batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        title = doc['metadata']['_Springer']['title']
        phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        if 'T =' in title:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                missing_t_field.append(doc['key'])
            else:
                present_t_field.append(doc['key'])
        if ' ht' in title:
            y.append(doc['key'])
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                httitle_missing_t_field.append(doc['key'])
            else:
                exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
                try:
                    temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
                    temp_exp = float(re.sub('\(.*\)', '', temp_str))
                    if temp_exp < 400:
                        incorrect_ht_title.append(doc['key'])
                except:
                    pass
        if ' ht' in phase:
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                htphase_missing_t_field.append(doc['key'])
            else:
                exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
                try:
                    temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
                    temp_exp = float(re.sub('\(.*\)', '', temp_str))
                    if temp_exp < 400:
                        incorrect_ht_phase.append(doc['key'])
                except:
                    pass
        if len(re.findall(r'\(.* ht.*\)', title)) > 0:
            no_of_ht_paran.append(doc['key'])
            if 'temperature' not in doc['metadata']['_Springer']['expdetails']:
                htparan_t_missing.append(doc['key'])
            else:
                exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
                try:
                    temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
                    temp_exp = float(re.sub('\(.*\)', '', temp_str))
                    if temp_exp < 400:
                        incorrect_ht_paran.append(doc['key'])
                except:
                    pass
    print 'Number of records missing temp field = {}'.format(len(missing_t_field))
    print 'Number of records present with temp field = {}'.format(len(present_t_field))
    print 'Number of HT title records missing temp field = {}'.format(len(httitle_missing_t_field))
    print 'Number of HT title records incorrectly labeled according to Exp T = {}'.format(len(incorrect_ht_title))
    print 'Number of HT phase records missing temp field = {}'.format(len(htphase_missing_t_field))
    print 'Number of HT phase records incorrectly labeled according to Exp T = {}'.format(len(incorrect_ht_phase))
    print 'Number of HT in title paranthesis = {}'.format(len(no_of_ht_paran))
    print 'Number of HT in title paranthesis with Temp field missing = {}'.format(len(htparan_t_missing))
    print 'Number of HT in title paranthesis incorrectly labeled according to Exp T = {}'.format(len(incorrect_ht_paran))
    print 'sd_1926713' in incorrect_ht_paran
    print htparan_t_missing[:10]
    print len(y)
