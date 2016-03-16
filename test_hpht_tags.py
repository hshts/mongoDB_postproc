import unittest
import pymongo


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_tags']


def detect_hp_ht(doc):
    tags = {}
    try:
        phaselabel = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        titlelabel = doc['metadata']['_Springer']['title']
    except KeyError as e:
        print e, 'Key not found'
        return
    if ' hp' in titlelabel:
        hp_titlelabel = 'hp'
    elif 'p =' in titlelabel:
        hp_titlelabel = 'p ='
    else:
        hp_titlelabel = None
    hp_phaselabel = 'hp' if ' hp' in phaselabel else None
    if {hp_titlelabel, hp_phaselabel} == {'hp', None}:
        # print None
        # coll.update({'key': doc['key']}, {'$set': {'is_hp': None}})
        tags['is_hp'] = None
    elif hp_titlelabel == 'hp' and hp_phaselabel == 'hp':
        # print 'HP'
        # coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
        tags['is_hp'] = True
    elif hp_titlelabel == 'p =':
        # print 'HP'
        # coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
        tags['is_hp'] = True
    else:
        # print 'AP'
        # coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
        tags['is_hp'] = False
    ht_titlelabel = ''
    if ' ht' in titlelabel or 'T =' in titlelabel:
        if 'T =' in titlelabel:
            try:
                temp_k = float(re.findall(r'T\s*=\s*(.*)\s*K', titlelabel)[0])
                if temp_k > 400:
                    # print temp_k
                    ht_titlelabel = 'T ='
                elif temp_k <= 400:
                    # print temp_k
                    ht_titlelabel = 'rt'
            except:
                ht_titlelabel = 'rt'
        else:
            ht_titlelabel = 'ht'
    else:
        ht_titlelabel = None
    if ' ht' in phaselabel:
        ht_phaselabel = 'ht'
    elif ' rt' in phaselabel:
        ht_phaselabel = 'rt'
    else:
        ht_phaselabel = None
    if {ht_phaselabel, ht_titlelabel} == {'ht', 'rt'} or {ht_phaselabel, ht_titlelabel} == {'ht', None}:
        # print None
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
        tags['is_ht'] = None
    elif ht_titlelabel == 'T =' and ht_phaselabel == 'rt':
        # print None
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
        tags['is_ht'] = None
    elif ht_titlelabel == 'T =':
        # print 'HT'
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': True}})
        tags['is_ht'] = True
    elif hp_titlelabel == 'ht' and ht_phaselabel == 'ht':
        # print 'HT'
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': True}})
        tags['is_ht'] = True
    else:
        # print 'RT'
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': False}})
        tags['is_ht'] = False
    return tags


class Testtags(unittest.TestCase):
    def test1(self):
        for doc in coll.find({'key': 'sd_0456664'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})


if __name__ == '__main__':
    unittest.main()

