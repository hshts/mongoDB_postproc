import unittest
import pymongo
import re


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
    # PbTe
    def test1(self):
        for doc in coll.find({'key': 'sd_0456664'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test2(self):
        for doc in coll.find({'key': 'sd_1610906'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test3(self):
        for doc in coll.find({'key': 'sd_1610909'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test4(self):
        for doc in coll.find({'key': 'sd_0456666'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test5(self):
        for doc in coll.find({'key': 'sd_0529813'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test6(self):
        for doc in coll.find({'key': 'sd_0533656'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test7(self):
        for doc in coll.find({'key': 'sd_0456647'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    # ZrO2
    def test8(self):
        for doc in coll.find({'key': 'sd_1252340'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test9(self):
        for doc in coll.find({'key': 'sd_1250760'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test10(self):
        for doc in coll.find({'key': 'sd_1626333'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test11(self):
        for doc in coll.find({'key': 'sd_1211700'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test12(self):
        for doc in coll.find({'key': 'sd_0541122'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test121(self):
        for doc in coll.find({'key': 'sd_0541206'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test122(self):
        for doc in coll.find({'key': 'sd_2040724'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test13(self):
        for doc in coll.find({'key': 'sd_0453509'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test14(self):
        for doc in coll.find({'key': 'sd_1521666'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': True})

    def test15(self):
        for doc in coll.find({'key': 'sd_0560825'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    # Ag
    def test16(self):
        for doc in coll.find({'key': 'sd_0552728'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test17(self):
        for doc in coll.find({'key': 'sd_1822505'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    ###
    def test18(self):
        for doc in coll.find({'key': 'sd_1212949'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test19(self):
        for doc in coll.find({'key': 'sd_0455995'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    # Ca
    def test20(self):
        for doc in coll.find({'key': 'sd_1925112'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test21(self):
        for doc in coll.find({'key': 'sd_1400924'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test22(self):
        for doc in coll.find({'key': 'sd_1928058'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test23(self):
        for doc in coll.find({'key': 'sd_1932324'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    ###
    def test24(self):
        for doc in coll.find({'key': 'sd_1502611'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test25(self):
        for doc in coll.find({'key': 'sd_1252608'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test26(self):
        for doc in coll.find({'key': 'sd_1701652'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test27(self):
        for doc in coll.find({'key': 'sd_1310301'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})


if __name__ == '__main__':
    unittest.main()
