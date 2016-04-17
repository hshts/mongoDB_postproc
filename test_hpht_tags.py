import unittest
import pymongo
import re


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_tags']


def detect_hp_ht(doc):
    tags = {}
    coll = db['pauling_file_tags']
    title = doc['metadata']['_Springer']['title']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    # Set pressure tags
    if 'p =' in title or ' hp' in title:
        hp_title = True
    else:
        hp_title = None
    if ' hp' in phase:
        hp_phase = True
    else:
        hp_phase = None
    if hp_title == hp_phase:
        if hp_title is not None:
            tags['is_hp'] = hp_title
            # coll.update({'key': doc['key']}, {'$set': {'is_hp': hp_title}})
        else:
            tags['is_hp'] = False
            # coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
    else:
        tags['is_hp'] = True
        # coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
    # Set temperature tags
    if 'temperature' in doc['metadata']['_Springer']['expdetails']:
        exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
        try:
            temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
            temp_exp = float(re.sub('\(.*\)', '', temp_str))
            if temp_exp > 450:
                tags['is_ht'] = True
                # coll.update({'key': doc['key']}, {'$set': {'is_ht': True}})
            elif temp_exp < 350:
                tags['is_ht'] = False
                # coll.update({'key': doc['key']}, {'$set': {'is_ht': False}})
            else:
                tags['is_ht'] = None
                # coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
            # coll.update({'key': doc['key']}, {'$set': {'temperature': temp_exp}})
        except:
            tags['is_ht'] = None
            # coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
    else:
        tags['is_ht'] = None
        # coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
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

    def test13(self):
        for doc in coll.find({'key': 'sd_0541206'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test14(self):
        for doc in coll.find({'key': 'sd_2040724'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test15(self):
        for doc in coll.find({'key': 'sd_0453509'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test16(self):
        for doc in coll.find({'key': 'sd_1521666'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': True})

    def test17(self):
        for doc in coll.find({'key': 'sd_0560825'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    # Ag
    def test18(self):
        for doc in coll.find({'key': 'sd_0552728'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})

    def test19(self):
        for doc in coll.find({'key': 'sd_1822505'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    ###
    def test20(self):
        for doc in coll.find({'key': 'sd_1212949'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test21(self):
        for doc in coll.find({'key': 'sd_0455995'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    # Ca
    def test22(self):
        for doc in coll.find({'key': 'sd_1925112'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test23(self):
        for doc in coll.find({'key': 'sd_1400924'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})

    def test24(self):
        for doc in coll.find({'key': 'sd_1928058'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test25(self):
        for doc in coll.find({'key': 'sd_1932324'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    ###
    def test26(self):
        for doc in coll.find({'key': 'sd_1502611'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test27(self):
        for doc in coll.find({'key': 'sd_1252608'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})

    def test28(self):
        for doc in coll.find({'key': 'sd_1701652'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test29(self):
        for doc in coll.find({'key': 'sd_1310301'}):
            self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})


if __name__ == '__main__':
    unittest.main()
