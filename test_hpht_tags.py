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
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test2(self):
        for doc in coll.find({'key': 'sd_1610906'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test3(self):
        for doc in coll.find({'key': 'sd_1610909'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test4(self):
        for doc in coll.find({'key': 'sd_0456666'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test5(self):
        for doc in coll.find({'key': 'sd_0529813'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test6(self):
        for doc in coll.find({'key': 'sd_0533656'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test7(self):
        for doc in coll.find({'key': 'sd_0456647'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    # ZrO2
    def test8(self):
        for doc in coll.find({'key': 'sd_1252340'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test9(self):
        for doc in coll.find({'key': 'sd_1250760'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test10(self):
        for doc in coll.find({'key': 'sd_1626333'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test11(self):
        for doc in coll.find({'key': 'sd_1211700'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test12(self):
        for doc in coll.find({'key': 'sd_0541122'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test13(self):
        for doc in coll.find({'key': 'sd_0541206'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test14(self):
        for doc in coll.find({'key': 'sd_2040724'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test15(self):
        for doc in coll.find({'key': 'sd_0453509'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test16(self):
        for doc in coll.find({'key': 'sd_1521666'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test17(self):
        for doc in coll.find({'key': 'sd_0560825'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    # Ag
    def test18(self):
        for doc in coll.find({'key': 'sd_0552728'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test19(self):
        for doc in coll.find({'key': 'sd_1822505'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    ###
    def test20(self):
        for doc in coll.find({'key': 'sd_1212949'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test21(self):
        for doc in coll.find({'key': 'sd_0455995'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    # Ca
    def test22(self):
        for doc in coll.find({'key': 'sd_1925112'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test23(self):
        for doc in coll.find({'key': 'sd_1400924'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test24(self):
        for doc in coll.find({'key': 'sd_1928058'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test25(self):
        for doc in coll.find({'key': 'sd_1932324'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    ###
    def test26(self):
        for doc in coll.find({'key': 'sd_1502611'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test27(self):
        for doc in coll.find({'key': 'sd_1252608'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})

    def test28(self):
        for doc in coll.find({'key': 'sd_1701652'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': True})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': False, 'is_ht': None})

    def test29(self):
        for doc in coll.find({'key': 'sd_1310301'}):
            print doc['metadata']['_Springer']['title']
            print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
            if 'temperature' in doc['metadata']['_Springer']['expdetails']:
                print doc['metadata']['_Springer']['expdetails']['temperature']
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': False})
            else:
                self.assertEqual(detect_hp_ht(doc), {'is_hp': True, 'is_ht': None})


if __name__ == '__main__':
    unittest.main()
