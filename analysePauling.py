import pymongo
from pymatgen import Composition
import pandas as pd


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']

if __name__ == '__main__':
    space_groups = []
    hp_space_groups = []
    df = pd.DataFrame()
    for doc in coll.find().batch_size(75).limit(100):
        try:
            space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
        except ValueError:
            print 'Value error for doc with key {}'.format(doc['key'])
            space_groups.append(None)
    # print space_groups
    df['Space group'] = pd.Series(space_groups)
    for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).limit(100):
        try:
            hp_space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
        except ValueError:
            print 'Value error for doc with key {}'.format(doc['key'])
            hp_space_groups.append(None)
    df['HP Space group'] = pd.Series(hp_space_groups)
    print df
    '''
    print 'Number of docs with "rt" = {}'.format(coll.find({'$text': {'$search': 'rt'}}).count())
    print 'Number of docs with "ht" = {}'.format(coll.find({'$text': {'$search': 'ht'}}).count())
    print coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).count()
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).limit(20):
        print doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).skip(1010).limit(1):
        print doc['key'], doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        try:
            # c = Composition(doc['metadata']['_Springer']['geninfo']['Phase Label(s)'])
            std_formula = ' ' + doc['metadata']['_Springer']['geninfo']['Standard Formula'] + ' '
            # print c
            # for match in coll.find({'$text': {'$match': std_formula}}):
            for match in coll.find({'metadata._Springer.geninfo.Standard Formula': std_formula}):
                print match['key'], match['metadata']['_Springer']['geninfo']['Phase Label(s)'], match['metadata']['_Springer']['geninfo']['Space Group']
                # print doc['key'], doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
        except:
            print 'Error in parsing composition'
    '''
