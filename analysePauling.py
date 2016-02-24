import pymongo
from pymatgen import Composition
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from chronograph.chronograph import Chronograph


client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']
# cg = Chronograph(verbosity=2, start_timing=True)

if __name__ == '__main__':
    keys = []
    space_groups = []
    hp = []
    ht = []
    df = pd.DataFrame()
    # cg.start('first section')
    # for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).limit(100):
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).batch_size(75):
        if doc['key'] not in keys:
            try:
                keys.append(doc['key'])
                hp.append('Yes')
                ht.append('No')
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'Value error for doc with key {}'.format(doc['key'])
                print 'Cannot parse {}'.format(doc['metadata']['_Springer']['geninfo']['Space Group'])
                space_groups.append(None)
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'ht', '$options': 'i'}}).batch_size(75):
        if doc['key'] not in keys:
            try:
                keys.append(doc['key'])
                hp.append('No')
                ht.append('Yes')
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'Value error for doc with key {}'.format(doc['key'])
                print 'Cannot parse {}'.format(doc['metadata']['_Springer']['geninfo']['Space Group'])
                space_groups.append(None)
    for doc in coll.find().batch_size(75):
        if doc['key'] not in keys:
            try:
                keys.append(doc['key'])
                hp.append('No')
                ht.append('No')
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'Value error for doc with key {}'.format(doc['key'])
                space_groups.append(None)
    df['key'] = pd.Series(keys)
    df['space group'] = pd.Series(space_groups)
    df['hp'] = pd.Series(hp)
    df['ht'] = pd.Series(ht)
    df['property'] = pd.Series(['1'] * len(keys))
    print df.head()
    sns.set_style('whitegrid')
    # sns.violinplot(x='property', y='space group', hue='hp', data=df, palette='muted', split=True, inner='stick')
    sns.violinplot(x='property', y='space group', hue='ht', data=df, palette='muted', split=True, inner='stick')
    plt.show()
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
