import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure, Composition

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file']


def insert_states(state, state_keys):
    newcoll = db[state]
    newcoll.drop()
    compositions = []
    cursor = db[state + '_keys'].find()
    keys_df = pd.DataFrame(list(cursor))
    x = 0
    for key in state_keys:
        x += 1
        if x % 1000 == 0:
            print x
        for doc in coll.find({'key': key}):
            if 'structure' in doc:
                comp = Structure.from_dict(doc['structure']).composition.reduced_formula
                # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
                compositions.append(comp)
                try:
                    space_group = int(doc['metadata']['_Springer']['geninfo']['Space Group'])
                except:
                    print '{} Space group parsing error'.format(state)
                    space_group = None
                try:
                    density = float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2])
                except:
                    print '{} Density parsing error'.format(state)
                    density = None
                newcoll.insert({'key': doc['key'], 'composition': str(comp), state: 'Yes', 'space_group': space_group,
                                'density': density, 'property': 1})
    print '{} DONE!'.format(state)
    gs_compositions = []
    y = 0
    for doc in coll.find().batch_size(75).limit(5000):
        y += 1
        if y % 1000 == 0:
            print y
        if 'structure' in doc and doc['key'] not in keys:
            comp = Structure.from_dict(doc['structure']).composition.reduced_formula
            # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
            if comp in compositions:
                print 'MATCH!', doc['key'], comp
                gs_compositions.append(comp)
                try:
                    space_group = int(doc['metadata']['_Springer']['geninfo']['Space Group'])
                except ValueError:
                    print 'GS Space group parsing error'
                    space_group = None
                try:
                    density = float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2])
                except:
                    print 'GS Density parsing error'
                    density = None
                newcoll.insert({'key': doc['key'], 'composition': str(comp), state: 'No', 'space_group': space_group,
                                'density': density, 'property': 1})
    print 'GS DONE!'
    print len(gs_compositions)


def insert_statekeys(state):
    if state == 'lt':
        newcoll = db['ht_keys']
    else:
        newcoll = db[state + '_keys']
    # for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75):
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': state}}).batch_size(75):
        newcoll.insert({'key': doc['key']})
    cursor = newcoll.find()
    df = pd.DataFrame(list(cursor))
    keys_lst = df['key'].tolist()
    x = 0
    y = 0
    if state == 'hp':
        for doc in coll.find({'metadata._Springer.title': {'$regex': 'p ='}}).batch_size(75):
            if doc['key'] not in keys_lst:
                x += 1
                newcoll.insert({'key': doc['key']})
    elif state == 'ht' or state == 'lt':
        for doc in coll.find({'metadata._Springer.title': {'$regex': 'T ='}}).batch_size(75):
            if doc['key'] not in keys_lst:
                y += 1
                newcoll.insert({'key': doc['key']})
    print 'Number of "p =" keys not in hp tags = {}'.format(x)
    print 'Number of "T =" keys not in ht/lt tags = {}'.format(y)


if __name__ == '__main__':
    props = ['hp', 'ht', 'lt']
    for prop in props:
        insert_statekeys(prop)
    # all_statekeys[prop] = pd.DataFrame
    # insert_states('hp')
    # insert_states('ht')
    # hp_cursor = db['hp'].find()
    # hp_df = pd.DataFrame(list(hp_cursor))
    # print hp_df.head(20)
    # ht_cursor = db['ht'].find()
    # ht_df = pd.DataFrame(list(ht_cursor))
    # print ht_df.head(20)
    # sns.set_style('whitegrid')
    # sns.violinplot(x='property', y='space_group', hue='hp', data=hp_df, palette='muted', split=True, inner='stick')
    # plt.show()
    # sns.violinplot(x='property', y='space_group', hue='ht', data=ht_df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='density', hue='hp', data=df, palette='muted', split=True, inner='stick')
    # plt.show()
    # tips = sns.load_dataset("tips")
    # print tips.head()
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)
