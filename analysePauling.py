import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure, Composition

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file']


def insert_states(state, incl_keys, excl_keys):
    newcoll = db[state]
    newcoll.drop()
    state_compositions = {}
    x = 0
    for key in incl_keys:
        x += 1
        if x % 1000 == 0:
            print x
        for doc in coll.find({'key': key}):
            if 'structure' in doc:
                comp = Structure.from_dict(doc['structure']).composition.reduced_formula
                # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
                state_compositions[comp] = key
    print 'Collecting composition-key pairs for {} state done'.format(state)
    gs_compositions = []
    y = 0
    for doc in coll.find().batch_size(75).limit(5000):
        y += 1
        if y % 1000 == 0:
            print y
        if 'structure' in doc and doc['key'] not in excl_keys:
            comp = Structure.from_dict(doc['structure']).composition.reduced_formula
            # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
            if comp in state_compositions:
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
    for st_comp in state_compositions:
        if st_comp in gs_compositions:
            common_key = state_compositions[st_comp]
            for doc in coll.find({'key': common_key}):
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
                newcoll.insert({'key': doc['key'], 'composition': str(st_comp), state: 'Yes', 'space_group': space_group,
                                'density': density, 'property': 1})
    print '{} DONE!'.format(state)


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
    all_keys = {}
    props = ['hp', 'ht']
    for prop in props:
        col = prop + '_keys'
        cursor = db[col].find()
        df = pd.DataFrame(list(cursor))
        all_keys[prop] = df['key'].tolist()
    for prop in all_keys:
        in_keys = all_keys[prop]
        out_keys = []
        for other_key in all_keys:
            if other_key != prop:
                out_keys += all_keys[other_key]
        print prop, len(in_keys), len(out_keys)
        insert_states(prop, in_keys, out_keys)
        cursor = db[prop].find()
        df = pd.DataFrame(list(cursor))
        print df.head(20)
        # sns.set_style('whitegrid')
        # sns.violinplot(x='property', y='space_group', hue=prop, data=df, palette='muted', split=True, inner='stick')
        # plt.show()
    # sns.violinplot(x='property', y='density', hue='hp', data=df, palette='muted', split=True, inner='stick')
    # plt.show()
    # tips = sns.load_dataset("tips")
    # print tips.head()
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)
'''
    props = ['hp', 'ht']
    for prop in props:
        cursor = db[prop].find()
        df = pd.DataFrame(list(cursor))
        print df.groupby(['composition', prop]).size()
        df_mean = df.groupby(['composition', prop], as_index=False).mean()
        print df_mean
        sns.set_style('whitegrid')
        sns.violinplot(x='property', y='space_group', hue=prop, data=df_mean, palette='muted', split=True, inner='stick')
        plt.show()
        # x = 0
        # for doc in db[prop].find().batch_size(75):
        #     if doc[prop] == 'Yes':
        #         if db[prop].find({'composition': doc['composition'], prop: 'No'}).count() > 0:
        #             x += 1
        # print 'No. of docs with both Yes/No present = {}'.format(x)
'''


