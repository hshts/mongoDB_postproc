import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure, Composition

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file']


def insert_states(state):
    st = state
    newcoll = db[state]
    newcoll.drop()
    keys = []
    compositions = []
    x = 0
    # for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).limit(100):
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': st}}).batch_size(75).limit(1000):
        x += 1
        if x % 1000 == 0:
            print x
        if 'structure' in doc:
            keys.append(doc['key'])
            comp = Structure.from_dict(doc['structure']).composition.reduced_formula
                # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
            compositions.append(comp)
            try:
                space_group = int(doc['metadata']['_Springer']['geninfo']['Space Group'])
            except:
                print '{} Space group parsing error'.format(st)
                space_group = None
            try:
                density = float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2])
            except:
                print '{} Density parsing error'.format(st)
                density = None
            newcoll.insert(
                {'key': doc['key'], 'composition': str(comp), st: 'Yes', 'space_group': space_group, 'density': density,
                 'property': 1})
    print '{} DONE!'.format(st)
    print len(keys)
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
                    newcoll.insert({'key': doc['key'], 'composition': str(comp), st: 'No', 'space_group': space_group,
                                    'density': density, 'property': 1})
    print 'GS DONE!'
    print len(gs_compositions)


if __name__ == '__main__':
    insert_states('hp')
    # insert_states('ht')
    hp_cursor = db['hp'].find()
    hp_df = pd.DataFrame(list(hp_cursor))
    print hp_df.head(20)
    # ht_cursor = db['ht'].find()
    # ht_df = pd.DataFrame(list(ht_cursor))
    # print ht_df.head(20)
    sns.set_style('whitegrid')
    sns.violinplot(x='property', y='space_group', hue='hp', data=hp_df, palette='muted', split=True, inner='stick')
    plt.show()
    # sns.violinplot(x='property', y='space_group', hue='ht', data=ht_df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='density', hue='hp', data=df, palette='muted', split=True, inner='stick')
    # plt.show()
    # tips = sns.load_dataset("tips")
    # print tips.head()
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)
