import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']
newcoll = db['analysePauling']

if __name__ == '__main__':
    hp_keys = []
    gs_compositions = []
    hp_compositions = []
    x = 0
    # for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).limit(100):
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp'}}).batch_size(75).limit(1000):
        x += 1
        if x % 1000 == 0:
            print x
        if 'structure' in doc:
            hp_keys.append(doc['key'])
            comp = Structure.from_dict(doc['structure']).composition
            hp_compositions.append(comp)
            try:
                space_group = int(doc['metadata']['_Springer']['geninfo']['Space Group'])
            except:
                print 'HP Space group parsing error'
                space_group = None
            try:
                density = float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2])
            except:
                print 'HP Density parsing error'
                density = None
            newcoll.insert(
                {'key': doc['key'], 'composition': str(comp), 'hp': 'Yes', 'ht': 'No', 'space_group': space_group,
                 'density': density, 'property': 1})
    print 'HP DONE!'
    print len(hp_keys)
    z = 0
    for doc in coll.find().batch_size(75).limit(5000):
        z += 1
        if z % 1000 == 0:
            print z
        if 'structure' in doc and doc['key'] not in hp_keys:
            comp = Structure.from_dict(doc['structure']).composition
            if comp in hp_compositions:
                print 'MATCH!', doc['key'], comp
                gs_compositions.append(comp)
                try:
                    space_group = int(doc['metadata']['_Springer']['geninfo']['Space Group'])
                except ValueError:
                    print 'HT Space group parsing error'
                    space_group = None
                try:
                    density = float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2])
                except:
                    print 'HT Density parsing error'
                    density = None
                newcoll.insert(
                    {'key': doc['key'], 'composition': str(comp), 'hp': 'No', 'ht': 'No', 'space_group': space_group,
                     'density': density, 'property': 1})
    print 'GS DONE!'
    print len(gs_compositions)
    print newcoll
    cursor = newcoll.find()
    df = pd.DataFrame(list(cursor))
    print df.head(20)
    # df['key'] = pd.Series(keys)
    # df['space group'] = pd.Series(space_groups)
    # df['hp'] = pd.Series(hp)
    # df['ht'] = pd.Series(ht)
    # df['density'] = pd.Series(density)
    # df['property'] = pd.Series(['1'] * len(hp))
    # print df.head()
    # print df.shape
    sns.set_style('whitegrid')
    sns.violinplot(x='property', y='space_group', hue='hp', data=df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='space group', hue='ht', data=df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='density', hue='hp', data=df, palette='muted', split=True, inner='stick')
    plt.show()
    # tips = sns.load_dataset("tips")
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)
    # print tips.head()
