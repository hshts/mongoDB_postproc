import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

client = pymongo.MongoClient()
db = client.springer
coll = db['pauling_file_unique_Parse']

if __name__ == '__main__':
    keys = []
    space_groups = []
    hp = []
    ht = []
    density = []
    df = pd.DataFrame()
    # for doc in coll.find({'$text': {'$search': 'hp'}}).batch_size(75).limit(100):
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'hp', '$options': 'i'}}).batch_size(
            75):
        if doc['key'] not in keys:
            keys.append(doc['key'])
            hp.append('Yes')
            ht.append('No')
            try:
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'HP Space group parsing error for key {} and space group {}'.format(doc['key'],
                                                                                          doc['metadata']['_Springer'][
                                                                                              'geninfo']['Space Group'])
                space_groups.append(None)
            try:
                density.append(float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2]))
            except:
                print 'HP Density parsing error for key {} and space group {}'.format(doc['key'],
                                                                                      doc['metadata']['_Springer'][
                                                                                          'geninfo']['Space Group'])
                density.append(None)
    for doc in coll.find({'metadata._Springer.geninfo.Phase Label(s)': {'$regex': 'ht', '$options': 'i'}}).batch_size(
            75):
        if doc['key'] not in keys:
            keys.append(doc['key'])
            hp.append('No')
            ht.append('Yes')
            try:
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'HT Space group parsing error for key {} and space group {}'.format(doc['key'],
                                                                                          doc['metadata']['_Springer'][
                                                                                              'geninfo']['Space Group'])
                space_groups.append(None)
            try:
                density.append(float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2]))
            except:
                print 'HT Density parsing error for key {} and space group {}'.format(doc['key'],
                                                                                      doc['metadata']['_Springer'][
                                                                                          'geninfo']['Space Group'])
                density.append(None)
    for doc in coll.find().batch_size(75):
        if doc['key'] not in keys:
            keys.append(doc['key'])
            hp.append('No')
            ht.append('No')
            try:
                space_groups.append(int(doc['metadata']['_Springer']['geninfo']['Space Group']))
            except ValueError:
                print 'Space group parsing error for key {} and space group {}'.format(doc['key'],
                                                                                       doc['metadata']['_Springer'][
                                                                                           'geninfo']['Space Group'])
                space_groups.append(None)
            try:
                density.append(float(doc['metadata']['_Springer']['geninfo']['Density'].split()[2]))
            except:
                print 'Density parsing error for key {} and space group {}'.format(doc['key'],
                                                                                   doc['metadata']['_Springer'][
                                                                                       'geninfo']['Space Group'])
                density.append(None)
    df['key'] = pd.Series(keys)
    df['space group'] = pd.Series(space_groups)
    df['hp'] = pd.Series(hp)
    df['ht'] = pd.Series(ht)
    df['density'] = pd.Series(density)
    df['property'] = pd.Series(['1'] * len(keys))
    print df.head()
    tips = sns.load_dataset("tips")
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)
    print tips.head()
    sns.set_style('whitegrid')
    sns.violinplot(x='property', y='space group', hue='hp', data=df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='space group', hue='ht', data=df, palette='muted', split=True, inner='stick')
    # sns.violinplot(x='property', y='density', hue='hp', data=df, palette='muted', split=True, inner='stick')
    plt.show()