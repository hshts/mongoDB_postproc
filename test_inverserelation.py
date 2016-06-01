import pymongo
import pandas as pd
from pymatgen import Structure, Composition
import matplotlib.pyplot as plt

client = pymongo.MongoClient()
db = client.springer


def create_df():
    newcoll = db['abc3']
    newcoll.drop()
    x = 0
    for doc in db['pauling_file_min_tags'].find().batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        if doc['metadata']['_structure']['anonymized_formula'] == 'ABC3' and doc['is_ht'] in [True,False] \
                and 'TiO3' in doc['metadata']['_structure']['reduced_cell_formula']:
            newcoll.insert(doc)
    cursor = newcoll.find()
    df = pd.DataFrame(list(cursor))
    for i, row in df.iterrows():
        df.set_value(i, 'reduced_cell_formula', row['metadata']['_structure']['reduced_cell_formula'])
        try:
            df.set_value(i, 'space_group', int(row['metadata']['_Springer']['geninfo']['Space Group']))
        except:
            df.set_value(i, 'space_group', None)
        try:
            df.set_value(i, 'density', float(row['metadata']['_Springer']['geninfo']['Density'].split()[2]))
        except IndexError as e:
            df.set_value(i, 'density', None)
        structure = Structure.from_dict(row['structure'])
        num_density = (structure.num_sites/structure.volume)
        no_of_atoms = Composition(structure.composition).num_atoms
        num_vol = (structure.volume/no_of_atoms)
        df.set_value(i, 'number_density', num_density)
        df.set_value(i, 'number_volume', num_vol)
        if row['metadata']['_structure']['is_ordered']:
            df.set_value(i, 'is_ordered', 1)
        else:
            df.set_value(i, 'is_ordered', 0)
    df.to_pickle('abc3.pkl')


if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    # create_df()
    df = pd.read_pickle('abc3.pkl')
    df_groupby = df.groupby(['reduced_cell_formula', 'is_ht'], as_index=False).mean()
    # print df_groupby['reduced_cell_formula'].unique()
    print df_groupby.loc[(df_groupby['space_group'] > 60) & (df_groupby['space_group'] < 70) & (df_groupby['is_ht'] == True) & (df_groupby['is_ht_dataset'] == 0)]
    # fig, ax = plt.subplots()
    for i, row in df_groupby.iterrows():
        if row['is_ht']:
            df.loc[i, 'col_dataset'] = 'r'
        elif not row['is_ht']:
            df.loc[i, 'col_dataset'] = 'k'
        # if row['is_ht_dataset'] == 1:
        #     df.loc
        # if row['is_ht_dataset'] == 0:
        #     ax.text(row['number_volume'], row['space_group'], row['reduced_cell_formula'], fontsize=8)
    color_col = df['col_dataset'].dropna()
    df_new = df_groupby.loc[df_groupby['is_ht_dataset'] == 1]
    # df_groupby.plot(x='number_volume', y='space_group', kind='scatter', facecolors='none', c=color_col, edgecolor=color_col,  s=80)
    # df_groupby.plot(x='number_volume', y='space_group', kind='scatter', facecolors='none', c=color_col, edgecolor=color_col,  s=80, ax=ax)
    df_new.plot(x='number_volume', y='space_group', kind='scatter', facecolors='none', c=color_col, edgecolor=color_col,  s=80)
    plt.xlim(5,25)
    plt.ylim(0,230)
    plt.xlabel(r'Volume (A$^3$/atom)')
    plt.ylabel('Space group')
    plt.title('Perovskites (XYO3)')
    plt.show()
