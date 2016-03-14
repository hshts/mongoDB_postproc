import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure
from getcoordination import getcoordination
from matminer.descriptors.composition_features import *


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
    for doc in coll.find().batch_size(75):
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
                newcoll.insert(
                    {'key': doc['key'], 'composition': str(st_comp), state: 'Yes', 'space_group': space_group,
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


def make_state_colls():
    """
        ##### CREATION OF 'hp' AND 'ht' COLLECTIONS
    :return:
    """
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


def add_coordination_tocoll():
    """
    Add a coordination number column to the hp/ht collections
    :return:
    """
    # props = ['hp', 'ht']
    props = ['ht']
    for prop in props:
        for document in db[prop].find().batch_size(75).skip(12392):
            for doc in db['pauling_file'].find({'key': document['key']}):  # Should be just 1 loop for 1 result
                try:
                    coord_dict = getcoordination(Structure.from_dict(doc['structure']))
                    print coord_dict
                    db[prop].update({'key': document['key']}, {'$set': {'max_coordination': max(coord_dict.values())}})
                except Exception as e:
                    print e
                    print 'Error for key {}!'.format(doc['key'])


def add_numberdensity_tocoll():
    """
    Add a number density column to the hp/ht collections
    :return:
    """
    props = ['hp', 'ht']
    for prop in props:
        x = 0
        for document in db[prop].find().batch_size(75):
            x += 1
            if x % 1000 == 0:
                print x
            for doc in db['pauling_file'].find({'key': document['key']}):  # Should be just 1 loop for 1 result
                try:
                    structure = Structure.from_dict(doc['structure'])
                    num_density = structure.num_sites/structure.volume
                    db[prop].update({'key': document['key']}, {'$set': {'number_density': num_density}})
                except Exception as e:
                    print e
                    print 'Error for key {}!'.format(doc['key'])


def group_merge_df(prop):
    pd.set_option('display.width', 1000)
    cursor = db[prop].find()
    df = pd.DataFrame(list(cursor))
    df_groupby = df.groupby(['composition', prop], as_index=False).median()
    df_groupby = df_groupby.groupby(prop, as_index=False)
    df_groupby_no = pd.DataFrame
    df_groupby_yes = pd.DataFrame
    for name, group in df_groupby:
        if name == 'No':
            df_groupby_no = group
        elif name == 'Yes':
            df_groupby_yes = group
    df_merge = pd.merge(df_groupby_no, df_groupby_yes, on='composition')
    # x = 0
    for i, row in df_merge.iterrows():
        # x += 1
        try:
            coeff_lst = get_linear_thermal_expansion(row['composition'])
            df_merge.loc[i, 'linear_thermal_exp_coeff'] = np.mean(coeff_lst)
            if np.mean(coeff_lst) < 20:
                df_merge.loc[i, 'color'] = 'k'
            # elif 10 <= np.mean(coeff_lst) < 20:
            #     df_merge.loc[i, 'color'] = 'r'
            elif np.mean(coeff_lst) >= 20:
                df_merge.loc[i, 'color'] = 'r'
            else:
                df_merge.loc[i, 'color'] = 'k'
        except ValueError:
            df_merge.loc[i, 'color'] = 'k'
            continue
        # if x > 5:
        #     break
    print df_merge.head(10)
    print df_merge.describe()
    return df_merge
    # print df_merge.loc[df_merge['composition'].isin(
    #     ['Th', 'Cm', 'Cf', 'Cs', 'Li', 'GaTe', 'TmTe', 'Li2S', 'HoSn3', 'ZnF2', 'ZrO2'])]


def plot_results(df):
    """

    :return:
    """
    plot_props = ['density', 'space_group', 'number_density']
    for pro in plot_props:
        fig, ax = plt.subplots()
        for k, v in df.iterrows():
            if pro == 'density':
                label_cutoff = 0.5
            # elif pro == 'space_group':
            else:
                label_cutoff = 0.75
            if (abs(v[pro + '_y'] - v[pro + '_x'])) / v[pro + '_x'] > label_cutoff:
                ax.text(v[pro + '_x'], v[pro + '_y'], v['composition'])
        df.plot(x=pro + '_x', y=pro + '_y', kind='scatter', ax=ax, c=df['color'])
        plt.xlabel(pro + ' of ground states')
        plt.ylabel(pro + ' of excited states')
        if 'hp_x' in df.columns:
            plt.title('HP (high pressure) phases')
        else:
            plt.title('HT (high temeperature) phases')
        plt.show()
        # sns.set_style('whitegrid')
        # sns.violinplot(x='property', y='space_group', hue=prop, data=df_med, palette='muted', split=True)
        # plt.show()
        # sns.violinplot(x='property', y='density', hue=prop, data=df_med, palette='muted', split=True)
        # plt.show()
        # tips = sns.load_dataset("tips")
        # print tips.head()
        # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)


if __name__ == '__main__':
    # add_coordination_tocoll()
    props = ['hp', 'ht']
    for coll in props:
        merged_df = group_merge_df(coll)
        plot_results(merged_df)
