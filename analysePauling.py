import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure
from getcoordination import getcoordination
from matminer.descriptors.composition_features import *
from pymatgen.matproj.rest import MPRester
import itertools
from collections import defaultdict

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


def insert_states(state, incl_keys, excl_keys):
    coll = db['pauling_file']
    newcoll = db[state]
    newcoll.drop()
    state_keys = []
    state_compositions = []
    x = 0
    for key in incl_keys:
        x += 1
        if x % 1000 == 0:
            print x
        for doc in coll.find({'key': key}):  # 1 result
            if 'structure' in doc:
                comp = Structure.from_dict(doc['structure']).composition.reduced_formula
                # comp = Composition(doc['metadata']['_Springer']['geninfo']['Standard Formula'])
                state_keys.append(key)
                state_compositions.append(comp)
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
                newcoll.insert({'key': doc['key'], 'composition': str(comp), state: False, 'space_group': space_group,
                                'density': density, 'property': 1})
    print 'GS DONE!'
    print len(gs_compositions)
    for idx, st_comp in enumerate(state_compositions):
        if st_comp in gs_compositions:
            st_key = state_keys[idx]
            for doc in coll.find({'key': st_key}):
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
                newcoll.update({'key': doc['key']},
                               {'key': doc['key'], 'composition': str(st_comp), state: True, 'space_group': space_group,
                                'density': density, 'property': 1}, upsert=True)
                # newcoll.insert(
                #     {'key': doc['key'], 'composition': str(st_comp), state: True, 'space_group': space_group,
                #      'density': density, 'property': 1})
    print '{} DONE!'.format(state)


def insert_statekeys(state):
    coll = db['pauling_file']
    # if state == 'lt':
    #     newcoll = db['ht_keys']
    # else:
    newcoll = db[state + '_keys']
    newcoll.drop()
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
    elif state == 'ht':
        # or state == 'lt':
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


def add_coordination_tocoll():
    """
    Add a coordination number column to the hp/ht collections
    :return:
    """
    props = ['hp', 'ht']
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


def add_numberdensity_tocoll(prop):
    """
    Add a number density column to the hp/ht collections
    :return:
    """
    for doc in db['pauling_file_tags_' + prop].find().batch_size(75):
        try:
            structure = Structure.from_dict(doc['structure'])
            num_density = structure.num_sites / structure.volume
            db['pauling_file_tags_' + prop].update({'key': doc['key']}, {'$set': {'number_density': num_density}})
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
        if not name:
            df_groupby_no = group
        elif name:
            df_groupby_yes = group
    df_merge = pd.merge(df_groupby_no, df_groupby_yes, on='composition')
    return df_merge


def add_descriptor_todf(df):
    for i, row in df.iterrows():
        try:
            coeff_lst = get_linear_thermal_expansion(row['composition'])
            df.loc[i, 'linear_thermal_exp_coeff'] = np.mean(coeff_lst)
            if np.mean(coeff_lst) < 20:
                df.loc[i, 'color_thermalcoeff'] = 'k'
            elif np.mean(coeff_lst) >= 20:
                df.loc[i, 'color_thermalcoeff'] = 'r'
            else:
                df.loc[i, 'color_thermalcoeff'] = 'k'
            rig_mod_lst = get_rigidity_modulus(row['composition'])
            df.loc[i, 'rigidity_modulus'] = np.mean(rig_mod_lst)
            if np.mean(rig_mod_lst) < 50:
                df.loc[i, 'color_rigmod'] = 'k'
            elif np.mean(rig_mod_lst) >= 50:
                df.loc[i, 'color_rigmod'] = 'r'
            else:
                df.loc[i, 'color_rigmod'] = 'k'
            '''
            mp_results = mpr.query(row['composition'], ['e_above_hull', 'band_gap'])
            if len(mp_results) == 0:
                comp_bandgap = np.nan
            else:
                e_above_hull = []
                band_gap = []
                for result in mp_results:
                    e_above_hull.append(result['e_above_hull'])
                    band_gap.append(result['band_gap'])
                comp_bandgap = band_gap[e_above_hull.index(min(e_above_hull))]
            if comp_bandgap == 0:
                df.loc[i, 'color_class'] = 'b'
            elif 0 < comp_bandgap <= 3:
                df.loc[i, 'color_class'] = 'g'
            elif comp_bandgap > 3:
                df.loc[i, 'color_class'] = 'r'
            else:
                df.loc[i, 'color_class'] = 'k'
            '''
        except ValueError:
            df.loc[i, 'color_thermalcoeff'] = 'k'
            df.loc[i, 'color_rigmod'] = 'k'
            df.loc[i, 'color_class'] = 'k'
            continue
    print df.head(5)
    print df.describe()
    print df.loc[df['composition'].isin(
        ['Th', 'Cm', 'Cf', 'Cs', 'Li', 'GaTe', 'TmTe', 'Li2S', 'HoSn3', 'ZnF2', 'ZrO2'])]
    return df


def plot_xy(df, propname):
    """

    :return:
    """
    plot_props = ['density', 'space_group', 'number_density']
    for pro in plot_props:
        # fig, ax = plt.subplots()
        # for k, v in df.iterrows():
        #     if pro == 'density':
        #         label_cutoff = 0.5
        #     elif pro == 'space_group':
        # else:
        #     label_cutoff = 0.75
        # if (abs(v[pro + '_y'] - v[pro + '_x'])) / v[pro + '_x'] > label_cutoff:
        #     ax.text(v[pro + '_x'], v[pro + '_y'], v['composition'])
        df.plot(x=pro + '_x', y=pro + '_y', kind='scatter')
        plt.xlabel(pro + ' of ground states')
        plt.ylabel(pro + ' of excited states')
        if propname == 'hp':
            plt.title('HP (high pressure) phases')
        elif propname == 'ht':
            plt.title('HT (high temeperature) phases')
        plt.show()
        sns.set_style('whitegrid')


def plot_violin(df, propname):
    plot_props = ['density', 'space_group', 'number_density']
    for pro in plot_props:
        sns.violinplot(x='is_' + propname + '_dataset', y=pro, hue='is_' + propname, data=df, palette='muted',
                       split=True)
        plt.show()
    # tips = sns.load_dataset("tips")
    # print tips.head()
    # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)


def get_meta_from_structure(structure):
    comp = structure.composition
    elsyms = sorted(set([e.symbol for e in comp.elements]))
    meta = {'nsites': len(structure),
            'elements': elsyms,
            'nelements': len(elsyms),
            'formula': comp.formula,
            'reduced_cell_formula': comp.reduced_formula,
            'reduced_cell_formula_abc': Composition(comp.reduced_formula)
                .alphabetical_formula,
            'anonymized_formula': comp.anonymized_formula,
            'chemsystem': '-'.join(elsyms),
            'is_ordered': structure.is_ordered,
            'is_valid': structure.is_valid()}
    return meta


def add_metastructuredata():
    z = 0
    coll = db['pauling_file']
    for doc in coll.find().batch_size(75):
        if z % 1000 == 0:
            print z
        if 'structure' in doc:
            metastrucdata = get_meta_from_structure(Structure.from_dict(doc['structure']))
            coll.update({'key': doc['key']}, {'$set': {'metadata._structure': metastrucdata}})
            z += 1


def detect_hp_ht(doc):
    coll = db['pauling_file_tags']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    title = doc['metadata']['_Springer']['title']
    hp_title = None
    hp_phase = None
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
            coll.update({'key': doc['key']}, {'$set': {'is_hp': hp_title}})
        else:
            coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
    else:
        coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
    ht_title = None
    ht_phase = None
    if 'T =' in title:
        try:
            temp_k = float(re.findall(r'T\s*=\s*(.*)\s*K', title)[0])
            if temp_k > 400:
                ht_title = True
            elif temp_k <= 400:
                ht_title = False
        except:
            ht_title = None
    elif ' ht' in title:
        ht_title = True
    elif ' rt' in title:
        ht_title = False
    else:
        ht_title = None
    if ' ht' in phase:
        ht_phase = True
    elif ' rt' in phase:
        ht_phase = False
    else:
        ht_phase = None
    if ht_title == ht_phase:
        if ht_title is not None:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': ht_title}})
        else:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': False}})
    elif ht_title is not None and ht_phase is not None:
        coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
    else:
        if ht_title is not None:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': ht_title}})
        else:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': ht_phase}})


def set_hpht_dataset_tags():
    tagcoll = db['pauling_file_tags']
    comps_hp_true = set()
    comps_hp_false = set()
    comps_ht_true = set()
    comps_ht_false = set()
    comps_ids = defaultdict(list)
    x = 0
    for doc in tagcoll.find({'structure': {'$exists': True}}, {'key': 1, 'is_hp': 1, 'is_ht': 1,
                                                               'metadata._structure.reduced_cell_formula':
                                                                   1}).batch_size(
        75):
        x += 1
        if x % 1000 == 0:
            print x
        composition = doc['metadata']['_structure']['reduced_cell_formula']
        if doc['is_hp'] is True:
            comps_hp_true.add(composition)
        elif doc['is_hp'] is False:
            comps_hp_false.add(composition)
        if doc['is_ht'] is True:
            comps_ht_true.add(composition)
        elif doc['is_ht'] is False:
            comps_ht_false.add(composition)
        comps_ids[composition].append(doc['key'])
    hp_unique_comps = comps_hp_true.intersection(comps_hp_false)
    print len(hp_unique_comps)
    for comp in hp_unique_comps:
        ids_toset = comps_ids[comp]
        for id in ids_toset:
            tagcoll.update({'key': id}, {'$set': {'is_hp_dataset': True}})
    ht_unique_comps = comps_ht_true.intersection(comps_ht_false)
    print len(ht_unique_comps)
    for comp in ht_unique_comps:
        ids_toset = comps_ids[comp]
        for id in ids_toset:
            tagcoll.update({'key': id}, {'$set': {'is_ht_dataset': True}})


def tags_group_merge_df(prop):
    coll = db['pauling_file_tags_' + prop]
    cursor = coll.find()
    df = pd.DataFrame(list(cursor))
    for i, row in df.iterrows():
        if row['metadata']['_structure']['is_valid']:
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
            num_density = structure.num_sites / structure.volume
            df.set_value(i, 'number_density', num_density)
    df_groupby = df.groupby(['reduced_cell_formula', 'is_' + prop], as_index=False).mean()
    df_2nd_groupby = df_groupby.groupby('is_' + prop, as_index=False)
    df_groupby_false = pd.DataFrame
    df_groupby_true = pd.DataFrame
    for name, group in df_2nd_groupby:
        if not name:
            df_groupby_false = group
        elif name:
            df_groupby_true = group
    df_merge = pd.merge(df_groupby_false, df_groupby_true, on='reduced_cell_formula')
    return df_groupby, df_merge


def save_ddf_pkl(df, name):
    df.to_pickle(name + '.pkl')


def analyze_df(df_pkl):
    df = pd.read_pickle(df_pkl)
    print df.describe()


if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    props = ['hp', 'ht']
    for name in props:
        grouped_df, merged_df = tags_group_merge_df(name)
        save_ddf_pkl(merged_df, name)
        analyze_df(name + '.pkl')
        # plot_violin(grouped_df, name)
        # plot_xy(merged_df, name)
