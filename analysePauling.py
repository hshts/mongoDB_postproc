import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure
from getcoordination import getcoordination
from matminer.descriptors.composition_features import *
from pymatgen.matproj.rest import MPRester
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


def get_meta_from_structure(structure):
    comp = structure.composition
    elsyms = sorted(set([e.symbol for e in comp.elements]))
    meta = {'nsites': len(structure),
            'elements': elsyms,
            'nelements': len(elsyms),
            'formula': comp.formula,
            'reduced_cell_formula': comp.reduced_formula,
            'reduced_cell_formula_abc': Composition(comp.reduced_formula).alphabetical_formula,
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


def get_compd_class(prop, reduced_formula):
    df = pd.read_pickle('pauling_file_tags_' + prop + '.pkl')
    return df.loc[df['reduced_cell_formula'] == reduced_formula]['metadata'].iloc[0]['_Springer']['geninfo'][
        'Compound Class(es)']


def old_set_hpht_tags(doc):
    """
    Sets temperature tags based on labels in the title and phase label fields
    :param doc: Pauling file record
    :return:
    """
    coll = db['pauling_file_tags']
    title = doc['metadata']['_Springer']['title']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    # Set pressure tags
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
    # Set temperature tags
    ht_title = None
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
    elif ' rt' in title or ' lt' in title:
        ht_title = False
    else:
        ht_title = None
    if ' ht' in phase:
        ht_phase = True
    elif ' rt' in phase or ' lt' in phase:
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


def create_tagscoll():
    db['pauling_file_tags'].drop()
    coll = db['pauling_file']
    coll.aggregate([{'$project': {'key': 1, 'metadata': 1, 'structure': 1}}, {'$out': 'pauling_file_tags'}])
    db['pauling_file_tags'].create_index([('key', pymongo.ASCENDING)], unique=True)


def set_hpht_tags(doc, lt_highcutff, ht_lowcutoff):
    """
    Sets temperature tags only based on the value of the field 'metadata._Springer.expdetails.temperature'
    :param lt_highcutff: highest temperature for low temperature measurments
    :param ht_lowcutoff: lowest temperature for high temperature measurement
    :param doc: Pauling file record
    :return:
    """
    coll = db['pauling_file_tags']
    title = doc['metadata']['_Springer']['title']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    # Set pressure tags
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
    # Set temperature tags
    if 'temperature' in doc['metadata']['_Springer']['expdetails']:
        exp_t = doc['metadata']['_Springer']['expdetails']['temperature']
        try:
            temp_str = re.findall(r'T\s*=\s*(.*)\s*K', exp_t)[0]
            temp_exp = float(re.sub('\(.*\)', '', temp_str))
            if temp_exp > ht_lowcutoff:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': True}})
            elif temp_exp < lt_highcutff:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': False}})
            else:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
        except:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
    else:
        coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})


def set_hpht_dataset_tags():
    tagcoll = db['pauling_file_tags']
    # Initialize the tags 'is_hp_dataset' and 'is_ht_dataset'
    tagcoll.update({'structure': {'$exists': True}}, {'$set': {'is_hp_dataset': False, 'is_ht_dataset': False}},
                   multi=True)
    comps_hp_true = set()
    comps_hp_false = set()
    comps_ht_true = set()
    comps_ht_false = set()
    comps_ids = defaultdict(list)
    x = 0
    for doc in tagcoll.find({'structure': {'$exists': True}}).batch_size(75):
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
            # Remove docs with 'is_ht' = None ('null' in mongo)
            for doc in tagcoll.find({'key': id}):
                ht_tag = doc['is_ht']
                if ht_tag is not None:
                    tagcoll.update({'key': id}, {'$set': {'is_ht_dataset': True}})


def create_hphtcolls():
    db['pauling_file_tags_hp'].drop()
    db['pauling_file_tags_ht'].drop()
    tagcoll = db['pauling_file_tags']
    hp_pipeline = [{'$match': {'is_ht': False, 'is_hp_dataset': True}}, {'$out': 'pauling_file_tags_hp'}]
    tagcoll.aggregate(pipeline=hp_pipeline)
    db['pauling_file_tags_hp'].create_index([('key', pymongo.ASCENDING)], unique=True)
    ht_pipeline = [{'$match': {'is_hp': False, 'is_ht_dataset': True}}, {'$out': 'pauling_file_tags_ht'}]
    tagcoll.aggregate(pipeline=ht_pipeline)
    db['pauling_file_tags_ht'].create_index([('key', pymongo.ASCENDING)], unique=True)


def coll_to_pickle(prop):
    cursor = db['pauling_file_tags_' + prop].find()
    df = pd.DataFrame(list(cursor))
    df.to_pickle('pauling_file_tags_' + prop + '.pkl')


def group_merge_df(prop):
    df = pd.read_pickle('pauling_file_tags_' + prop + '.pkl')
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
            composition = Composition(row['metadata']['_structure']['reduced_cell_formula'])
            num_density = len(composition.get_el_amt_dict()) / structure.volume
            df.set_value(i, 'number_density', num_density)
        if row['metadata']['_structure']['is_ordered']:
            df.set_value(i, 'is_ordered', 1)
        else:
            df.set_value(i, 'is_ordered', 0)
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


def plot_violin(df, propname):
    plot_props = ['density', 'space_group', 'number_density']
    for pro in plot_props:
        sns.violinplot(x='is_' + propname + '_dataset', y=pro, hue='is_' + propname, data=df, palette='muted',
                       split=True)
        plt.show()
        # tips = sns.load_dataset("tips")
        # print tips.head()
        # sns.violinplot(x="day", y="total_bill", hue="smoker", data=tips, palette="muted", split=True)


def plot_xy(df, propname, descriptor=None):
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
        if descriptor is None:
            df.plot(x=pro + '_x', y=pro + '_y', kind='scatter')
        else:
            color_column = df[descriptor]
            df.plot(x=pro + '_x', y=pro + '_y', kind='scatter', c=color_column)
        plt.xlabel(pro + ' of ground states')
        plt.ylabel(pro + ' of excited states')
        if propname == 'hp':
            plt.title('HP (high pressure) phases')
        elif propname == 'ht':
            plt.title('HT (high temeperature) phases')
        plt.show()
        sns.set_style('whitegrid')


# TODO: Check how to automatically get stats (mean, median,..) from the descriptor column and use them to set limits
# for plot colors
# TODO: Check how to set legends in plots (return them here and pass them onto plot_xy()
class AddDescriptor:
    def __init__(self, propname):
        self.df = pd.read_pickle(propname + '.pkl')
        self.descriptor = ''

    def X(self):
        self.descriptor = 'col_eleneg_std'
        for i, row in self.df.iterrows():
            try:
                electronegativity_std = get_std(get_pymatgen_eldata_lst(row['reduced_cell_formula'], 'X'))
                self.df.loc[i, 'eleneg_std'] = electronegativity_std
                if electronegativity_std < 0.70:
                    self.df.loc[i, 'col_eleneg_std'] = 'b'
                elif 0.70 <= electronegativity_std <= 1.40:
                    self.df.loc[i, 'col_eleneg_std'] = 'g'
                else:
                    self.df.loc[i, 'col_eleneg_std'] = 'r'
            except ValueError:
                self.df.loc[i, 'col_eleneg_std'] = 'k'
                continue
        return self.df, self.descriptor

    def coefficient_of_linear_thermal_expansion(self):
        self.descriptor = 'col_thermalcoeff'
        for i, row in self.df.iterrows():
            try:
                coeff_std = get_std(get_linear_thermal_expansion(row['reduced_cell_formula']))
                self.df.loc[i, 'linear_thermal_exp_coeff'] = coeff_std
                if coeff_std < 3.10:
                    self.df.loc[i, 'col_thermalcoeff'] = 'r'
                elif 3.10 <= coeff_std <= 7.45:
                    self.df.loc[i, 'col_thermalcoeff'] = 'g'
                else:
                    self.df.loc[i, 'col_thermalcoeff'] = 'b'
            except:
                self.df.loc[i, 'col_thermalcoeff'] = 'k'
                continue
        return self.df, self.descriptor

    def is_magnetic(self):
        self.descriptor = 'col_mag'
        ferromagnetic = ['Fe', 'Co', 'Ni', 'Gd']
        paramagnetic = ['Li', 'O', 'Na', 'Mg', 'Al', 'Ca', 'Ti', 'Mn', 'Sr', 'Zr', 'Mo', 'Ru', 'Rh', 'Pd', 'Sn', 'Ba',
                        'Ce', 'Nd', 'Sm', 'Eu', 'Tb', 'Dy', 'Ho', 'Er', 'Tm', 'W', 'Os', 'Ir', 'Pt']
        for i, row in self.df.iterrows():
            is_magnetic = False
            elements = Composition(row['reduced_cell_formula']).elements
            for elem in elements:
                if elem.symbol in ferromagnetic:
                    self.df.loc[i, 'col_mag'] = 'b'
                    is_magnetic = True
                    break
                elif elem.symbol in paramagnetic:
                    self.df.loc[i, 'col_mag'] = 'g'
                    is_magnetic = True
                    break
            if not is_magnetic:
                self.df.loc[i, 'col_mag'] = 'r'
        return self.df, self.descriptor

    def is_ordered(self):
        self.descriptor = 'col_ord'
        for i, row in self.df.iterrows():
            if row['is_ordered_x'] == row['is_ordered_y']:
                if row['is_ordered_x'] == 1:
                    self.df.loc[i, 'col_ord'] = 'b'
                elif row['is_ordered_x'] == 0:
                    self.df.loc[i, 'col_ord'] = 'r'
            else:
                self.df.loc[i, 'col_ord'] = 'k'
        return self.df, self.descriptor


def analyze_df(prop):
    df = pd.read_pickle(prop + '.pkl')
    for i, row in df.iterrows():
        df.set_value(i, 'sg_diff', row['space_group_y'] - row['space_group_x'])
    print df.sort_values('sg_diff').dropna().tail(60)
    print df.loc[(60 < df['space_group_x']) & (df['space_group_x'] < 65)]
    print df.sort_values('number_density_y').dropna().tail(60)


if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    # create_tagscoll()
    '''
    x = 0
    for doc in db['pauling_file_tags'].find({'structure': {'$exists': True}}).batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        set_hpht_tags(doc, 350, 450)
    # '''
    # set_hpht_dataset_tags()
    # create_hphtcolls()
    props = ['ht']
    for name in props:
        # coll_to_pickle(name)
        # grouped_df, merged_df = group_merge_df(name)
        # print merged_df.head(20)
        # print merged_df.describe()
        # plot_violin(grouped_df, name)
        # plot_xy(merged_df, name)
        # merged_df.to_pickle(name + '.pkl')
        # analyze_df(name)
        # df_desc, desc = getattr(AddDescriptor(name), 'X')()
        # df_desc, desc = getattr(AddDescriptor(name), 'coefficient_of_linear_thermal_expansio')()
        # df_desc, desc = getattr(AddDescriptor(name), 'is_magnetic')()
        df_desc, desc = getattr(AddDescriptor(name), 'is_ordered')()
        # print df_withdesc.describe()
        plot_xy(df_desc, name, desc)
