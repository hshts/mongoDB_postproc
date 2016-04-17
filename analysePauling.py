import pymongo
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pymatgen import Structure
from getcoordination import getcoordination, get_mean_specie_sites
from matminer.descriptors.composition_features import *
from pymatgen.matproj.rest import MPRester
from collections import defaultdict
import re

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


def create_tagscoll():
    db['pauling_file_min_tags1'].drop()
    coll = db['pauling_file']
    coll.aggregate(
        [{'$match': {'structure': {'$exists': True}}}, {'$project': {'key': 1, 'metadata': 1, 'structure': 1}},
         {'$out': 'pauling_file_min_tags1'}])
    db['pauling_file_min_tags1'].create_index([('key', pymongo.ASCENDING)], unique=True)


def set_hpht_tags(doc, lt_highcutff, ht_lowcutoff):
    """
    Sets temperature tags only based on the value of the field 'metadata._Springer.expdetails.temperature'
    :param lt_highcutff: highest temperature for low temperature measurments
    :param ht_lowcutoff: lowest temperature for high temperature measurement
    :param doc: Pauling file record
    :return:
    """
    coll = db['pauling_file_min_tags1']
    title = doc['metadata']['_Springer']['title']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    # Set pressure tags
    if 'p =' in title or ' hp' in title:
        if 'p =' in title:
            pressure_str = re.findall(r'p = (.*) GPa', title)[0]
            pressure_val = float(re.sub('\(.*\)', '', pressure_str))
            coll.update({'key': doc['key']}, {'$set': {'pressure (GPa)': pressure_val}})
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
            temp_val = float(re.sub('\(.*\)', '', temp_str))
            if temp_val > ht_lowcutoff:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': True}})
            elif temp_val < lt_highcutff:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': False}})
            else:
                coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
            coll.update({'key': doc['key']}, {'$set': {'temperature (K)': temp_val}})
        except:
            coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})
    else:
        coll.update({'key': doc['key']}, {'$set': {'is_ht': None}})


def set_hpht_dataset_tags():
    tagcoll = db['pauling_file_min_tags1']
    # Initialize the tags 'is_hp_dataset' and 'is_ht_dataset'
    tagcoll.update({'$set': {'is_hp_dataset': False, 'is_ht_dataset': False}}, multi=True)
    comps_hp_true = set()
    comps_hp_false = set()
    comps_ht_true = set()
    comps_ht_false = set()
    comps_ids = defaultdict(list)
    x = 0
    for doc in tagcoll.find().batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        composition = doc['metadata']['_structure']['reduced_cell_formula']
        if doc['is_hp'] is True and doc['is_ht'] is False:
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
    hp_pipeline = [{'$match': {'is_ht': False, 'is_hp_dataset': True, 'metadata._structure.is_valid': True}},
                   {'$out': 'pauling_file_tags_hp'}]
    tagcoll.aggregate(pipeline=hp_pipeline)
    db['pauling_file_tags_hp'].create_index([('key', pymongo.ASCENDING)], unique=True)
    ht_pipeline = [{'$match': {'is_hp': False, 'is_ht_dataset': True, 'metadata._structure.is_valid': True}},
                   {'$out': 'pauling_file_tags_ht'}]
    tagcoll.aggregate(pipeline=ht_pipeline)
    db['pauling_file_tags_ht'].create_index([('key', pymongo.ASCENDING)], unique=True)


def remove_deuterium(prop):
    coll = db['pauling_file_tags_' + prop]
    cursor = coll.find()
    df = pd.DataFrame(list(cursor))
    for i, row in df.iterrows():
        for el in row['metadata']['_structure']['elements']:
            if el == 'D':
                coll.remove({'key': row['key']})
                break


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
                elif 0.70 <= electronegativity_std <= 1.00:
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
                coeff_std = get_std(
                    get_pymatgen_eldata_lst(row['reduced_cell_formula'], 'coefficient_of_linear_thermal_expansion'))
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
        ferromagnetic = ['Fe']
        # paramagnetic = ['Li', 'O', 'Na', 'Mg', 'Al', 'Ca', 'Ti', 'Mn', 'Sr', 'Zr', 'Mo', 'Ru', 'Rh', 'Pd', 'Sn', 'Ba',
        #                 'Ce', 'Nd', 'Sm', 'Eu', 'Tb', 'Dy', 'Ho', 'Er', 'Tm', 'W', 'Os', 'Ir', 'Pt']
        for i, row in self.df.iterrows():
            is_magnetic = False
            elements = Composition(row['reduced_cell_formula']).elements
            for elem in elements:
                if elem.symbol in ferromagnetic:
                    self.df.loc[i, 'col_mag'] = 'b'
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
                if row['is_ordered_x'] <= 0.5 and row['is_ordered_y'] > 0.5:
                    self.df.loc[i, 'col_ord'] = 'c'
                elif row['is_ordered_x'] > 0.5 and row['is_ordered_y'] <= 0.5:
                    self.df.loc[i, 'col_ord'] = 'm'
                else:
                    self.df.loc[i, 'col_ord'] = 'g'
        return self.df, self.descriptor

    def coordination(self):
        self.descriptor = 'col_coord'
        for i, row in self.df.iterrows():
            print getcoordination(row['structure'])
        return self.df, self.descriptor


def analyze_df(prop):
    df = pd.read_pickle(prop + '.pkl')
    for i, row in df.iterrows():
        df.set_value(i, 'sg_diff', row['space_group_y'] - row['space_group_x'])
    # print df.loc[df['reduced_cell_formula'] == 'Fe']
    print df.sort_values('sg_diff').dropna().head(50)
    # print df.loc[(60 < df['space_group_x']) & (df['space_group_x'] < 65)]
    # print df.sort_values('number_density_y').dropna().tail(60)


def get_coordination(idx_struct):
    specie_meancoord = get_mean_specie_sites(getcoordination(Structure.from_dict(idx_struct[1])))
    el_amt = Structure.from_dict(idx_struct[1]).composition.get_el_amt_dict()
    cations = []
    anions = []
    X_cutoff = 2.5
    for el in el_amt:
        if Element(el).X < X_cutoff:
            cations.append(el)
        else:
            anions.append(el)
    total_cation_coords = 0
    total_cation_amts = 0
    for cation in cations:
        try:
            total_cation_coords += (el_amt[cation] * specie_meancoord[cation])
        except KeyError:
            for sp in specie_meancoord:
                if cation in sp:
                    cation_key = sp
                    break
            total_cation_coords += (el_amt[cation] * specie_meancoord[cation_key])
        total_cation_amts += el_amt[cation]
    cations_weighted_coord = total_cation_coords / total_cation_amts
    total_anion_coords = 0
    total_anion_amts = 0
    for anion in anions:
        try:
            total_anion_coords += (el_amt[anion] * specie_meancoord[anion])
        except KeyError:
            for sp in specie_meancoord:
                if anion in sp:
                    anion_key = sp
                    break
            total_anion_coords += (el_amt[anion] * specie_meancoord[anion_key])
        total_anion_amts += el_amt[anion]
    anions_weighted_coord = total_anion_coords / total_anion_amts
    print cations_weighted_coord, anions_weighted_coord
    print '----------'
    return idx_struct[0], cations_weighted_coord, anions_weighted_coord


def plot_common_comp():
    hp_df = pd.read_pickle('hp.pkl')
    print hp_df.head()
    ht_df = pd.read_pickle('ht.pkl')
    print ht_df.head()
    hpht_df = pd.merge(hp_df, ht_df, on='reduced_cell_formula')
    for i, row in hpht_df.iterrows():
        hpht_df.set_value(i, 'sg_diff_hp', row['space_group_y_x'] - row['space_group_x_x'])
        hpht_df.set_value(i, 'sg_diff_ht', row['space_group_y_y'] - row['space_group_x_y'])
    # print hpht_df
    hpht_df.plot(x='sg_diff_ht', y='sg_diff_hp', kind='scatter')
    plt.show()
    print hpht_df.sort(['sg_diff_hp', 'sg_diff_ht'])


if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    create_tagscoll()
    # '''
    x = 0
    for doc in db['pauling_file_min_tags1'].find().batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        set_hpht_tags(doc, 350, 450)
    # '''
    # props = ['hp', 'ht']
    # for name in props:
    #     coll_to_pickle(name)
    #     grouped_df, merged_df = group_merge_df(name)
    # print grouped_df.head(10)
    # print grouped_df.describe()
    # print merged_df.head(10)
    # print merged_df.describe()
    # plot_violin(grouped_df, name)
    # plot_xy(merged_df, name)
    # merged_df.to_pickle(name + '.pkl')
    # analyze_df(name)
    # df_desc, desc = getattr(AddDescriptor(name), 'coordination')()
    # print df_desc.head()
    # print df_desc.describe()
    # plot_xy(df_desc, name, desc)
    '''
    big_df = pd.read_pickle('pauling_file_tags_ht.pkl')
    idxs = big_df.index.tolist()
    p = mp.Pool(processes=4)
    job_args = [(idx, big_df.ix[idx, 'structure']) for idx in idxs]
    pool_results = p.map(get_coordination, job_args)
    p.close()
    p.join()
    for idx_coord in pool_results:
        big_df.set_value(idx_coord[0], 'cation_coord', idx_coord[1])
        big_df.set_value(idx_coord[0], 'anion_coord', idx_coord[2])
    print big_df.describe()
    # '''
    # plot_common_comp()
