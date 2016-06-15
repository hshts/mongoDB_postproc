import re
from collections import defaultdict
import matplotlib.pyplot as plt
import pandas as pd
import pymongo
import seaborn as sns
from pandas.io import json
from pymatgen import Structure
from pymatgen.matproj.rest import MPRester
from getcoordination import VoronoiCoordFinder_edited, get_avg_cns, get_cation_weighted_avg, EffectiveCoordFinder
from matminer.descriptors.composition_features import *
from tina_cn_code.okeeffe_CN import get_avg_cn as okeeffe_get_avg_cn

client = pymongo.MongoClient()
db = client.springer
mpr = MPRester()


def create_tagscoll():
    coll = db['pauling_file']
    min_tags_collname = 'pauling_file_min_tags'
    db[min_tags_collname].drop()
    coll.aggregate([{'$match': {'structure': {'$exists': True}, 'metadata._structure.is_valid': True,
                                'errors': {
                                    '$nin': ['structural composition and refined/alphabetic formula do not match']}}},
                    {'$project': {'key': 1, 'metadata': 1, 'structure': 1}}, {'$out': min_tags_collname}])
    db[min_tags_collname].create_index([('key', pymongo.ASCENDING)], unique=True)
    # Remove Deuterium
    for doc in db[min_tags_collname].find().batch_size(75):
        for el in doc['metadata']['_structure']['elements']:
            if el == 'D':
                db[min_tags_collname].remove({'key': doc['key']})
                break


def set_hpht_tags(doc, lt_highcutff, ht_lowcutoff):
    """
    Sets temperature tags only based on the value of the field 'metadata._Springer.expdetails.temperature'
    :param lt_highcutff: highest temperature for low temperature measurments
    :param ht_lowcutoff: lowest temperature for high temperature measurement
    :param doc: Pauling file record
    :return:
    """
    coll = db['pauling_file_min_tags']
    title = doc['metadata']['_Springer']['title']
    phase = doc['metadata']['_Springer']['geninfo']['Phase Label(s)']
    # Set pressure tags
    if doc['key'] in ['sd_1601567', 'sd_1601568',
                      'sd_1601569']:  # These 3 ids (sd_1601567, sd_1601568, sd_1601569) have 'p =' in title but are
        # unparsable and they are all < 1atm
        coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
    elif 'p =' in title:
        try:
            pressure_str = re.findall(r'p = (.*) GPa', title)[0]
            pressure_val = float(re.sub('\(.*\)', '', pressure_str))
            coll.update({'key': doc['key']}, {'$set': {'pressure (GPa)': pressure_val}})
            if pressure_val > 0.00010132501:
                coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
            else:
                coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
        except UnicodeEncodeError as e:
            coll.update({'key': doc['key']}, {'$set': {'is_hp': None}})
            raise UnicodeEncodeError
    elif ' hp' in title or ' hp' in phase:
        coll.update({'key': doc['key']}, {'$set': {'is_hp': True}})
    else:
        coll.update({'key': doc['key']}, {'$set': {'is_hp': False}})
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
    tagcoll = db['pauling_file_min_tags']
    # Initialize the tags 'is_hp_dataset' and 'is_ht_dataset'
    tagcoll.update({}, {'$set': {'is_hp_dataset': False, 'is_ht_dataset': False}}, multi=True)
    comps_hp_true = set()
    comps_hp_false = set()
    comps_ht_true = set()
    comps_ht_false = set()
    hpcomps_ids = defaultdict(list)
    htcomps_ids = defaultdict(list)
    x = 0
    for doc in tagcoll.find().batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        composition = doc['metadata']['_structure']['reduced_cell_formula_abc']
        if doc['is_hp'] is True and doc['is_ht'] in [False, None]:
            comps_hp_true.add(composition)
            hpcomps_ids[composition].append(doc['key'])
        elif doc['is_hp'] is False and doc['is_ht'] in [False, None]:
            comps_hp_false.add(composition)
            hpcomps_ids[composition].append(doc['key'])
        if doc['is_ht'] is True and doc['is_hp'] is False:
            comps_ht_true.add(composition)
            htcomps_ids[composition].append(doc['key'])
        elif doc['is_ht'] is False and doc['is_hp'] is False:
            comps_ht_false.add(composition)
            htcomps_ids[composition].append(doc['key'])
    hp_unique_comps = comps_hp_true.intersection(comps_hp_false)
    print len(hp_unique_comps)
    for comp in hp_unique_comps:
        ids_toset = hpcomps_ids[comp]
        for id in ids_toset:
            tagcoll.update({'key': id}, {'$set': {'is_hp_dataset': True}})
    ht_unique_comps = comps_ht_true.intersection(comps_ht_false)
    print len(ht_unique_comps)
    for comp in ht_unique_comps:
        ids_toset = htcomps_ids[comp]
        for id in ids_toset:
            # Remove docs with 'is_ht' = None ('null' in mongo)
            for doc in tagcoll.find({'key': id}):
                ht_tag = doc['is_ht']
                if ht_tag is not None:
                    tagcoll.update({'key': id}, {'$set': {'is_ht_dataset': True}})


def add_features(df):
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
        composition = Composition(structure.composition)
        num_density = (composition.num_atoms/structure.volume)
        df.set_value(i, 'number_density', num_density)
        num_vol = (1/num_density)
        df.set_value(i, 'number_volume', num_vol)
        if row['metadata']['_structure']['is_ordered']:
            df.set_value(i, 'is_ordered', 1)
        else:
            df.set_value(i, 'is_ordered', 0)
    return df


def set_coordination(df):
    for i, row in df.iterrows():
        if i % 10 == 0:
            print i
        struc = Structure.from_dict(row['structure'])
        # using own edited Voronoi algorithm
        try:
            specie_meancoord = get_avg_cns(VoronoiCoordFinder_edited(struc).get_cns())
        except Exception as e:
            print e
            continue
        df.set_value(i, 'VoronoiEd_cn', json.dumps(specie_meancoord))
        df.set_value(i, 'Voronoi_cation_avgcn', get_cation_weighted_avg(specie_meancoord, struc))
        # using own Effective coordination algorithm
        try:
            specie_meaneffcoord = get_avg_cns(EffectiveCoordFinder(struc).get_cns(radius=10.0))
        except Exception as e:
            print e
            continue
        df.set_value(i, 'eff_cn', json.dumps(specie_meaneffcoord))
        df.set_value(i, 'eff_cation_avgcn', get_cation_weighted_avg(specie_meaneffcoord, struc))
        # using Tina's O'Keeffe coordination algorithm
        if row['metadata']['_structure']['is_ordered']:
            try:
                okeeffe_coord = okeeffe_get_avg_cn(struc)
            except Exception as e:
                print e
                continue
            df.set_value(i, 'okeeffe_cn', json.dumps(okeeffe_coord))
            df.set_value(i, 'okeeffe_cn_avg', get_cation_weighted_avg(okeeffe_coord, struc))
    return df


def group_merge_df(prop):
    df = pd.read_pickle(prop + '_cn.pkl')
    df_groupby = df.groupby(['reduced_cell_formula', 'is_' + prop], as_index=False).mean()
    df_2nd_groupby = df_groupby.groupby('is_' + prop, as_index=False)
    df_groupby_false = pd.DataFrame
    df_groupby_true = pd.DataFrame
    for name, group in df_2nd_groupby:
        if not name:
            df_groupby_false = group
        elif name:
            df_groupby_true = group
    df_merge = pd.merge(df_groupby_false, df_groupby_true, on='reduced_cell_formula',
                        suffixes=('_l' + prop[-1], '_h' + prop[-1]))
    return df_groupby, df_merge


def plot_violin(df, propname):
    plot_props = ['space_group']
    # plot_props = ['space_group', 'density', 'number_density', 'number_volume']
    if propname == 'hp':
        df['is_' + propname] = df['is_' + propname].map({True: 'HP', False: 'LP'})
    elif propname == 'ht':
        df['is_' + propname] = df['is_' + propname].map({True: 'HT', False: 'LT'})
    for pro in plot_props:
        sns.violinplot(x='is_' + propname + '_dataset', y=pro, hue='is_' + propname, data=df, palette='muted',
                       split=True)
        if pro == 'space_group':
            plt.xlabel('Number of compounds', fontsize=48)
            plt.ylabel('Space Group', fontsize=48)
            if propname == 'hp':
                plt.title('Space group distribution of HP and LP compounds', fontsize=48)
            elif propname == 'ht':
                plt.title('Space group distribution of HT and LT compounds', fontsize=48)
            plt.yticks(fontsize=48)
            plt.ylim((-50, 300))
            plt.legend(title='', fontsize=48)
        plt.show()


def plot_xy(df, propname, descriptor=None):
    plot_props = ['space_group', 'density', 'number_density', 'number_volume']
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
            df.plot(x=pro + '_l' + propname[-1], y=pro + '_h' + propname[-1], kind='scatter')
        else:
            color_column = df[descriptor]
            df.plot(x=pro + '_l' + propname[-1], y=pro + '_h' + propname[-1], kind='scatter', c=color_column)
        if pro == 'space_group':
            plt.xlabel('Space groups of ground states', fontsize=48)
            plt.ylabel('Space groups of excited states', fontsize=48)
            plt.xlim((0, 250))
            plt.ylim((0, 250))
        if propname == 'hp':
            plt.title('HP and LP phases', fontsize=48)
        elif propname == 'ht':
            plt.title('HT and LT phases', fontsize=48)
        plt.xticks(fontsize=48)
        plt.yticks(fontsize=48)
        plt.show()
        sns.set_style('whitegrid')


# TODO: Check how to automatically get stats (mean, median,..) from the descriptor column and use them to set limits
# for plot colors
# TODO: Check how to set legends in plots (return them here and pass them onto plot_xy()
class AddDescriptor:
    def __init__(self, propname):
        self.df = pd.read_pickle(propname + '_NEWF_merged.pkl')
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
            if row['is_ordered_l'] == row['is_ordered_y']:
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
            try:
                if 2 < row['okeeffe_cn_avg'] < 4:
                    self.df.loc[i, 'col_coord'] = 'y'
                elif 4 <= row['okeeffe_cn_avg'] < 6:
                    self.df.loc[i, 'col_coord'] = 'g'
                elif 6 <= row['okeeffe_cn_avg'] < 8:
                    self.df.loc[i, 'col_coord'] = 'b'
                elif 8 <= row['okeeffe_cn_avg'] < 10:
                    self.df.loc[i, 'col_coord'] = 'r'
                elif 10 <= row['okeeffe_cn_avg'] < 12:
                    self.df.loc[i, 'col_coord'] = 'c'
                else:
                    self.df.loc[i, 'col_coord'] = 'k'
            except KeyError:
                self.df.loc[i, 'col_coord'] = 'k'
        return self.df, self.descriptor


def analyze_df(prop):
    df = pd.read_pickle(prop + '_merged.pkl')
    for i, row in df.iterrows():
        df.set_value(i, 'sg_diff', row['space_group_h' + prop[-1]] - row['space_group_l' + prop[-1]])
        try:
            composition = Composition(row['reduced_cell_formula'])
            df.set_value(i, 'numden_diff', row['number_density_h' + prop[-1]] - row['number_density_l' + prop[-1]])
            df.set_value(i, 'numvol_diff', ((len(composition.get_el_amt_dict()) * composition.weight)/(row['number_density_h' + prop[-1]])) - ((len(composition.get_el_amt_dict()) * composition.weight)/(row['number_density_l' + prop[-1]])))
            df.set_value(i, 'den_diff', row['density_h' + prop[-1]] - row['density_l' + prop[-1]])
            df.set_value(i, 'vol_diff', (composition.weight/(row['density_h' + prop[-1]])) - (composition.weight/(row['density_l' + prop[-1]])))
        except ZeroDivisionError:
            pass
    df.plot(x='vol_diff', y='sg_diff', kind='scatter')
    print df.sort_values('sg_diff').tail(50)
    if prop == 'hp':
        plt.title('HP')
    elif prop == 'ht':
        plt.title('HT')
    plt.show()
    # print df.loc[df['reduced_cell_formula'] == 'Fe']
    # print df.loc[(60 < df['space_group_x']) & (df['space_group_x'] < 65)]
    # print df.sort_values('number_density_y').dropna().tail(60)


def plot_common_comp():
    hp_df = pd.read_pickle('hp_merged.pkl')
    ht_df = pd.read_pickle('ht_merged.pkl')
    hpht_df = pd.merge(hp_df, ht_df, on='reduced_cell_formula')
    for i, row in hpht_df.iterrows():
        hpht_df.set_value(i, 'sg_diff_hp', row['space_group_y_x'] - row['space_group_x_x'])
        hpht_df.set_value(i, 'sg_diff_ht', row['space_group_y_y'] - row['space_group_x_y'])
    # print hpht_df
    hpht_df.plot(x='sg_diff_ht', y='sg_diff_hp', kind='scatter')
    plt.show()
    print hpht_df.sort_values(['sg_diff_ht', 'sg_diff_hp'], ascending=[False, True])


if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    # create_tagscoll()
    '''
    x = 0
    for doc in db['pauling_file_min_tags'].find().batch_size(75):
        x += 1
        if x % 1000 == 0:
            print x
        set_hpht_tags(doc, 350, 450)
    # '''
    # set_hpht_dataset_tags()
    props = ['hp', 'ht']
    for name in props:
        # cursor = db['pauling_file_min_tags'].find({'is_' + name + '_dataset': True})
        # df = pd.DataFrame(list(cursor))
        # df_feat = add_features(df)
        # df_cn = set_coordination(df_feat)
        # df_cn.to_pickle(name + '_cn.pkl')
        grouped_df, merged_df = group_merge_df(name)
        # merged_df.to_pickle(name + '_cn_merged.pkl')
        plot_violin(grouped_df, name)
        plot_xy(merged_df, name)
        # analyze_df(name)
        # plot_descs = ['coordination']
        # for plot_desc in plot_descs:
        #     df_desc, desc = getattr(AddDescriptor(name), plot_desc)()
        #     plot_xy(df_desc, name, desc)
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

