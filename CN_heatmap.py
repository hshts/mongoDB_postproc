import pymongo
import pandas as pd
from pandas.io import json
import matplotlib.pyplot as plt
import seaborn as sns

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    cns = ['VoronoiEd_cn', 'eff_cn', 'okeeffe_cn']
    props = ['hp', 'ht']
    elements = ['Li', 'Na', 'Mg', 'K', 'Ca', 'Al', 'Si']
    for cn in cns:
        for prop in props:
            # big_df = pd.read_pickle(prop + '-O.pkl')
            # '''
            print '-------------------'
            big_df = pd.DataFrame()
            for el in elements:
                indexes_to_drop = []
                df = pd.read_pickle(prop + '_cn.pkl')
                comps_to_remove = []
                for i, row in df.iterrows():
                    if el not in row['metadata']['_structure']['elements'] \
                            or 'O' not in row['metadata']['_structure']['elements']:
                        indexes_to_drop.append(i)
                    if el in row['metadata']['_structure']['elements']:
                        try:
                            df.set_value(i, el + '_cn', json.loads(row[cn])[el])
                        except KeyError:
                            for sp in json.loads(row[cn]).keys():
                                if el in sp:
                                    df.set_value(i, el + '_cn', json.loads(row[cn])[sp])
                        except TypeError:
                            pass
                    if row['is_ordered'] < 1:
                        comps_to_remove.append(row['reduced_cell_formula'])
                df.drop(df.index[indexes_to_drop], inplace=True)
                for comp in comps_to_remove:
                    df.drop(df[df['reduced_cell_formula'] == comp].index, inplace=True)
                df_groupby = df.groupby(['reduced_cell_formula', 'is_' + prop], as_index=False).mean()
                # Find all compounds with CN > 8
                # print df_groupby[df_groupby[el + '_cn'] > 8]
                # for i, row in df_groupby[df_groupby[el + '_cn'] > 8].iterrows():
                #     print df_groupby.loc[df_groupby['reduced_cell_formula'] == row['reduced_cell_formula']]
                #     print df.loc[df['reduced_cell_formula'] == row['reduced_cell_formula']]
                newdf = df_groupby[['is_' + prop, el + '_cn']].groupby('is_' + prop).mean()
                big_df[el + '-O'] = newdf[el + '_cn'].tolist()
                print big_df
            big_df = big_df.transpose()
            if prop == 'hp':
                big_df.columns = ['Low pressure', 'High pressure']
            else:
                big_df.columns = ['Low temperature', 'High temperature']
            # big_df.to_pickle(prop + '-O_' + cn + '.pkl')
            print big_df
            sns.set(font_scale=2)
            sns.heatmap(big_df)
            if cn == 'VoronoiEd_cn':
                name = 'Voronoi'
            elif cn == 'eff_cn':
                name = 'Effective'
            else:
                name = "O'Keeffe"
            if prop == 'hp':
                plt.xlabel('Pressure (GPa)', fontsize=28)
                plt.ylabel('Bonds', fontsize=28)
                plt.title('Average ' + name + ' coordination number', fontsize=28)
            else:
                plt.xlabel('Temperature (K)', fontsize=28)
                plt.ylabel('Bonds', fontsize=28)
                plt.title('Average ' + name + ' coordination number', fontsize=28)
            plt.show()
