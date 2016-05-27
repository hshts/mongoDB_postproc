import pymongo
import pandas as pd
from pandas.io import json
import matplotlib.pyplot as plt

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    pd.set_option('display.width', 1000)
    cns = ['eff_cn', 'VoronoiEd_cn', 'okeeffe_cn']
    props = ['hp', 'ht']
    elements = ['Li', 'Na', 'Mg', 'K', 'Ca', 'Al', 'Si']
    for cn in cns:
        for prop in props:
            # big_df = pd.read_pickle(prop + '-O_' + cn + '.pkl')
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
                            df.set_value(i, el + '-O', json.loads(row[cn])[el])
                        except KeyError:
                            for sp in json.loads(row[cn]).keys():
                                if el in sp:
                                    df.set_value(i, el + '-O', json.loads(row[cn])[sp])
                        except TypeError:
                            pass
                    if row['is_ordered'] < 1:
                        comps_to_remove.append(row['reduced_cell_formula'])
                df.drop(df.index[indexes_to_drop], inplace=True)
                for comp in comps_to_remove:
                    df.drop(df[df['reduced_cell_formula'] == comp].index, inplace=True)
                df_groupby = df.groupby(['reduced_cell_formula', 'is_' + prop], as_index=False).mean()
                bins = [2, 4, 6, 8, 10, 12]
                df_groupby['cn_bucket'] = pd.cut(df_groupby[el + '-O'], bins=bins)
                newdf = df_groupby[['is_' + prop, 'cn_bucket', el + '-O']].groupby(['is_' + prop, 'cn_bucket']).count()
                big_df = pd.concat([big_df, newdf], axis=1)
            print big_df
            print big_df.index.tolist()
            big_df.index.set_levels([['Low pressure/CN=', 'High pressure/CN='],
                                     ['(2-4)', '(4-6)', '(6-8)', '(8-10)', '(10-12)']], inplace=True)
            big_df.plot.bar(width=0.8)
            if cn == 'VoronoiEd_cn':
                name = 'Voronoi'
            elif cn == 'eff_cn':
                name = 'Effective'
            else:
                name = "O'Keeffe"
            plt.xlabel('Pressure/CN', fontsize=28)
            plt.ylabel('Number of compounds', fontsize=28)
            plt.title(prop[0].upper() + prop[1].upper() +
                      '/ambient conditions: number of M-O compounds in different ranges of average ' + name +
                      ' coordination numbers', fontsize=24)
            plt.show()

