import pymongo
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder
from pymatgen import Structure
from pymatgen.io.cif import CifParser
from utils import *

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find({'metadata._Springer.geninfo.Standard Formula': 'Li2O'}).limit(1):
        if 'structure' in doc:
            print doc['metadata']['_Springer']['geninfo']['Standard Formula']
            for siteno in range(len(doc['structure']['sites'])):
                print VoronoiCoordFinder(
                    CifParser.from_string(doc['cif_string']).get_structures()[0]).get_voronoi_polyhedra(siteno).values()
            print EffectiveCoordFinder(CifParser.from_string(doc['cif_string']).get_structures()[0]).get_cation_CN()

