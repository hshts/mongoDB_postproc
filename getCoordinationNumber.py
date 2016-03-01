import pymongo
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder
from pymatgen import Structure
from pymatgen.io.cif import CifParser

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    d = 0
    for doc in db['pauling_file_unique_Parse'].find({'metadata._Springer.geninfo.Standard Formula': 'Li2O'}).limit(1):
        if 'structure' in doc:
            coordination_numbers = []
            print doc['metadata']['_Springer']['geninfo']['Standard Formula']
            for siteno in range(len(doc['structure']['sites'])):
                print VoronoiCoordFinder(Structure.from_dict(doc['structure'])).get_coordination_number(siteno)
                coordination = 0
                weights = VoronoiCoordFinder(Structure.from_dict(doc['structure'])).get_voronoi_polyhedra(
                    siteno).values()
                max_weights_size = len(weights)
                print weights
                max_weight = max(weights)
                for weight in weights:
                    if weight > 0.80 * max_weight:
                        coordination += 1
                print 'Coordination = {}'.format(coordination)
                print '-----------'
                coordination_numbers.append(coordination)
            print coordination_numbers


            # print EffectiveCoordFinder(CifParser.from_string(doc['cif_string']).get_structures()[0]).get_cation_CN()
