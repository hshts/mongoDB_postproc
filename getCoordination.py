import pymongo
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder
from collections import defaultdict
from chronograph.chronograph import Chronograph

client = pymongo.MongoClient()
db = client.springer

def getcoordination(structure):
    species = []
    species_coord_dictlst = defaultdict(list)
    species_coord = {}
    struct_dict = structure.as_dict()
    no_of_sites = len(struct_dict['sites'])
    for specie in struct_dict['sites']:
        species.append(specie['label'])
    for siteno in range(no_of_sites):
        # print 'Voronoi coord number for {} = {}'.format(species[siteno],
        #                                                 VoronoiCoordFinder(structure).get_coordination_number(siteno))
        coordination = 0
        try:
            weights = VoronoiCoordFinder(structure).get_voronoi_polyhedra(siteno).values()
        except AttributeError:
            # print struct_dict['sites'][siteno]['species']
            continue
        # print 'Weights for {} = {}'.format(species[siteno], weights)
        max_weight = max(weights)
        for weight in weights:
            if weight > 0.74 * max_weight:
                coordination += 1
        species_coord_dictlst[species[siteno]].append(coordination)
        # print 'Calculated coordination for {} = {}'.format(species[siteno], coordination)
        # print '-----------'
    for el in species_coord_dictlst:
        species_coord[el] = max(species_coord_dictlst[el])
    return species_coord
