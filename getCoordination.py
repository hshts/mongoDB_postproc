import pymongo
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder

client = pymongo.MongoClient()
db = client.springer


def getcoordination(structure):
    species = []
    species_coord = {}
    struct_dict = structure.as_dict()
    no_of_sites = len(struct_dict['sites'])
    for specie in struct_dict['sites']:
        species.append(specie['label'])
    for siteno in range(no_of_sites):
        # print 'Voronoi coord number for {} = {}'.format(species[siteno],
        #                                                 VoronoiCoordFinder(structure).get_coordination_number(siteno))
        coordination = 0
        weights = VoronoiCoordFinder(structure).get_voronoi_polyhedra(siteno).values()
        # print 'Weights for {} = {}'.format(species[siteno], weights)
        max_weight = max(weights)
        for weight in weights:
            if weight > 0.74 * max_weight:
                coordination += 1
        if species_coord.has_key(species[siteno]):
            if species_coord[species[siteno]] == coordination:
                continue
        species_coord[species[siteno]] = coordination
        # print 'Calculated coordination for {} = {}'.format(species[siteno], coordination)
        # print '-----------'
    return species_coord
