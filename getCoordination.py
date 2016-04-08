import pymongo
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder
from collections import defaultdict
import numpy as np
from scipy.stats import mode

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
        # print 'getcoordination.py - species = {}'.format(struct_dict['sites'][siteno]['species'])
        # try:
        weights = VoronoiCoordFinder(structure).get_voronoi_polyhedra(siteno).values()
        # print VoronoiCoordFinder(structure).get_coordinated_sites(siteno)
        # except AttributeError:
        #     print 'Attribute error for {}'.format(struct_dict['sites'][siteno]['species'])
        #     continue
        # print 'Weights for {} = {}'.format(species[siteno], weights)
        max_weight = max(weights)
        for weight in weights:
            if weight > 0.74 * max_weight:
                coordination += 1
        species_coord_dictlst[species[siteno]].append(coordination)
        # print 'Calculated coordination for {} = {}'.format(species[siteno], coordination)
        # print '-----------'
    return species_coord_dictlst


def get_mean_specie_sites(coords_defaultdict):
    species_coord = {}
    for el in coords_defaultdict:
        species_coord[el] = np.average(coords_defaultdict[el])
    return species_coord


def get_mode_specie_sites(coords_defaultdict):
    species_coord = {}
    for el in coords_defaultdict:
        species_coord[el] = mode(coords_defaultdict[el])[0]
    return species_coord
