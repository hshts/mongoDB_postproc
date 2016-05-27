import pymongo
from pymatgen import Structure, MPRester
from pymatgen.analysis.structure_analyzer import VoronoiCoordFinder
from collections import defaultdict
import numpy as np
from pymatgen.io.cif import CifParser
from scipy.stats import mode
import math
import re

client = pymongo.MongoClient()
db = client.springer


def get_avg_cns(sp_cnlst):
    """
    Get the average coordination for each unique specie over all sites in a structure.

    :param sp_cnlst: (dict) A dictionary with keys corresponding to unique species and the values as lists to the atom's
        ECoN coordination number over all sites with the same specie in the structure
    :return: (dict) A dictionary with keys corresponding to unique species and the values to the cation's ECoN
        coordination number averaged over all polyhedra with the same specie in the structure
    """
    avg_cns = {}
    for specie in sp_cnlst:
        avg_cns[specie] = np.mean(sp_cnlst[specie])
    return avg_cns


def get_mode_cns(sp_cnlst):
    """
    Get the mode coordination for each unique specie over all sites in a structure.

    :param sp_cnlst: (dict) A dictionary with keys corresponding to unique species and the values as lists to the atom's
        ECoN coordination number over all sites with the same specie in the structure
    :return: (dict) A dictionary with keys corresponding to unique species and the values to the mode of cation's ECoN
        coordination number over all polyhedra with the same specie in the structure
    """
    avg_cns = {}
    for specie in sp_cnlst:
        avg_cns[specie] = mode(sp_cnlst[specie])[0][0]
    return avg_cns


def get_cation_weighted_avg(sp_cn, structure):
    total_cation_coords = 0
    total_cation_amts = 0
    cations = []
    anions = ['N', 'O', 'F', 'S', 'Cl', 'Se', 'Br', 'Sb', 'Te', 'I']
    # TODO: If only disordered sites contain cations, then this would fail. eg: sd_1212287
    for specie in sp_cn:
        sps_lst = re.findall(r'[A-Z][a-z]*', specie)
        for el in sps_lst:
            if el not in anions:
                cations.append(specie)
                break
    if len(cations) == 0:
        return
    el_amt = structure.composition.get_el_amt_dict()
    for cation in cations:
        try:
            total_cation_coords += (el_amt[cation] * sp_cn[cation])
        except KeyError:    # handle mixed occcupancy sites
            for sp in sp_cn:
                if cation in sp:
                    cation_key = sp
                    break
            total_cation_coords += (el_amt[cation] * sp_cn[cation_key])
        total_cation_amts += el_amt[cation]
    return total_cation_coords/total_cation_amts


class VoronoiCoordFinder_edited(object):

    def __init__(self, structure):
        self._structure = structure

    def get_cns(self):
        species = []
        species_coord_dictlst = defaultdict(list)
        struct_dict = self._structure.as_dict()
        no_of_sites = len(struct_dict['sites'])
        for specie in struct_dict['sites']:
            species.append(specie['label'])
        for siteno in range(no_of_sites):
            coordination = 0
            try:
                weights = VoronoiCoordFinder(self._structure).get_voronoi_polyhedra(siteno).values()
            except RuntimeError as e:
                print e
                continue
            max_weight = max(weights)
            for weight in weights:
                if weight > 0.50 * max_weight:
                    coordination += 1
            species_coord_dictlst[species[siteno]].append(coordination)
        return species_coord_dictlst


def calculate_weighted_avg(bonds):
    """
    Get the weighted average bond length given by the effective coordination number formula in Hoppe (1979)

    :param bonds: (list) list of floats that are the bond distances between a cation and its peripheral ions
    :return: (float) exponential weighted average
    """

    minimum_bond = min(bonds)
    weighted_sum = 0.0
    total_sum = 0.0
    for entry in bonds:
        weighted_sum += entry*math.exp(1 - (entry/minimum_bond)**6)
        total_sum += math.exp(1-(entry/minimum_bond)**6)
    return weighted_sum/total_sum


class EffectiveCoordFinder(object):

    """

    Finds the average effective coordination number for each cation in a given structure. It
    finds all cation-centered polyhedral in the structure, calculates the bond weight for each peripheral ion in the
    polyhedral, and sums up the bond weights to obtain the effective coordination number for each polyhedral. It then
    averages the effective coordination of all polyhedral with the same cation at the central site.

    We use the definition from Hoppe (1979) to calculate the effective coordination number of the polyhedrals:

    Hoppe, R. (1979). Effective coordination numbers (ECoN) and mean Active fictive ionic radii (MEFIR).
    Z. Kristallogr. , 150, 23-52.
    ECoN = sum(exp(1-(l_i/l_av)^6)), where l_av = sum(l_i*exp(1-(1_i/l_min)))/sum(exp(1-(1_i/l_min)))

    """

    def __init__(self, structure):
        self._structure = structure

    def get_cns(self, radius=10.0):
        """
        Get all specie-centered polyhedra for a structure

        :param radius: (float) distance in Angstroms for bond cutoff
        :return: (dict) A default dictionary with keys corresponding to different cations and the values to the cation's
            ECoN coordination numbers
        """
        sp_cns = defaultdict(list)
        for site in self._structure.sites:
            all_bond_lengths = []
            bond_weights = []
            for neighbor in self._structure.get_neighbors(site, radius):  # entry = (site, distance)
                if neighbor[1] < radius:
                    all_bond_lengths.append(neighbor[1])

            # if len(all_bond_lengths) == 0:
            #     continue
            weighted_avg = calculate_weighted_avg(all_bond_lengths)

            for bond in all_bond_lengths:
                bond_weight = math.exp(1-(bond/weighted_avg)**6)
                bond_weights.append(round(bond_weight, 3))

            sp_cns[site.species_string].append(sum(bond_weights))
        return sp_cns
