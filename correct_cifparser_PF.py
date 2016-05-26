import pymongo
from pymatgen.io.cif import CifParser, CifFile
import re
import json
from pymatgen import DummySpecie, Composition, Structure
from compositionMatcher import get_meta_from_structure

client = pymongo.MongoClient()
db = client.springer

if __name__ == '__main__':
    x = 0
    for doc in db['pauling_file'].find({'structure': {'$exists': True}}).batch_size(75):
        if doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436', 'sd_1500010']:
            continue
        x += 1
        if x % 1000 == 0:
            print x
        try:
            new_struct = CifParser.from_string(doc['cif_string']).get_structures()[0].as_dict()
        except Exception as e:
            print e
            continue
        db['pauling_file'].update({'key': doc['key']}, {'$set': {'structure': new_struct}})
    for doc in db['pauling_file'].find({'structure': {'$exists': True}}).batch_size(75):
        if doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436', 'sd_1500010']:
            continue
        x += 1
        if x % 1000 == 0:
            print x
        meta_dict = get_meta_from_structure(Structure.from_dict(doc['structure']))
        db['pauling_file'].update({'key': doc['key']}, {'$set': {'metadata._structure': meta_dict}})



    '''

    def _get_structure(self, data, primitive, substitution_dictionary=None):
        """
        Generate structure from part of the cif.
        """
        # Symbols often representing
        #common representations for elements/water in cif files
        special_symbols = {"D":"D", "Hw":"H", "Ow":"O", "Wat":"O", "wat": "O"}
        elements = [el.symbol for el in Element]

        lattice = self.get_lattice(data)
        self.symmetry_operations = self.get_symops(data)
        oxi_states = self.parse_oxi_states(data)

        coord_to_species = OrderedDict()

        def parse_symbol(sym):

            if substitution_dictionary:
                return substitution_dictionary.get(sym)
            else:
                m = re.findall(r"w?[A-Z][a-z]*", sym)
                if m and m != "?":
                    if len(m) > 1:     # if more than one specie on site
                        return m
                    return m[0]
                return ""

        def get_matching_coord(coord):
            for op in self.symmetry_operations:
                c = op.operate(coord)
                for k in coord_to_species.keys():
                    if np.allclose(pbc_diff(c, k), (0, 0, 0),
                                   atol=self._site_tolerance):
                        return tuple(k)
            return False

        for i in range(len(data["_atom_site_label"])):

            x = str2float(data["_atom_site_fract_x"][i])
            y = str2float(data["_atom_site_fract_y"][i])
            z = str2float(data["_atom_site_fract_z"][i])

            try:
                occu = str2float(data["_atom_site_occupancy"][i])
            except (KeyError, ValueError):
                occu = 1

            # In the Pauling file, CIF files have switched the keys "_atom_site_label" and "_atom_site_type_symbol", and
            # partially occupied sites have " + " in the key "_atom_site_type_symbol"

            # if ' + ' in data["_atom_site_type_symbol"][i]:
            #     symbol = parse_symbol(data["_atom_site_label"][i])
            # else:
            symbol = parse_symbol(data["_atom_site_type_symbol"][i])


            # if parse_symbol() returns list, i.e. more than one specie on site
            if type(symbol) == list:

                els_occu = {}

                # parse symbol to get element names and occupancy and store in "els_occu"
                elemocc_lst = []
                symbol_str = data["_atom_site_type_symbol"][i]
                elemocc_brackets = symbol_str.split('+')
                for elocc in elemocc_brackets:
                    elemocc_lst.append(re.sub('\([0-9]*\)', '', elocc.strip()))
                for elocc_idx in range(len(elemocc_lst)):
                    els_occu[str(re.findall('\D+', elemocc_lst[elocc_idx].strip())[1]).replace('<sup>', '')] = float(
                        '0' + re.findall('\.?\d+', elemocc_lst[elocc_idx].strip())[1])

                if occu > 0:
                    coord = (x, y, z)
                    if coord not in coord_to_species:
                        coord_to_species[coord] = Composition(els_occu)
                    else:
                        coord_to_species[coord] += els_occu
            else:
                if symbol:
                    if symbol not in elements and symbol not in special_symbols:
                        symbol = symbol[:2]
                else:
                    continue
                # make sure symbol was properly parsed from _atom_site_label
                # otherwise get it from _atom_site_type_symbol
                try:
                    if symbol in special_symbols:
                        get_el_sp(special_symbols.get(symbol))
                    else:
                        Element(symbol)
                except (KeyError, ValueError):
                    # sometimes the site doesn't have the type_symbol.
                    # we then hope the type_symbol can be parsed from the label
                    if "_atom_site_type_symbol" in data.data.keys():
                        symbol = data["_atom_site_type_symbol"][i]

                if oxi_states is not None:
                    if symbol in special_symbols:
                        el = get_el_sp(special_symbols.get(symbol) +
                                       str(oxi_states[symbol]))
                    else:
                        el = Specie(symbol, oxi_states.get(symbol, 0))
                else:

                    el = get_el_sp(special_symbols.get(symbol, symbol))

                if occu > 0:
                    coord = (x, y, z)
                    match = get_matching_coord(coord)
                    if not match:
                        coord_to_species[coord] = Composition({el: occu})
                    else:
                        coord_to_species[match] += {el: occu}

        if any([sum(c.values()) > 1 for c in coord_to_species.values()]):
            warnings.warn("Some occupancies sum to > 1! If they are within "
                          "the tolerance, they will be rescaled.")

        allspecies = []
        allcoords = []

        if coord_to_species.items():
            for species, group in groupby(
                    sorted(list(coord_to_species.items()), key=lambda x: x[1]),
                    key=lambda x: x[1]):
                tmp_coords = [site[0] for site in group]

                coords = self._unique_coords(tmp_coords)

                allcoords.extend(coords)
                allspecies.extend(len(coords) * [species])

            # rescale occupancies if necessary
            for i, species in enumerate(allspecies):
                totaloccu = sum(species.values())
                if 1 < totaloccu <= self._occupancy_tolerance:
                    allspecies[i] = species / totaloccu

        if allspecies and len(allspecies) == len(allcoords):
            struct = Structure(lattice, allspecies, allcoords)
            struct = struct.get_sorted_structure()

            if primitive:
                struct = struct.get_primitive_structure()
                struct = struct.get_reduced_structure()
            return struct
    '''