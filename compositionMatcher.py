import pymongo
from pymatgen import Composition
from pymatgen.io.cif import CifParser

client = pymongo.MongoClient()
db = client.springer


def get_meta_from_structure(structure):
    """
    Used by `structure_to_mock_job`, to "fill out" a job document.
    :param structure: pymatgen structure object
    :return: (dict) structure metadata
    """
    comp = structure.composition
    elsyms = sorted(set([e.symbol for e in comp.elements]))
    meta = {'nsites': len(structure),
            'elements': elsyms,
            'nelements': len(elsyms),
            'formula': comp.formula,
            'reduced_cell_formula': comp.reduced_formula,
            'reduced_cell_formula_abc': Composition(comp.reduced_formula).alphabetical_formula,
            'anonymized_formula': comp.anonymized_formula,
            'chemsystem': '-'.join(elsyms),
            'is_ordered': structure.is_ordered,
            'is_valid': structure.is_valid(tol=0.50)}
    return meta

# ID's to verify: 'sd_0541484', 'sd_0457328'
if __name__ == '__main__':
    x = 0
    y = 0
    z = 0
    a = 0
    b = 0
    c = 0
    keys = []
    for doc in db['pauling_file_min_tags'].find().batch_size(75):
        if doc['key'] in ['sd_1301665', 'sd_0456987', 'sd_1125437', 'sd_1125436', 'sd_1500010']:
            continue
        x += 1
        if x % 1000 == 0:
            print x
        if (doc['metadata']['_Springer']['geninfo']['Refined Formula']).replace(u'\u2013', '-') != '-':
            form = doc['metadata']['_Springer']['geninfo']['Refined Formula']
        else:
            form = doc['metadata']['_Springer']['geninfo']['Alphabetic Formula']
        form = form.replace('[', '')
        form = form.replace(']', '')
        try:
            redform = Composition(form).reduced_formula
        except:
            print 'Could not parse composition for key:{} with composition:{}'.format(doc['key'], form)
            y += 1
            continue
        alphaform_comp = Composition(redform).alphabetical_formula
        alphaform_frac = Composition(alphaform_comp).fractional_composition
        structure_comp = doc['metadata']['_structure']['reduced_cell_formula_abc']
        structure_frac = Composition(structure_comp).fractional_composition
        if alphaform_frac != structure_frac and not alphaform_frac.almost_equals(structure_frac, 0.15):
            for document in db['pauling_file'].find({'key': doc['key']}):
                try:
                    struct_lst = CifParser.from_string(document['cif_string']).get_structures()
                except:
                    pass
                if len(struct_lst) > 1:
                    matched = False
                    for struct in struct_lst[1:]:
                        another_frac = Composition(struct.composition).fractional_composition
                        if alphaform_frac == another_frac or alphaform_frac.almost_equals(another_frac, 0.15):
                            matched = True
                            # '''
                            db['pauling_file'].update({'key': doc['key']}, {'$set': {'structure': struct.as_dict()}})
                            db['pauling_file'].update({'key': doc['key']}, {
                                '$set': {'metadata._structure': get_meta_from_structure(struct)}})
                            db['pauling_file'].update({'key': doc['key']},
                                                      {'$addToSet': {'remarks': 'used alternate CIF structure'}})
                            # '''
                            break
                    if matched is False:
                        # '''
                        db['pauling_file'].update({'key': doc['key']}, {'$addToSet': {
                            'errors': 'structural composition and refined/alphabetic formula do not match'}})
                        # '''
                        print '-----'
                        print doc['key']
                        print '{} and {} do not match'.format(alphaform_comp, structure_comp)
                        z += 1
                        if doc['is_hp_dataset']:
                            a += 1
                        if doc['is_ht_dataset']:
                            b += 1
                        if 'cif_string_old' in doc['metadata']['_Springer']:
                            c += 1
    print 'Total number of unparsable compositions = {}, Total number of unmatched composition = {}, in HP dataset = ' \
          '{}, in HT datatset = {}, and those with corrected CIFs = {}'.format(y, z, a, b, c)
