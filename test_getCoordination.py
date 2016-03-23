import unittest
from getcoordination import getcoordination
from pymatgen import Structure


class Testgetcoordination(unittest.TestCase):
    def test_Al(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Al_mp-134_conventional_standard.cif')),
                         {'Al': 12})

    def test_Cu(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Cu_mp-30_conventional_standard.cif')),
                         {'Cu': 12})

    def test_Li(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Li_mp-135_conventional_standard.cif')),
                         {'Li': 8})

    def test_Mg(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Mg_mp-153_conventional_standard.cif')),
                         {'Mg': 12})

    def test_Zn(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Zn_mp-79_conventional_standard.cif')),
                         {'Zn': 6})

    def test_Zr(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Zr_mp-131_conventional_standard.cif')),
                         {'Zr': 12})

    def test_Li2O(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/Li2O_mp-1960_conventional_standard.cif')),
                         {'Li': 4, 'O': 8})

    def test_PbTe(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/TePb_mp-19717_conventional_standard.cif')),
            {'Pb': 6, 'Te': 6})

    def test_ZnSb(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/ZnSb_mp-753_conventional_standard.cif')),
                         {'Zn': 4, 'Sb': 4})

    def test_ZnS(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/ZnS_mp-10695_conventional_standard.cif')),
                         {'Zn': 4, 'S': 4})

    def test_CsCl(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/CsCl_mp-22865_conventional_standard.cif')),
            {'Cs': 8, 'Cl': 8})

    def test_CoSb3(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/CoSb3_mp-1317_conventional_standard.cif')),
            {'Co': 6, 'Sb': 4})

    def test_Co2NiGa(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/GaCo2Ni_mp-20551_conventional_standard.cif')),
            {'Co': 12, 'Ga': 12, 'Ni': 12})

    def test_Zn8Cu5(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/Zn8Cu5_mp-1368_conventional_standard.cif')),
            {'Zn': 8, 'Cu': 5})

    def test_ZrNiSn(self):
        self.assertEqual(
            getcoordination(Structure.from_file('test_structures/ZrNiSn_mp-924129_conventional_standard.cif')),
            {'Zr': 4, 'Ni': 8, 'Sn': 4})

    def test_CaF2(self):
        self.assertEqual(getcoordination(Structure.from_file('test_structures/CaF2_mp-2741_conventional_standard.cif')),
                         {'Ca': 8, 'F': 4})




if __name__ == '__main__':
    unittest.main()
