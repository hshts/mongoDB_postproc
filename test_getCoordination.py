import unittest
from getcoordination import getcoordination


class Testgetcoordination(unittest.TestCase):
    def test_Li2O(self):
        self.assertEqual(getcoordination('test_structures/Li2O_mp-1960_conventional_standard.cif'), {'Li': 4, 'O': 8})

    def test_PbTe(self):
        self.assertEqual(getcoordination('test_structures/TePb_mp-19717_conventional_standard.cif'), {'Pb': 6, 'Te': 6})

    def test_ZnSb(self):
        self.assertEqual(getcoordination('test_structures/ZnSb_mp-753_conventional_standard.cif'), {'Zn': 4, 'Sb': 4})

    def test_ZnS(self):
        self.assertEqual(getcoordination('test_structures/ZnS_mp-10695_conventional_standard.cif'), {'Zn': 4, 'S': 4})

    def test_CsCl(self):
        self.assertEqual(getcoordination('test_structures/CsCl_mp-573697_conventional_standard.cif'), {'Cs': 6, 'Cl': 6})


if __name__ == '__main__':
    unittest.main()
