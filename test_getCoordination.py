import unittest
from getcoordination import getcoordination


class Testgetcoordination(unittest.TestCase):
    def test_Li2O(self):
        self.assertEqual(getcoordination('test_structures/Li2O_mp-1960_conventional_standard.cif'), {'Li': 4, 'O': 8})

    def test_PbTe(self):
        self.assertEqual(getcoordination('test_structures/TePb_mp-19717_conventional_standard.cif'), {'Pb': 6, 'Te': 6})

if __name__ == '__main__':
    unittest.main()
