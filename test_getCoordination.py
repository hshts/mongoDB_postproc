import unittest
from getcoordination import getcoordination


class Testgetcoordination(unittest.TestCase):
    def test_Li2O(self):
        self.assertEqual(getcoordination('test_structures/Li2O_mp-1960_conventional_standard.cif'),
                         [4, 4, 4, 4, 4, 4, 4, 4, 8, 8, 8, 8])


if __name__ == '__main__':
    unittest.main()
