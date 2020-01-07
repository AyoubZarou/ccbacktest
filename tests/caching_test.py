import unittest
from ccbacktest.data.caching import get_diff_and_update


class DiffAndUpdateTest(unittest.TestCase):
    def test_all(self):
        a = [0, 1, 2, 8, 10, 20]
        self.assertEqual(get_diff_and_update([0, 1], a), ([], a))
        self.assertEqual(get_diff_and_update([-2, -1], a), ([[-2, -1]], [-2, -1, *a]))
        self.assertEqual(get_diff_and_update([30, 40], a), ([[30, 40]], [*a, 30, 40]))
        self.assertEqual(get_diff_and_update([0, 2], a), ([[1, 2]], [0, 8, 10, 20]))
        self.assertEqual(get_diff_and_update([1.5, 2], a), ([[1.5, 2]], [0, 1, 1.5, 8, 10, 20]))
        self.assertEqual(get_diff_and_update([1.5, 15], a), ([[1.5, 2], [8, 10]], [0, 1, 1.5, 20]))
        self.assertEqual(get_diff_and_update([0, 40], a), ([[1, 2], [8, 10], [20, 40]], [0, 40]))
        self.assertEqual(get_diff_and_update([-1, 40], a), ([[-1, 0], [1, 2], [8, 10], [20, 40]], [-1, 40]))
        self.assertEqual(get_diff_and_update([-1, 20], a), ([[-1, 0], [1, 2], [8, 10]], [-1, 20]))
        self.assertEqual(get_diff_and_update([0, 10], [1, 2]), ([[0, 1], [2, 10]], [0, 10]))
        self.assertEqual(get_diff_and_update([0, 10], []), ([[0, 10]], [0, 10]))
        self.assertEqual(get_diff_and_update([0, 10], [-1, 8]), ([[8, 10]], [-1, 10]))
        self.assertEqual(get_diff_and_update([0, 1], [5, 6]), ([[0, 1]], [0, 1, 5, 6]))

    def test_fail(self):
        self.assertRaises(AssertionError, get_diff_and_update, [0, 0], [])
        self.assertRaises(AssertionError, get_diff_and_update, [0], [4])
        self.assertRaises(AssertionError, get_diff_and_update, [0, -1], [4])


if __name__ == '__main__':
    unittest.main()
