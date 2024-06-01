import unittest


def assert_items_equal(result, expected):
    case = unittest.TestCase()
    case.maxDiff = None
    case.assertCountEqual(result, expected)
