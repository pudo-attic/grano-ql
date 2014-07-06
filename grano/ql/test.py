import unittest

from grano.test.util import make_test_app
from grano.ql.query import run as query


class TestQuery(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = make_test_app()

    def test_single_entity_id(self):
        res = query({'id': None}).to_dict()
        assert 'id' in res, res
        assert res['id'] is not None, res

    def test_list_entity_id(self):
        res = query([{'id': None}]).to_dict()
        assert len(res), res
        assert 'id' in res[0], res
        assert res[0]['id'] is not None, res

    def test_default_fields(self):
        res = query({'*': None}).to_dict()
        assert 'id' in res, res
        assert res['id'] is not None, res
        assert res['status'] is not None, res

if __name__ == '__main__':
    unittest.main()
