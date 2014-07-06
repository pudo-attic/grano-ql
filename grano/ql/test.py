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

    def test_author_nested(self):
        res = query({'author': None}).to_dict()
        assert 'author' in res, res
        assert res['author'] is not None, res
        assert res['author']['login'] is not None, res

    def test_project_nested(self):
        res = query({'project': None}).to_dict()
        assert 'project' in res, res
        assert res['project'] is not None, res
        assert res['project']['slug'] is not None, res
        assert '__parent_id' not in res['project'], res

if __name__ == '__main__':
    unittest.main()
