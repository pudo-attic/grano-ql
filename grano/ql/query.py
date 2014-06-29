from itertools import groupby
from sqlalchemy.orm import aliased

from grano.lib.exc import BadRequest
from grano.model import Entity, Project, Account, Relation
from grano.model import db
from grano.ql.parser import QueryNode


PARENT_ID = '__parent_id'


class FieldQuery(object):

    def __init__(self, parent, name, qn):
        self.parent = parent
        self.name = name
        self.qn = qn

    @property
    def column(self):
        return getattr(self.parent.alias, self.name)

    def filter(self, q):
        if self.qn is not None and self.qn.value is not None:
            q = q.filter(self.column == self.qn.value)
        return q

    def add_columns(self, q):
        return q.add_columns(self.column)

    def to_dict(self):
        return {
            'name': self.name,
            'qn': self.qn
        }


class ObjectQuery(object):

    model = {}
    domain_object = None
    default_fields = []

    def __init__(self, parent, name, qn):
        self.qn = self.expand_query(qn)
        self.alias = aliased(self.domain_object)
        self.parent = parent
        self.name = name
        self.children = {}
        self.joiners = {}

        # instantiate the model:
        for name, cls in self.model.items():
            if isinstance(cls, tuple):
                cls, joiner = cls
                self.joiners[name] = joiner
            qn = None
            for qn_ in self.qn.children:
                if qn_.name == name:
                    qn = qn_
            self.children[name] = cls(self, name, qn)

    def expand_query(self, qn):
        """ Handle wildcard queries. """
        qn = self.patch_qn(qn)
        value = qn.value
        if value is None:
            value = {'*': None}
        if '*' in value and value.pop('*') is None:
            for name in self.default_fields:
                if name not in value:
                    value[name] = None
        self.fake_id = 'id' not in value
        if self.fake_id:
            value['id'] = None
        return qn.update(value)

    def patch_qn(self, qn):
        return qn

    @property
    def root(self):
        """ Get the root level of the query. """
        if self.parent is None:
            return self
        return self.parent.root

    @property
    def children_objects(self):
        """ Iterate through all children that are objects. """
        for name, child in self.children.items():
            if isinstance(child, ObjectQuery):
                yield name, child

    def filter(self, q):
        """ Apply the joins and filters specified on this level of the
        query. """
        for name, joiner in self.joiners.items():
            child = self.children[name]
            q = q.join(child.alias, joiner(self.alias))
        for child in self.qn.children:
            if child.value is None:
                continue
            if child.name not in self.children:
                raise BadRequest('Unknown field: %s' % child.name)
            q = self.children[child.name].filter(q)
        return q

    def add_columns(self, q):
        """ Define the columns to be retrieved when this is the active
        level of the query. """
        for name, child in self.children.items():
            if isinstance(child, ObjectQuery):
                continue
            for qn in self.qn.children:
                if qn.value is None and qn.name == name:
                    q = child.add_columns(q)
        return q

    def _make_object(self, record):
        """ Combine the results of a query and return object into
        a result object. """
        data = dict([(k, v) for (k, v) in self.qn.value.items() if k != '*'])
        if record is not None:
            res = dict(zip(record.keys(), record))
            data[PARENT_ID] = res.get(PARENT_ID)
            for qn in self.qn.children:
                if qn.name == 'id' and self.fake_id:
                    continue
                if qn.value is None:
                    data[qn.name] = res.get(qn.name)
        return data

    @property
    def query(self):
        """ Construct a query for this level of the query. """
        q = db.session.query()
        q = self.root.filter(q)
        q = self.add_columns(q)
        if self.parent is not None:
            col = self.parent.alias.id.label(PARENT_ID)
            q = q.add_columns(col)
        # TODO: temp
        q = q.limit(25)
        return q

    def run(self):
        """ Actually run the query, recursively. """
        results = [self._make_object(r) for r in self.query]
        for name, child in self.children_objects:
            for parent_id, nested in child.run():
                for result in results:
                    if parent_id == result['id']:
                        result[name] = nested
        for parent_id, results in groupby(results,
                                          lambda r: r.pop(PARENT_ID)):
            results = list(results)
            if not self.qn.as_list:
                results = results.pop()
            yield parent_id, results

    def to_dict(self):
        # TODO: this is just for debug.
        return {
            #'query_node': self.qn,
            #'children': self.children,
            'result': self.run().next()[1]
        }


class ProjectQuery(ObjectQuery):

    domain_object = Project
    model = {
        'id': FieldQuery,
        'slug': FieldQuery,
        'label': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }
    default_fields = ['slug', 'label']

    def patch_qn(self, qn):
        if isinstance(qn.value, basestring):
            qn.update({'slug': qn.value})
        return qn


class AccountQuery(ObjectQuery):

    domain_object = Account
    model = {
        'id': FieldQuery,
        'login': FieldQuery,
        'full_name': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }
    default_fields = ['login', 'full_name']

    def patch_qn(self, qn):
        if isinstance(qn.value, basestring):
            qn.update({'login': qn.value})
        return qn


class EntityQuery(ObjectQuery):

    domain_object = Entity
    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery,
        'project': (ProjectQuery, lambda p: p.project),
        'author': (AccountQuery, lambda p: p.author)
    }
    default_fields = ['id', 'status', 'project', 'author']


def run(query):
    qn = QueryNode(None, query)
    eq = EntityQuery(None, None, qn)
    return eq
