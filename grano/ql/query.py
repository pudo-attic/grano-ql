from itertools import groupby
from sqlalchemy.orm import aliased

from grano.lib.exc import BadRequest
from grano.model import Entity, Project, Account, Relation
from grano.model import Schema
from grano.model import db
from grano.ql.parser import QueryNode

# TODO: properties stub query object
# TODO: and/or query branches / list queries


PARENT_ID = '__parent_id'


class FieldQuery(object):

    def __init__(self, parent, name, node):
        self.parent = parent
        self.name = name
        self.node = node

    @property
    def column(self):
        return getattr(self.parent.alias, self.name)

    def filter(self, q):
        if self.node is not None and self.node.value is not None:
            q = q.filter(self.column == self.node.value)
        return q

    def add_columns(self, q):
        return q.add_columns(self.column)

    def to_dict(self):
        return {
            'name': self.name,
            'node': self.node
        }


class ObjectQuery(object):

    model = {}
    domain_object = None
    default_fields = []

    def __init__(self, parent, name, node):
        self.node = self.expand_query(name, node)
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
            for node in self.node.children:
                if node.name == name:
                    self.children[name] = cls(self, name, node)

    def expand_query(self, name, node):
        """ Handle wildcard queries. """
        node = self.patch_node(node)
        value = node.value if node else None
        if value is None:
            value = {'*': None}
        if '*' in value and value.pop('*') is None:
            for name in self.default_fields:
                if name not in value:
                    value[name] = None
        if 'id' not in value:
            value['id'] = None
        if node is not None and node.as_list:
            value = [value]
        return QueryNode(name, value)

    def patch_node(self, node):
        return node

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
            if name in self.children:
                child = self.children[name]
                q = q.join(child.alias, joiner(self.alias))
        for child in self.node.children:
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
            for node in self.node.children:
                if node.value is None and node.name == name:
                    q = child.add_columns(q)
        return q

    def _make_object(self, record):
        """ Combine the results of a query and return object into
        a result object. """
        data = dict([(k, v) for (k, v) in self.node.value.items() if k != '*'])
        if record is not None:
            res = dict(zip(record.keys(), record))
            data[PARENT_ID] = res.get(PARENT_ID)
            for node in self.node.children:
                #if node.name == 'id' and self.fake_id:
                #    del data[node.name]
                if node.value is None:
                    data[node.name] = res.get(node.name)
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
        if self.parent is None:
            q = q.limit(25)
        return q

    def run(self):
        """ Actually run the query, recursively. """
        results = [self._make_object(r) for r in self.query]
        for name, child in self.children_objects:
            if name not in [node.name for node in self.node.children]:
                continue
            for parent_id, nested in child.run():
                for result in results:
                    if parent_id == result['id']:
                        result[name] = nested
        for parent_id, results in groupby(results,
                                          lambda r: r.pop(PARENT_ID)):
            results = list(results)
            if not self.node.as_list:
                results = results.pop()
            yield parent_id, results

    def to_dict(self):
        return self.run().next()[1]


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

    def patch_node(self, node):
        if node is not None and isinstance(node.value, basestring):
            node.update({'slug': node.value})
        return node


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

    def patch_node(self, node):
        if node is not None and isinstance(node.value, basestring):
            node.update({'login': node.value})
        return node


class SchemaQuery(ObjectQuery):

    domain_object = Schema
    model = {
        'id': FieldQuery,
        'name': FieldQuery,
        'label': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }
    default_fields = ['name', 'label']

    def patch_node(self, node):
        if node is not None and isinstance(node.value, basestring):
            node.update({'name': node.value})
        return node


class PropertyQuery(ObjectQuery):

    domain_object = Project
    model = {
        'id': FieldQuery,
        'value': FieldQuery,
        'source_url': FieldQuery,
        'active': FieldQuery
    }
    default_fields = ['value', 'source_url']

    def patch_node(self, node):
        if node is not None and isinstance(node.value, basestring):
            node.update({'value': node.value})
        return node


class PropertiesQuery(object):

    def __init__(self, parent, name, node):
        print node.children


class RelationQuery(ObjectQuery):

    domain_object = Relation
    model = {
        'id': FieldQuery,
        'project': (ProjectQuery, lambda p: p.project),
        'author': (AccountQuery, lambda p: p.author),
        'schema': (SchemaQuery, lambda p: p.schema),
        'properties': (PropertiesQuery, lambda p: p.properties),
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }
    default_fields = ['id', 'project']


class EntityQuery(ObjectQuery):

    domain_object = Entity
    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery,
        'project': (ProjectQuery, lambda p: p.project),
        'schemata': (SchemaQuery, lambda p: p.schemata),
        'author': (AccountQuery, lambda p: p.author),
        'inbound': (RelationQuery, lambda p: p.inbound),
        'outbound': (RelationQuery, lambda p: p.outbound),
        'properties': (PropertiesQuery, lambda p: p.properties),
    }
    default_fields = ['id', 'status', 'project']

RelationQuery.model['source'] = (EntityQuery, lambda p: p.source)
RelationQuery.model['target'] = (EntityQuery, lambda p: p.target)


def run(query):
    node = QueryNode(None, query)
    eq = EntityQuery(None, None, node)
    return eq
