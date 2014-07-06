from itertools import groupby
from datetime import datetime

from sqlalchemy.orm import aliased
#from sqlalchemy.sql import and_, or_

from grano.lib.exc import BadRequest
from grano.model import Entity, Project, Account, Relation
from grano.model import Schema, EntityProperty, RelationProperty
from grano.model import db
from grano.ql.parser import QueryNode

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
        self.name = name
        self.node = self.expand_query(name, node)
        self.alias = aliased(self.domain_object)
        self.parent = parent
        self.children = {}
        self.joiners = {}

        # instantiate the model:
        for name, cls in self.model.items():
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
            if not isinstance(child, FieldQuery):
                yield name, child

    def filter(self, q):
        """ Apply the joins and filters specified on this level of the
        query. """
        if self.parent:
            q = self.join_parent(q)
        for child in self.node.children:
            if child.value is None:
                continue
            if child.name not in self.children:
                raise BadRequest('Unknown field: %s' % child.name)
            q = self.children[child.name].filter(q)
        return q

    def join_parent(self, q):
        return q

    def add_columns(self, q):
        """ Define the columns to be retrieved when this is the active
        level of the query. """
        for name, child in self.children.items():
            if not isinstance(child, FieldQuery):
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
                if node.value is None:
                    data[node.name] = res.get(node.name)
        return data

    def query(self, parent_ids):
        """ Construct a query for this level of the query. """
        q = db.session.query()
        q = self.root.filter(q)
        q = self.add_columns(q)
        if self.parent is not None:
            col = self.parent.alias.id.label(PARENT_ID)
            if parent_ids is not None:
                q = q.filter(col.in_(parent_ids))
            q = q.add_columns(col)
        # TODO: temp
        if self.parent is None:
            q = q.limit(25)
        #print q
        if not self.node.as_list:
            q = q.limit(1)
        return q.distinct()

    def run(self, parent_ids=None):
        """ Actually run the query, recursively. """
        results = [self._make_object(r) for r in self.query(parent_ids)]
        ids = [r.get('id') for r in results]

        if not len(results):
            p = self._make_object({})
            p.pop(PARENT_ID)
            yield None, [p] if self.node.as_list else p

        for name, child in self.children_objects:
            if name not in [node.name for node in self.node.children]:
                continue
            for parent_id, nested in child.run(parent_ids=ids):
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

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.project)


class AuthorQuery(ObjectQuery):

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

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.author)


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

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.schema)


class SchemataQuery(SchemaQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.schemata)


class PropertyQuery(ObjectQuery):
    """ Property queries are the second level in querying a set of
    properties, they are called by the PropertiesQuery. This is somewhat
    complex because we need to handle types (i.e. query the appropriate
    column for the submitted input type, or retrieve all to find the
    one that holds a value. """

    value_columns = {
        'value_string': basestring,
        'value_datetime': datetime,
        'value_integer': int,
        'value_float': float,
        'value_boolean': bool
    }
    model = {
        'id': FieldQuery,
        'name': FieldQuery,
        'value_string': FieldQuery,
        'value_datetime': FieldQuery,
        'value_integer': FieldQuery,
        'value_float': FieldQuery,
        'value_boolean': FieldQuery,
        'source_url': FieldQuery,
        'active': FieldQuery
    }
    default_fields = value_columns.keys() + ['source_url']

    def patch_node(self, node):
        value = node.value
        if isinstance(value, basestring):
            value = {'value': value}
        if value is None:
            value = dict([(d, None) for d in self.default_fields])

        # determine the actual underlying column
        # TODO: figure out how to do datetime from JSON queries!
        if value is not None and value.get('value') is not None:
            obj = value.pop('value')
            for col, type_ in self.value_columns.items():
                if isinstance(obj, type_):
                    value[col] = obj

        if self.name == '*':
            value['name'] = None
        else:
            value['name'] = self.name

        value['active'] = True
        node.update(value)
        return node

    def _make_object(self, record):
        record = super(PropertyQuery, self)._make_object(record)
        for col in self.value_columns.keys():
            if col in record:
                if 'value' not in record:
                    record['value'] = None
                val = record.pop(col)
                if val is not None:
                    record['value'] = val
        return record

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.properties)


class EntityPropertyQuery(PropertyQuery):
    domain_object = EntityProperty


class RelationPropertyQuery(PropertyQuery):
    domain_object = RelationProperty


class PropertiesQuery(object):

    def __init__(self, parent, name, node):
        value = node.value
        if value is None:
            value = {'*': None}
        node.update(value)

        self.parent = parent
        self.children = {}
        for child in node.children:
            if not child.as_list:
                child.el = [child.el]
            prop = self.child_cls(parent, child.name, child)
            self.children[child.name] = prop

    def filter(self, q):
        for name, child in self.children.items():
            q = child.filter(q)
        return q

    def run(self, parent_ids=None):
        results = {}
        for name, child in self.children.items():
            for parent_id, values in child.run(parent_ids=parent_ids):
                if parent_id not in results:
                    results[parent_id] = {}
                for value in values:
                    results[parent_id][value.pop('name')] = value
        return results.items()


class EntityPropertiesQuery(PropertiesQuery):
    child_cls = EntityPropertyQuery


class RelationPropertiesQuery(PropertiesQuery):
    child_cls = RelationPropertyQuery


class RelationQuery(ObjectQuery):

    domain_object = Relation
    model = {
        'id': FieldQuery,
        'project': ProjectQuery,
        'author': AuthorQuery,
        'schema': SchemaQuery,
        'properties': RelationPropertiesQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }
    default_fields = ['id', 'project']


class InboundRelationQuery(RelationQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.inbound)


class OutboundRelationQuery(RelationQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.outbound)


class EntityQuery(ObjectQuery):

    domain_object = Entity
    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery,
        'project': ProjectQuery,
        'schemata': SchemataQuery,
        'author': AuthorQuery,
        'inbound': InboundRelationQuery,
        'outbound': OutboundRelationQuery,
        'properties': EntityPropertiesQuery,
    }
    default_fields = ['id', 'status', 'properties', 'project']


class SourceEntityQuery(EntityQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.source)


class TargetEntityQuery(EntityQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.target)


InboundRelationQuery.model['source'] = SourceEntityQuery
OutboundRelationQuery.model['target'] = TargetEntityQuery


def run(query):
    node = QueryNode(None, query)
    eq = EntityQuery(None, None, node)
    return eq
