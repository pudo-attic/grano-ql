from itertools import groupby
from datetime import datetime

from sqlalchemy.orm import aliased
from sqlalchemy.sql import and_, or_

from grano.model import Entity, Project, Account, Relation
from grano.model import Schema, Property
from grano.model import db
from grano.ql.parser import EXTRA_FIELDS, EntityParserNode

# TODO: and/or query branches / list queries


PARENT_ID = '__parent_id'


class FieldQuery(object):
    """ Query a simple column, as opposed to a more complex nested
    object. This will simply join the required column onto the
    parent object and - if necessary - filter it's values. """

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


class ObjectQuery(object):

    model = {}
    domain_object = None
    domain_object_marker = None
    default_fields = []

    def __init__(self, parent, name, node):
        self.name = name
        self.node = node
        self.alias = aliased(self.domain_object)
        self.parent = parent
        self.children = {}
        self.joiners = {}

        # instantiate the model:
        for name, cls in self.model.items():
            for node in self.node.children:
                if node.name == name:
                    self.children[name] = cls(self, name, node)

    def get_child_node_value(self, name, default=None):
        for node in self.node.children:
            if node.name == name:
                return node.value
        return default

    @property
    def root(self):
        """ Get the root level of the query. """
        if self.parent is None:
            return self
        return self.parent.root

    def filter(self, q):
        """ Apply the joins and filters specified on this level of the
        query. """
        if self.parent:
            q = self.join_parent(q)
        for child in self.node.children:
            if child.name in EXTRA_FIELDS:
                continue
            if child.name in self.children:
                q = self.children[child.name].filter(q)
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
        if self.domain_object_marker is not None:
            data['obj'] = self.domain_object_marker
        if record is not None:
            res = dict(zip(record.keys(), record))
            data[PARENT_ID] = res.get(PARENT_ID)
            for node in self.node.children:
                if node.value is None:
                    data[node.name] = res.get(node.name)
        return data

    def query(self, parent_ids):
        """ Construct a SQL query for this level of the request. """
        q = db.session.query()
        q = self.filter(q)
        q = self.add_columns(q)
        if self.parent is not None:
            col = self.parent.alias.id.label(PARENT_ID)
            q = q.add_columns(col)
            if parent_ids is not None:
                q = q.filter(col.in_(parent_ids))
            q = q.distinct(col)

        # pagination
        if self.parent is None:
            q = q.offset(self.get_child_node_value('offset', 0))
            if not self.node.as_list:
                q = q.limit(1)
            else:
                q = q.limit(self.get_child_node_value('limit', 10))

        q = q.distinct(self.children['id'].column)
        return q

    def run(self, parent_ids=None):
        """ Collect results for the query from this level and all
        children. Returns a generator, of parent_id, results tuples.
        """
        results = [self._make_object(r) for r in self.query(parent_ids)]
        ids = [r.get('id') for r in results]

        if not len(results):
            p = self._make_object({})
            p.pop(PARENT_ID)
            yield None, [p] if self.node.as_list else p

        for name, child in self.children.items():
            if isinstance(child, FieldQuery):
                continue
            if name not in [node.name for node in self.node.children]:
                continue
            for parent_id, nested in child.run(parent_ids=ids):
                for result in results:
                    if parent_id == result['id']:
                        result[name] = nested

        by_parent = groupby(results, lambda r: r.pop(PARENT_ID))
        for parent_id, results in by_parent:
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

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.author)


class SchemaQuery(ObjectQuery):

    domain_object = Schema
    model = {
        'id': FieldQuery,
        'name': FieldQuery,
        'hidden': FieldQuery,
        'label': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }

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

    domain_object = Property
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

    def __init__(self, parent, name, node):
        if 'value' in node.value:
            obj = node.value.pop('value')
            if obj is None:
                for col in self.value_columns:
                    node.value[col] = None
            else:
                for col, type_ in self.value_columns.items():
                    if isinstance(obj, type_):
                        node.value[col] = obj

        if name == '*':
            node.value['name'] = None
        else:
            node.value['name'] = name
        node.value['active'] = True
        node.as_list = True
        super(PropertyQuery, self).__init__(parent, name, node)

    def _make_object(self, record):
        record = super(PropertyQuery, self)._make_object(record)
        record.pop('active', None)
        for col in self.value_columns.keys():
            if col in record:
                if 'value' not in record:
                    record['value'] = None
                val = record.pop(col)
                if val is not None:
                    record['value'] = val
        return record


class EntityPropertyQuery(PropertyQuery):

    def join_parent(self, q):
        return q.filter(self.alias.entity_id == self.parent.alias.id)


class RelationPropertyQuery(PropertyQuery):

    def join_parent(self, q):
        return q.filter(self.alias.relation_id == self.parent.alias.id)


class PropertiesQuery(object):
    """ A stub query object to retrieve all the requested properties
    and return them in an associative array. """

    def __init__(self, parent, name, node):
        self.parent = parent
        self.children = {}
        for child in node.children:
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
    domain_object_marker = 'relation'

    model = {
        'id': FieldQuery,
        'project': ProjectQuery,
        'author': AuthorQuery,
        'schema': SchemaQuery,
        'properties': RelationPropertiesQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery
    }


class InboundRelationQuery(RelationQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.inbound)


class OutboundRelationQuery(RelationQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.outbound)


class BidiRelationQuery(RelationQuery):

    def join_parent(self, q):
        cond = or_(self.alias.source_id == self.parent.alias.id,
                   self.alias.target_id == self.parent.alias.id)
        return q.filter(cond)


class EntityQuery(ObjectQuery):

    domain_object = Entity
    domain_object_marker = 'entity'
    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery,
        'project': ProjectQuery,
        'schemata': SchemataQuery,
        'schema': SchemataQuery,
        'author': AuthorQuery,
        'inbound': InboundRelationQuery,
        'outbound': OutboundRelationQuery,
        'relations': BidiRelationQuery,
        'properties': EntityPropertiesQuery,
    }


class SourceEntityQuery(EntityQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.source)


class TargetEntityQuery(EntityQuery):

    def join_parent(self, q):
        return q.join(self.alias, self.parent.alias.target)


class BidiEntityQuery(EntityQuery):

    def join_parent(self, q):
        cond = and_(or_(self.parent.alias.source_id == self.alias.id,
                        self.parent.alias.target_id == self.alias.id),
                    self.parent.parent.alias.id != self.alias.id)
        return q.filter(cond)


InboundRelationQuery.model['source'] = SourceEntityQuery
OutboundRelationQuery.model['target'] = TargetEntityQuery
BidiRelationQuery.model['other'] = BidiEntityQuery


def run(query):
    node = EntityParserNode(None, query)
    node.value['limit'] = min(1000, node.value.get('limit', 25))
    node.value['offset'] = max(0, node.value.get('offset', 0))
    eq = EntityQuery(None, None, node)
    return eq
