from sqlalchemy.orm import aliased

from grano.lib.exc import BadRequest
from grano.model import Entity, Project
from grano.model import db
from grano.ql.parser import QueryNode


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

    def retrieve(self, q):
        return q.add_columns(self.column)

    def to_dict(self):
        return {
            'name': self.name,
            'qn': self.qn
        }


class ObjectQuery(object):

    model = {}
    domain_object = None

    def __init__(self, parent, name, qn):
        self.qn = self.patch_qn(qn)
        self.parent = parent
        self.name = name
        self.children = {}

        # instantiate the model:
        if self.qn:
            for name, cls in self.model.items():
                qn = None
                for qn_ in self.qn.children:
                    if qn_.name == name:
                        qn = qn_
                self.children[name] = cls(self, name, qn)

        self.alias = aliased(self.domain_object)

    def patch_qn(self, qn):
        return qn

    @property
    def root(self):
        if self.parent is None:
            return self
        return self.parent.root

    def filter(self, q):
        if self.qn is None:
            return q
        for child in self.qn.children:
            if child.value is None:
                continue
            if child.name not in self.children:
                raise BadRequest('Unknown field: %s' % child.name)
            q = self.children[child.name].filter(q)
        return q

    def retrieve(self, q):
        if self.qn is None:
            return q
        for name, child in self.children.items():
            for qn in self.qn.children:
                if qn.value is not None:
                    continue
                if qn.name == name or qn.name == '*':
                    q = child.retrieve(q)
        return q

    def compose(self, record):
        data = dict([(k, v) for (k, v) in self.qn.value.items() if k != '*'])
        if record is not None:
            data.update(dict(zip(record.keys(), record)))
        return data

    def run(self):
        if self.qn is None:
            return None
        q = db.session.query()
        q = self.root.filter(q)
        q = self.retrieve(q)
        # TODO: offset, limit
        if self.qn.all:
            return map(self.compose, q)
        else:
            return self.compose(q.first())

    def to_dict(self):
        # TODO: this is just for debug.
        return {
            #'query_node': self.qn,
            'children': self.children,
            'result': self.run()
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

    def patch_qn(self, qn):
        if qn is not None and isinstance(qn.value, basestring):
            qn.update({'slug': qn.value})
        return qn


class EntityQuery(ObjectQuery):

    domain_object = Entity
    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery,
        'project': ProjectQuery
    }


def run(query):
    qn = QueryNode(None, query)
    eq = EntityQuery(None, None, qn)
    return eq
