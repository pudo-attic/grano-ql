from sqlalchemy.orm import aliased

from grano.lib.exc import BadRequest
from grano.model import Entity
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


class ObjectQuery(object):

    model = {}

    def __init__(self, qn):
        self.qn = qn
        self.children = {}

        # instantiate the model:
        for name, cls in self.model.items():
            qn = None
            for qn_ in self.qn.children:
                if qn_.name == name:
                    qn = qn_
            self.children[name] = cls(self, name, qn)

        self.alias = aliased(Entity)

    def filter(self, q):
        for child in self.qn.children:
            if child.value is None:
                continue
            if child.name not in self.children:
                raise BadRequest('Unknown field: %s' % child.name)
            q = self.children[child.name].filter(q)
        return q

    def retrieve(self, q):
        for name, child in self.children.items():
            for qn in self.qn.children:
                if qn.value is not None:
                    continue
                if qn.name == name or qn.name == '*':
                    q = child.retrieve(q)
        return q

    def compose(self, record):
        data = dict([(k, v) for (k, v) in self.qn.value.items() if k != '*'])
        if record is None:
            return data
        requested = dict(zip(record.keys(), record))
        data.update(requested)
        return data

    def run(self):
        q = db.session.query()
        q = self.filter(q)
        q = self.retrieve(q)
        # TODO: offset, limit
        if self.qn.is_list:
            return map(self.compose, q)
        else:
            return self.compose(q.first())

    def to_dict(self):
        # TODO: this is just for debug.
        return {
            'query_node': self.qn,
            'result': self.run()
        }


class EntityQuery(ObjectQuery):

    model = {
        'id': FieldQuery,
        'created_at': FieldQuery,
        'updated_at': FieldQuery,
        'status': FieldQuery
    }


def run(query):
    qn = QueryNode(None, query)
    eq = EntityQuery(qn)
    return eq
