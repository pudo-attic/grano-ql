from sqlalchemy.orm import aliased

from grano.lib.exc import BadRequest
from grano.model import Entity
from grano.model import db
from grano.ql.parser import QueryNode


class EntityQuery(object):

    def __init__(self, qn):
        self.qn = qn
        self.entity = aliased(Entity)
        self.q = db.session.query()

    def parse_column(self, qn):
        col = getattr(self.entity, qn.name)
        if qn.value is None:
            self.q = self.q.add_columns(col)
        else:
            self.q = self.q.filter(col == qn.value)

    def parse(self):
        for child in self.qn.children:
            # TODO: operator parsing
            handlers = {
                'id': self.parse_column,
                'created_at': self.parse_column,
                'updated_at': self.parse_column,
                'status': self.parse_column
            }
            if child.name not in handlers:
                raise BadRequest('Unknown field: %s' % child.name)
            handlers[child.name](child)

    def compose(self, record):
        data = self.qn.value.copy()
        if record is None:
            return data
        requested = dict(zip(record.keys(), record))
        for child in self.qn.children:
            if child.value is None:
                data[child.name] = requested.get(child.name)
        return data

    def run(self):
        self.parse()
        if self.qn.is_list:
            res = map(self.compose, self.q)
        else:
            res = self.compose(self.q.first())
        return res

    def to_dict(self):
        # TODO: this is just for debug.
        return {
            'query_node': self.qn,
            'q': unicode(self.q)
        }


def run(query):
    qn = QueryNode(None, query)
    eq = EntityQuery(qn)
    eq.parse()
    return eq.run()
