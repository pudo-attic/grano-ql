

class QueryNode(object):

    def __init__(self, name, el):
        self.el = el
        self.name = name
        self.is_root = name is None

    @property
    def all(self):
        return isinstance(self.el, (list, tuple))

    @property
    def value(self):
        if self.all:
            if len(self.el):
                return self.el[0]
        else:
            return self.el

    @property
    def is_leaf(self):
        return not isinstance(self.value, dict)

    @property
    def children(self):
        if self.is_leaf:
            return
        for k, v in self.value.items():
            yield QueryNode(k, v)

    def to_dict(self):
        # TODO: this is just for debug.
        return {
            'name': self.name,
            'value': self.value,
            'all': self.all,
            'is_leaf': self.is_leaf,
            'children': self.children
        }
