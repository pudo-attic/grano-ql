

class QueryNode(object):

    def __init__(self, name, el):
        self.el = el
        self.name = name

    @property
    def as_list(self):
        return isinstance(self.el, (list, tuple))

    @property
    def value(self):
        if self.as_list:
            if len(self.el):
                return self.el[0]
        else:
            return self.el

    def update(self, value):
        if self.as_list:
            self.el[0] = value
        else:
            self.el = value
        return self

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
            'as_list': self.as_list,
            'children': self.children
        }
