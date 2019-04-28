version = "0.1.0"


class ConAttr(dict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def __setattr__(self, attr, value):
        self[attr] = value

    def __iadd__(self, rhs):
        self.update(rhs)
        return self

    def __add__(self, rhs):
        d = ConAttr(self)
        d.update(rhs)
        return d

    def set_dict(self, d_name, k, v):
        try:
            attr = self.__getattr__(d_name)
        except AttributeError:
            self.__setattr__(d_name, {k: v})
        else:
            attr[k] = v


Content = ConAttr(
    version=version
    , flows=ConAttr()
    , EXEC_FLOW="exec_flow"
    , FIELD_FLOW="field_flow"
)
