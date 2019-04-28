

class SqlHelper(object):
    def call_proc(self, sql, **kwargs):
        pass

    def select(self, sql, **kwargs):
        pass

    def insert(self, *sqls, **kwargs):
        pass

    def update(self, *sqls, **kwargs):
        pass

    def delete(self, *sqls, **kwargs):
        pass


class Rule(object):
    def before(self, *args, **kwargs):
        pass

    def after(self, *args, **kwargs):
        pass


class InitRule(Rule):
    def before(self, *args, **kwargs):
        pass

    def after(self, *args, **kwargs):
        pass


class ExecuteRule(Rule):
    def before(self, *args, **kwargs):
        pass

    def after(self, *args, **kwargs):
        pass
