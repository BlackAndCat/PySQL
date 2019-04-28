# -*- coding:utf-8 -*-
from .Content import Content
from util.Interpreter import mysql, sqlserver, db_type


class Pysql(object):
    all_entity = dict()
    _instance = None

    def __init__(self):
        self._map_creator = None

    def __new__(cls, *args, **kwargs):
        if cls._instance:
            return cls._instance
        else:
            cls._instance = super(Pysql, cls).__new__(cls)
            return cls._instance

    def engine_create(self, db_type, connect_pools):
        from util import Interpreter
        Content.db_type = db_type
        Content.map.put_connect_pool(connect_pools)
        Interpreter.DB_TYPE = db_type

    def into_en_map(self, en_cls, table_name, db_name, create):
        if table_name not in self.all_entity:
            dic = self.all_entity.get(db_name)
            parm = {
                "cls": en_cls,
                "create": create
            }
            if not dic:
                self.all_entity[db_name] = {table_name: parm}
            else:
                self.all_entity[db_name][table_name] = parm

    def transfer(self, en_cls, table_name, db_name):
        """
            For the entity who haven't or can't init at the program start time
            Use this method can let them into the PySQL mapping system
        :return:
        """
        self.into_en_map(en_cls, table_name, db_name, create=False)
        self._start()

    def _start(self):
        if self._map_creator is None:
            if mysql():
                from Initlize.MySqlInitialize import MySqlInitialize
                self._map_creator = MySqlInitialize()
            elif sqlserver():
                # add SqlServerInitialize
                pass
        try:
            self._map_creator.create_map(self.all_entity)
        finally:
            self.all_entity.clear()

    def start(self):
        self._start()


def run():
    py_sql.start()


def create_engine(db_type, connect_pools=None):
    if not connect_pools:
        return None
    from .Mapping import Mapping
    from .Session import Session
    Content.map = Mapping()
    Content.session = Session()

    py_sql.engine_create(db_type, connect_pools)
    return Content


def table(table_name, db_name, create=False):
    def decorator(en_cls):
        py_sql.into_en_map(en_cls, table_name, db_name, create)
        return en_cls

    return decorator


def transfer(en_cls, table_name, db_name):
    py_sql.transfer(en_cls, table_name, db_name)


py_sql = None


def init():
    global py_sql
    py_sql = Pysql()
    Content.orm = py_sql

    from pkg.Flows.PysqlFlow import ExecuteSqlFlow, FieldCheckFlow

    exec_flow = ExecuteSqlFlow()

    Content.flows[Content.EXEC_FLOW] = exec_flow
    Content.flows[Content.FIELD_FLOW] = FieldCheckFlow()


"""
    For no model sql execute
"""


def table_is_exists(tb_name, db_name):
    return Content.session.is_exists(tb_name=tb_name, db_name=db_name)


def execute_sql(sql, table_name=None, db_name=None, pool_name=None):
    return Content.session.execute_select(sql, table_name=table_name, db_name=db_name, pool_name=pool_name)


if __name__ == 'PySQL':
    init()
