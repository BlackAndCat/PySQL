# -*- coding:utf-8 -*-
from .Content import Content
from pkg.Flows.Flow import rules


class Session(object):
    exec_flow = Content.flows.get(Content.EXEC_FLOW)
    _instance = None
    map = None

    _TABLE_EXISTS_SQL = "show tables like '%s'"
    _DB_EXISTS_SQL = "show databases like '%s'"

    def __init__(self):
        self.map = Content.map

    def __new__(cls, *args, **kwargs):
        if cls._instance:
            return cls._instance
        else:
            cls._instance = super(Session, cls).__new__(cls)
            return cls._instance

    def get_all_entity(self):
        return self.map.get_entity_mapping()

    def get_entity(self, en_name):
        return self.map.get_entity(en_name)

    def get_db_mapping(self):
        return self.map.get_db_mapping()

    def get_pools(self):
        return self.map.get_pools()

    def get_pool(self, **kwargs):
        return self.map.get_pool(**kwargs)

    def is_exists(self, tb_name=None, db_name=None):
        if tb_name:
            sql = self._TABLE_EXISTS_SQL % tb_name
        else:
            raise Exception("Must give db/table name.")

        result = self.execute_select(sql, db_name=db_name)
        if result:
            return True
        return False

    @rules(exec_flow)
    def execute_proc(self, sql_tuple, pool, *args, **kwargs):
        result = pool.call_proc(sql_tuple)
        return result

    @rules(exec_flow)
    def execute_select(self, sql_tuple, pool, *args, **kwargs):
        result = pool.select(sql_tuple)
        return result

    @rules(exec_flow)
    def execute_insert(self, sql_tuple, pool, *args, **kwargs):
        pool.insert(sql_tuple)
        return True

    @rules(exec_flow)
    def execute_update(self, sql_tuple, pool, *args, **kwargs):
        pool.update(sql_tuple)
        return True
