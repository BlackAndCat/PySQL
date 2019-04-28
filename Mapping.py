# -*- coding:utf-8 -*-


class Mapping(object):
    """
        table's name :key
        entity's instance :param
    """
    _entity_mapping = dict()

    """
        table's name :key
        database's name :param
    """
    _table_mapping = dict()

    """
        database's name :key
        pool's name :param
    """
    _db_mapping = dict()

    """
        pool's name :key
        pool's instance :param
    """
    _connect_pools = dict()

    def get_entity(self, en_name):
        return self._entity_mapping.get(en_name)

    def get_entity_mapping(self):
        return self._entity_mapping

    def get_table_mapping(self):
        return self._table_mapping

    def get_db_mapping(self):
        return self._db_mapping

    def get_db(self, table_name=None):
        if table_name:
            return self._table_mapping.get(table_name)

    def get_pools(self):
        return self._connect_pools

    def get_pool(self, pool_name=None, db_name=None, table_name=None):
        if db_name is None:
            for db, tbs in self._table_mapping.items():
                if table_name in tbs:
                    db_name = db
                    break
            # db_name = self._table_mapping.get(table_name)
        if db_name and not pool_name:
            pool_name = self._db_mapping.get(db_name)
        if pool_name:
            return self._connect_pools.get(pool_name)

    def _put_connect_pool(self, pool):
        if not isinstance(pool, dict):
            raise Exception("Connection must be dict")
        pool_name = pool.get("conn_name", pool["db"]) or str(len(self._connect_pools))
        self._connect_pools[pool_name] = pool

    def put_connect_pool(self, pools):
        if not isinstance(pools, (list, tuple)):
            pools = [pools]

        for each in pools:
            self._put_connect_pool(each)

            db_name = each.get("db")
            conn_name = each.get("conn_name")
            self.put_db_couple(db_name=db_name, conn_name=conn_name)

    def update_pool(self, pool_name, conn):
        if pool_name in self.get_pools().keys():
            self._connect_pools[pool_name] = conn

    def put_db_couple(self, db_name, conn_name):
        self._db_mapping[db_name] = conn_name

    def put_table_couple(self, table_name, db_name):
        if self._table_mapping.has_key(db_name):
            self._table_mapping.get(db_name).append(table_name)
        else:
            self._table_mapping[db_name] = [table_name]

    def put_entity_couple(self, table_name, en_ins):
        self._entity_mapping[table_name] = en_ins
