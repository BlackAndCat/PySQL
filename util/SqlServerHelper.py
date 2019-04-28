# -*- coding: utf-8 -*-
import pymssql
from DBUtils.PersistentDB import PersistentDB
from PySQL.Interface import SqlHelper


class SQLServerHelper(SqlHelper):
    _charset = "utf8"

    maxusage = 1000
    _pool = None

    dict_cursor = True

    def __init__(self, conn_str):
        self.connstr = conn_str
        self._build_pool(conn_str)

    def _build_pool(self, conn_str=None):
        conn_args = self.set_parm(conn_str)

        try:
            # self._pool = PersistentDB(pyodbc, maxusage=self.maxusage, **conn_args)
            self._pool = PersistentDB(pymssql, maxusage=self.maxusage, **conn_args)
        except Exception, ex:
            import traceback
            print traceback.print_exc()
            raise Exception("Failed init pool mes:[%s]" % str(ex))

    def set_parm(self, conn_str):
        conn_str["charset"] = self._charset
        return conn_str

    def _get_connection(self):
        try:
            conn = self._pool.connection()
            return conn
        except Exception, ex:
            raise Exception("Can't connection db. mes[%s] host:[%s]"
                            % (str(ex), self._server))

    def _get_cursor(self, conn):
        cursor = conn.cursor()
        return cursor

    def _re_connection(self):
        self._build_pool()
        conn = self._pool.connection()
        return conn

    def _rollback(self, conn):
        conn.rollback()

    def _set_data_to_dict(self, cur):
        data = cur.fetchall()
        if not data:
            return list()
        result = []
        columns = [column[0] for column in cur.description]
        for row in data:
            r = zip(columns, row)
            result.append(dict(r))
        return result

    def crossroads(func):
        def wrapper(*args, **kwargs):
            self = args[0]
            try:
                conn = self._get_connection()
                cursor = self._get_cursor(conn)
                kwargs["conn"] = conn
                kwargs["cursor"] = cursor
                result = func(*args, **kwargs)
            except Exception, ex:
                self._rollback(conn)
                raise Exception("Failed execute sql mes:[%s]" % str(ex))

            return result
        return wrapper

    @crossroads
    def select(self, sql, **kwargs):
        cursor = kwargs.get("cursor")
        cursor.execute(sql)

        if self.dict_cursor:
            result = self._set_data_to_dict(cursor)
        else:
            result = cursor.fetchall()
        return result

    @crossroads
    def insert(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")
        for sql_tuple in sqls:
            if isinstance(sql_tuple, str):
                cursor.execute(sql_tuple)
            else:
                query_str, values_arr = sql_tuple
                cursor.execute(query_str, values_arr)
        conn.commit()

    @crossroads
    def update(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")
        for sql in sqls:
            cursor.execute(sql)
        conn.commit()

    @crossroads
    def delete(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")

        for sql in sqls:
            cursor.execute(sql)
        conn.commit()
