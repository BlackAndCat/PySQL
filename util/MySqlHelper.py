# -*- coding: utf-8 -*-
# import pymysql, pymssql, MySQLdb, pyodbc
import pymysql
from pymysql import OperationalError
import time
from DBUtils.PersistentDB import PersistentDB
from PySQL.Interface import SqlHelper


class MySqlHelper(SqlHelper):
    _charset = "utf8"
    maxusage = 1000

    max_re_time = 50

    _pool = None

    def __init__(self, conn_dict):
        self._parm = conn_dict
        self._host = conn_dict.get("host")
        self._build_pool()

    def _build_pool(self):
        parm = self._parm
        try:
            self._pool = PersistentDB(creator=pymysql
                                      , maxusage=self.maxusage
                                      , host=self._host
                                      , port=int(parm.get("port"))
                                      , user=parm.get("user")
                                      , passwd=parm.get("passwd")
                                      , db=parm.get("db")
                                      , charset=self._charset)
            self._pool.connection()
        except Exception, ex:
            import traceback
            print traceback.print_exc()
            raise Exception("Failed init pool mes:[%s]" % str(ex))

    def _get_connection(self):
        try:
            return self._pool.connection()
        except Exception, ex:
            raise Exception("Can't connection db. mes[%s] host:[%s]"
                            % (str(ex), self._host))

    def _get_cursor(self, conn):
        # return conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        return conn.cursor(pymysql.cursors.DictCursor)

    def _re_connection(self):
        self._build_pool()
        return self._get_connection()

    def _rollback(self, conn):
        conn.rollback()

    def crossroads(func):
        def wrapper(*args, **kwargs):
            re_time = 0
            self = args[0]
            for each in xrange(self.max_re_time):
                try:
                    conn = self._get_connection()
                    cursor = self._get_cursor(conn)
                    kwargs["conn"] = conn
                    kwargs["cursor"] = cursor

                    conn.ping(reconnect=True)

                    result = func(*args, **kwargs)
                    break
                except Exception, ex:
                    re_time += 1
                    errMsg = "Failed to execute sql mes:[%s]" % str(ex)
                    if re_time > self.max_re_time:
                        raise Exception(errMsg)

                    if ("gone away" in errMsg) or ("refused it" in errMsg):

                        import traceback
                        print traceback.format_exc()
                        t = 60 * 10
                        print "Sleep %ss" % t
                        time.sleep(t)
                    else:
                        raise Exception(errMsg)

            return result
        return wrapper

    @crossroads
    def call_proc(self, sql, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.commit()
        return data

    @crossroads
    def select(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")

        for sql_tuple in sqls:
            if isinstance(sql_tuple, (str, unicode)):
                cursor.execute(sql_tuple)
            else:
                query_str, values_arr = sql_tuple
                cursor.execute(query_str, values_arr)
        conn.commit()

        return cursor.fetchall()

    @crossroads
    def insert(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")

        for sql_tuple in sqls:
            if isinstance(sql_tuple, (str, unicode)):
                cursor.execute(sql_tuple)
            else:
                query_str, values_arr = sql_tuple
                cursor.execute(query_str, values_arr)
        conn.commit()

    @crossroads
    def update(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")

        for sql_tuple in sqls:
            if isinstance(sql_tuple, (str, unicode)):
                cursor.execute(sql_tuple)
            else:
                query_str, values_arr = sql_tuple
                cursor.execute(query_str, values_arr)
        conn.commit()

    @crossroads
    def delete(self, *sqls, **kwargs):
        conn = kwargs.get("conn")
        cursor = kwargs.get("cursor")

        for sql in sqls:
            cursor.execute(sql)
        conn.commit()
