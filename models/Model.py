# -*- coding:utf-8 -*-
import PySQL
from PySQL.Exceptions import *
from PySQL.util import SqlBuilder
# from .Field import *
from ..Content import Content
import inspect


class Model(SqlBuilder.SqlNode):
    # show_name = str()
    _en_column = dict()
    _session = None
    _meta = None

    def __init__(self, *args, **kwargs):
        # self._ctx = dict()
        self._ctx = SqlBuilder.SqlContext()
        self.__data__ = dict()
        self._dirty = set(self.__data__)

        for k in kwargs:
            setattr(self, k, kwargs[k])

    def __new__(cls, *args, **kwargs):
        in_init = kwargs.get("init")
        parent_meta = cls.get_cls_meta()

        if in_init is None and parent_meta is None:
            raise EntityError("Error for create entity. There is no corresponding table for:[%s]" % cls.__name__)
        me = super(Model, cls).__new__(cls)
        if parent_meta:
            me._meta = parent_meta
            me._session = Content.session

        return me

    @classmethod
    def new(cls):
        return cls()

    @classmethod
    def transfer(cls, table_name, db_name):
        """
            For the entity who haven't or can't init at the program start time
            Use this method can let them into the PySQL's mapping system
        :return:
        """
        PySQL.transfer(cls, table_name, db_name)

    @classmethod
    def get_cls_meta(cls):
        try:
            return cls._meta
        except AttributeError:
            return None

    @classmethod
    def en_col_mapping(cls):
        en_col = dict()
        all_args = dir(cls)
        for each in all_args:
            if each.startswith("_"):
                continue
            pt = getattr(cls, each)
            if inspect.ismethod(pt) or inspect.isfunction(pt):
                continue

            lower_key = each.lower()
            en_col[lower_key] = each
        return en_col

    def retrieve_entity_col(self):
        """
            Get all the parameters, remove the private and methods
        """
        columns = self._meta.columns

        en_vals = dict()
        for k, col in columns.items():
            try:
                val = self.__getattribute__(k)
                cval = col.validate_self(val)
                if cval == "__pk__":
                    continue
                en_vals[k] = cval
            except AttributeError:
                continue
        return en_vals

    def to_dict(self):
        d = dict()
        cols = self.retrieve_entity_col()
        for k, v in cols.items():
            d[k] = v
        return d

    def _to_entity(self, res_li):
        if not res_li:
            return []
        en_list = list()

        for each in res_li:
            en = self.new()
            for k, v in each.items():
                lower_key = k.lower()
                original_key = self._en_column.get(lower_key)
                if original_key:
                    setattr(en, original_key, v)
                else:
                    setattr(en, k, v)

            en_list.append(en)
        return en_list

    def _build_proc_sql(self, proc_name, parm):
        if not isinstance(parm, (list, tuple)):
            parm = (parm,)

        sql = SqlBuilder.proc_sql(proc_name, parm)
        return sql

    def _build_select_sql(self):
        if not self._ctx.is_exist("SELECT"):
            [self._ctx.press("SELECT", '`%s`' % a) for a in self._meta.columns.keys()]

        if not self._ctx.is_exist("FROM"):
            self._ctx.press("FROM", self._meta.full_path)

        return SqlBuilder.select_sql(self._ctx)

        # self._ctx["path"] = self._meta.full_path
        # self._ctx["column"] = self._ctx["column"] if self._ctx.has_key("column") else self._meta.columns.keys()
        # self._ctx["where"] = self._ctx["where"] if self._ctx.has_key("where") else None
        #
        # return select_sql(self._ctx)

    def _build_insert_sql(self):
        values = []
        path = self._meta.full_path
        self._ctx.press("INSERT INTO", "")

        validate_value = self.retrieve_entity_col()
        for k, v in validate_value.items():
            field = self._meta.columns.get(k, None)
            if field is None: continue

            self._ctx.press(path, field)
            self._ctx.press("VALUES", "")  # For query type
            values.append(v)
        # for each in self._en_column:
        #     db_col_name = self._en_column.get(each)
        #     field = self._meta.columns.get(db_col_name, None)
        #     if field is None: continue
        #
        #     value = getattr(self, db_col_name, None)
        #     if not value: value = getattr(self, each, None)
        #
        #     self._ctx.press(path, field)
        #     self._ctx.press("VALUES", "")  # For query type
        #
        #     final_val = field.validate_self(value)
        #     values.append(final_val)
        #     # self._ctx.container("VALUES", value)

        sql = SqlBuilder.insert_sql(self._ctx, path=path)
        return sql, values
        # self._ctx["path"] = self._meta.full_path
        # # self._ctx["column"] = self._meta.columns.keys()
        # entity_values = self.retrieve_entity_col()
        # self._ctx["column"] = entity_values.keys()
        # self._ctx["value"] = entity_values.values()

    def _build_update_sql(self):
        if not self._ctx.is_exist("WHERE"):
            pk = self._meta.pk
            pkv = getattr(self, pk, None)
            if pkv is None: raise SQLError("Update sql must be have WHERE condition")

            cls = self.__class__
            pkf = getattr(cls, pk)
            self._ctx.press("WHERE", pkf == pkv)

        self._ctx.press("UPDATE", self._meta.full_path)

        sets = []
        columns = self._meta.columns
        for k, field in columns.items():
            val = getattr(self, k, None)
            if val is None: continue

            ex = "`%s` = %%s" % k
            self._ctx.press("SET", ex)
            sets.append(val)

        sql = SqlBuilder.update_sql(self._ctx)
        return sql, sets

    def call_proc(self, proc_name, parm=()):
        try:
            sql_tuple = self._build_proc_sql(proc_name, parm)
            result = self._session.execute_proc(sql_tuple, db_name=self._meta.db_name)
            return result
        except Exception, e:
            raise Exception("Failed to execute proc, %s" % str(e))

    def select(self, *columns):
        wh = "SELECT"
        [self._ctx.press(wh, b) for b in columns]

        # for col in columns:
        #     if IS_STR(col):
        #         self.__sql__.stack(wh, col)
        #     elif IS_LIST(col):
        #         [self.__sql__.stack(wh, a) for a in col]
        #     elif isinstance(col, Field):
        #         self.__sql__.stack(wh, col.column_name)

        # if IS_STR(column) or IS_LIST(column):
        #     self._ctx["column"] = column
        # else:
        #     self._ctx["column"] = self._meta.columns.keys()
        return self

    def left_join(self, other, on):
        wh = "FROM"
        SqlBuilder.Hierarchy(self._ctx, other._ctx)

        self._ctx.make_alias(self.__class__)
        other._ctx.make_alias(other.__class__)
        self._ctx.press(wh, SqlBuilder.Join(lhs=self, rhs=other, on=on))
        return self

    def where(self, *wheres):
        wh = "WHERE"
        [self._ctx.press(wh, a) for a in wheres]
        # if IS_STR(wheres):
        #     self._ctx["where"] = wheres
        # elif IS_DICT(wheres):
        #     nw = {k.name if isinstance(k, Field) else k: v for k, v in wheres.items()}
        #     self._ctx["where"] = nw
        # else:
        #     self._ctx["where"] = None
        return self

    def group_by(self, *gbs):
        wh = "GROUP BY"
        [self._ctx.press(wh, a) for a in gbs]
        # group_by = list()
        # for gb in gbs:
        #     if isinstance(gb, Field):
        #         group_by.append(gb.name)
        #     else:
        #         group_by.append(gb)
        #
        # self._ctx["group_by"] = group_by
        return self

    def order_by(self, *obs):
        wh = "ORDER BY"
        [self._ctx.press(wh, a) for a in obs]
        # o_str = "%s %s"
        # if isinstance(ob, Field):
        #     o_str = o_str % (ob.name, order)
        # else:
        #     o_str = o_str % (ob, order)
        #
        # self._ctx["order_by"] = o_str
        return self

    def limit(self, count, offset=0):
        wh = "LIMIT"
        self._ctx.press(wh, offset)
        self._ctx.press(wh, count)
        # self._ctx["limit"] = (offset, count)
        return self

    def find(self, format_to_entity=True):
        try:
            sql_tuple = self._build_select_sql()
            res_li = self._session.execute_select(sql_tuple, db_name=self._meta.db_name)

            if format_to_entity:
                result = self._to_entity(res_li)
                if len(result) == 1:
                    return result[0]
                return result
            else:
                return res_li
        except Exception, e:
            import traceback
            print traceback.format_exc()
            raise Exception("Failed to select. %s" % str(e))
        finally:
            if self._ctx:
                self._ctx.clear()

    def find_all(self):
        return self.find()

    def update(self):
        try:
            sql_tuple = self._build_update_sql()
            self._session.execute_update(sql_tuple, db_name=self._meta.db_name)
            return True
        except Exception, e:
            raise Exception("Failed to update. %s" % str(e))
        finally:
            if self._ctx:
                self._ctx.clear()

    def save(self):
        try:
            sql_tuple = self._build_insert_sql()
            self._session.execute_insert(sql_tuple, db_name=self._meta.db_name)
            return True
        except Exception, e:
            raise Exception("Failed to insert. %s" % str(e))
        finally:
            if self._ctx:
                self._ctx.clear()

    def _execute_sql(self, sql, db_name=None):
        if db_name is None:
            db_name = self._meta.db_name
        return self._session.execute_select(sql, db_name=db_name)

    def __sql__(self, ctx):
        if self._ctx is ctx:  # just full path
            return ctx.sql(self._meta.full_path)
        else:
            new_en = self.clone()
            new_en._ctx.manager = None
            with ctx():
                return ctx.sql(self._build_select_sql())
