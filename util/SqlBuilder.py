# -*- coding:utf-8 -*-
from PySQL.Exceptions import *
from .Interpreter import sqlserver, mysql
from ..Rule import *
import re
from ..Content import ConAttr


def column_rule(facts):
    if sqlserver(): facts = "[%s]" % facts
    elif mysql(): facts = "`%s`" % facts

    return facts


def value_rule(facts):
    if "'" in facts:
        if sqlserver():
            facts = facts.replace("'", "''")
        elif mysql():
            facts = facts.replace("'", "\\'")

    facts = "'%s'" % facts

    return facts


def slash_rule(facts):
    if "\\'" in facts:
        result = re.findall(r"\\['|\\.*]", facts, re.S | re.I)

        b = "".join(result).replace("\\'", "")
        sl_num = b.count("\\")
        if sl_num % 2 == 0:
            facts = facts.replace("\\\\", "\\")

    return facts


COL = column_rule
VAL = value_rule
SLASH = slash_rule

DEM = AND(NOT_NULL, IS_STR)
ESCAPE = SEQ(VAL, SLASH)

U_COL = IE(IF=DEM, TRUE=COL)
U_VAL = IE(IF=DEM, TRUE=ESCAPE)


JOIN = ConAttr(
    INNER='INNER',
    LEFT_OUTER='LEFT',
    RIGHT_OUTER='RIGHT')


OP = ConAttr(
    AND='AND',
    OR='OR',
    ADD='+',
    SUB='-',
    MUL='*',
    DIV='/',
    BIN_AND='&',
    BIN_OR='|',
    XOR='#',
    MOD='%',
    EQ='=',
    LT='<',
    LTE='<=',
    GT='>',
    GTE='>=',
    NE='!=',
    IN='IN',
    NOT_IN='NOT IN',
    IS='IS',
    IS_NOT='IS NOT',
    LIKE='LIKE',
    BETWEEN='BETWEEN',
    REGEXP='REGEXP',)


class Hierarchy(object):
    def __init__(self, *ctxs):
        for ctx in ctxs:
            ctx.manager = self

        self.pool = ctxs
        self.now_alias = {}


class SqlNode(object):
    def clone(self):
        obj = self.__class__.__new__(self.__class__)
        obj.__dict__ = self.__dict__.copy()
        return obj

    @staticmethod
    def copy(method):
        def inner(self, *args, **kwargs):
            clone = self.clone()
            method(clone, *args, **kwargs)
            return clone
        return inner

    @staticmethod
    def create_alias(alias_str=None):
        return Alias("", alias_str)

    def unalias(self):
        return self

    def __sql__(self, ctx):
        return ctx.sql(self)


def gen_alias():
    stop = False
    alias = []
    while True:
        num = len(alias) + 1
        alias.append("AL%s" % num)
        stop = yield alias[-1]
        if stop is True:
            break


alias_creator = gen_alias()


class Alias(SqlNode):
    def __init__(self, node, alias):
        self.node = node
        if alias:
            self._alias = alias
        else:
            self._alias = alias_creator.next()

    @property
    def name(self):
        return self._alias

    def unalias(self):
        return self.node

    def __sql__(self, ctx):
        return (ctx
                .sql(self.node)
                .literal('AS')
                .sql(self._alias))


class ColumnBase(SqlNode):
    def _e(op, inv=False):
        """
        Lightweight factory which returns a method that builds an Expression
        consisting of the left-hand and right-hand operands, using `op`.
        """
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner

    __and__ = _e(OP.AND)
    __or__ = _e(OP.OR)

    __add__ = _e(OP.ADD)
    __sub__ = _e(OP.SUB)
    __mul__ = _e(OP.MUL)
    __div__ = __truediv__ = _e(OP.DIV)
    __xor__ = _e(OP.XOR)

    __radd__ = _e(OP.ADD, inv=True)
    __rsub__ = _e(OP.SUB, inv=True)
    __rmul__ = _e(OP.MUL, inv=True)
    __rdiv__ = __rtruediv__ = _e(OP.DIV, inv=True)
    __rand__ = _e(OP.AND, inv=True)
    __ror__ = _e(OP.OR, inv=True)
    __rxor__ = _e(OP.XOR, inv=True)

    def __eq__(self, rhs):
        op = OP.IS if rhs is None else OP.EQ
        return Expression(self, op, rhs)
    def __ne__(self, rhs):
        op = OP.IS_NOT if rhs is None else OP.NE
        return Expression(self, op, rhs)

    __lt__ = _e(OP.LT)
    __le__ = _e(OP.LTE)
    __gt__ = _e(OP.GT)
    __ge__ = _e(OP.GTE)
    __lshift__ = _e(OP.IN)
    __rshift__ = _e(OP.IS)
    __mod__ = _e(OP.LIKE)

    bin_and = _e(OP.BIN_AND)
    bin_or = _e(OP.BIN_OR)
    in_ = _e(OP.IN)
    not_in = _e(OP.NOT_IN)
    regexp = _e(OP.REGEXP)

    # Special expressions.
    def is_null(self, is_null=True):
        op = OP.IS if is_null else OP.IS_NOT
        return Expression(self, op, None)
    def contains(self, rhs):
        return Expression(self, OP.ILIKE, '%%%s%%' % rhs)
    def startswith(self, rhs):
        return Expression(self, OP.ILIKE, '%s%%' % rhs)
    def endswith(self, rhs):
        return Expression(self, OP.ILIKE, '%%%s' % rhs)
    def max(self):
        return "max(%s)" % self.column_name
    def distinct(self):
        return "distinct(%s)" % self.column_name


class Expression(ColumnBase):
    def __init__(self, lhs, op, rhs, flat=False):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.flat = flat

    def __sql__(self, ctx):
        if IS_STR: rv = U_VAL(self.rhs)
        else: rv = self.rhs

        with ctx():
            return (ctx
                    .sql(self.lhs)
                    .literal(self.op)
                    .sql(rv)
                    )


class BaseJoin(SqlNode):
    def __join__(join_type='INNER', inverted=False):
        def method(self, other):
            if inverted:
                self, other = other, self
            return Join(self, other, join_type=join_type)

        return method

    __and__ = __join__(JOIN.INNER)
    __add__ = __join__(JOIN.LEFT_OUTER)
    __sub__ = __join__(JOIN.RIGHT_OUTER)
    __rand__ = __join__(JOIN.INNER, inverted=True)
    __radd__ = __join__(JOIN.LEFT_OUTER, inverted=True)
    __rsub__ = __join__(JOIN.RIGHT_OUTER, inverted=True)


class Join(BaseJoin):
    def __init__(self, lhs, rhs, join_type=JOIN.INNER, on=None):
        self.lhs = lhs
        self.rhs = rhs
        self.join_type = join_type
        self._on = on

    def __sql__(self, ctx):
        lhs_alias = ctx.model_alias[self.lhs.__class__]
        rhs_alias = ctx.model_alias[self.rhs.__class__]

        (ctx
             .sql(self.lhs)
             .sql(lhs_alias)
             .literal('\n%s JOIN' % self.join_type)
             .sql(self.rhs)
             .sql(rhs_alias)
        )
        if self._on:
            lname = "%s.%s" % (lhs_alias.name, self._on.lhs.column_name)
            rname = "%s.%s" % (rhs_alias.name, self._on.rhs.column_name)
            on = "%s = %s" % (lname, rname)
            ctx.literal('\nON').sql(on)


class SqlContext(object):
    def __init__(self):
        self._stack = {}
        self.values = {}
        self._sql = []
        self.manager = None

    @property
    def model_alias(self):
        if not self.manager:
            return None
        else:
            return self.manager.now_alias

    def __call__(self):
        return self

    def __enter__(self):
        self.literal('(')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.literal(')')

    def make_alias(self, model):
        alias_obj = SqlNode.create_alias()
        self.model_alias[model] = alias_obj
        # self.model_alias[model] = alias_obj

    def sql(self, obj):
        if obj is None:
            return self.literal("null")
        elif isinstance(obj, SqlNode):
            return obj.__sql__(self)
        else:
            return self.literal(obj)

    def to_sql(self, warehouse, div=" ", query_type=False, parentheses=False):
        sql_list = self._stack.get(warehouse, [])
        if not sql_list:
            return self

        def build_sub():
            for i in range(len(sql_list)):
                each = sql_list[i]
                if query_type:
                    if mysql():
                        self.sql("%s")
                    elif sqlserver():
                        self.sql("?")
                else:
                    self.sql(each)

                if i == len(sql_list) - 1:
                    continue

                self.sql("\n%s" % div)

        self.sql("\n%s" % warehouse)

        if parentheses:
            with self():
                build_sub()
        else:
            build_sub()

        return self

    def press(self, warehouse, val):
        if self._stack.has_key(warehouse):
            li = self._stack[warehouse]
            li.append(val)
        else:
            self._stack[warehouse] = [val]

    def is_exist(self, warehouse):
        if self._stack.has_key(warehouse):
            return True
        return False

    def literal(self, keyword):
        self._sql.append(str(keyword))
        return self

    def query(self):
        return " ".join(self._sql)

    def clear(self):
        self._sql, self._stack, self.now_alias = [], {}, []












CAN_NOT_LIST_ERR = ERR(TypeError, "Parameter can not be list or tuple")
CAN_NOT_DICT_ERR = ERR(TypeError, "Parameter can not be dict")


LOWER_FORMAT = SEQ(LOWER, TO_STR)
UPPER_FORMAT = SEQ(UPPER, TO_STR)

ARR_TO_STR = "IE(IF=IS_STR, TRUE=UN_DO, FALSE=JOIN('%s'))"

FORMAT_COLUMNS = FOR_LIST(ITEM_RULE=U_COL)

"""
    The rule for fields(insert or select)
"""
field_to_str = FUNC(ARR_TO_STR, ',')

filed_dict_format = FOR_DICT(KEY_RULE=U_COL)
field_list_format = FOR_LIST(ITEM_RULE=U_COL)

f_dict = {IS_DICT: filed_dict_format}
f_list = {IS_LIST: field_list_format}
f_str = {IS_STR: LOWER_FORMAT}
use_field_format = DECT(f_str, f_list, f_dict)


"""
    The rule for where
"""
where_to_str = FUNC(ARR_TO_STR, ' AND ')

# where_item_join = JOIN(' = ')
where_dict_format = FOR_DICT(KEY_RULE=U_COL, VAL_RULE=U_VAL)

w_list = {IS_LIST: CAN_NOT_LIST_ERR}  # raise error
w_dict = {IS_DICT: where_dict_format}
w_str = {IS_STR: LOWER_FORMAT}

user_where_format = DECT(w_str, w_dict, w_list)


"""
    The rule for insert
"""
user_insert_format = FOR_DICT(KEY_RULE=U_COL, VAL_RULE=UN_DO)


class BaseBuilder(object):
    PLA = "%s"

    def assemble(self, *args):
        raise SQLError("Error with assemble sql. haven't override [assemble] method")

    def build(self, *args, **kwargs):
        raise SQLError("Error with build sql. haven't override [build] method")


class SelectSQL(BaseBuilder):
    def attach(self, addition):
        if addition and IS_LIST(addition):
            return self.assemble(",", *addition)
        elif not addition:
            return ''
        return addition

    def assemble(self, join_str=" ", *args):
        if len(args) == 0:
            return ""
        if len(args) == 1 and IS_LIST(args):
            return args[0]
        elif IS_STR(args):
            return args

        sl = list()
        for item in args:
            if not item:
                continue
            sl.append(str(item))
        return join_str.join(sl)

    def build(self, ctx, query_type):
        columns = ctx.get("column")
        wheres = ctx.get("where")
        group_by = self.attach(ctx.get("group_by", None))
        order_by = self.attach(ctx.get("order_by", None))
        limit = self.attach(ctx.get("limit", None))

        where_str = wheres
        if IS_DICT(wheres):
            format_wheres = [(U_COL(k), U_VAL(v)) for k, v in wheres.items()]
            where_items = [self.assemble(" = ", *w_items) for w_items in format_wheres]
            where_str = self.assemble(" AND ", *where_items)

        if IS_LIST(columns):
            format_column = FORMAT_COLUMNS(columns)
            column_str = self.assemble(",", *format_column)
        else:
            column_str = columns

        sql = self.assemble(" "
                            , *("SELECT"
                                , column_str
                                , "FROM"
                                , ctx.get("path")
                                , "WHERE" if where_str else ''
                                , where_str
                                , "GROUP BY" if group_by else ''
                                , group_by
                                , "ORDER BY" if order_by else ''
                                , order_by
                                , "LIMIT" if limit else ''
                                , limit
                                )
                            )

        return sql, None


class UpdateSQL(BaseBuilder):
    def query_build(self, ctx):
        columns = ctx.get("column")
        values = ctx.get("value")
        wheres = ctx.get("where")
        format_col = FORMAT_COLUMNS(columns)

        if IS_DICT(wheres):
            format_where = [(U_COL(k), U_VAL(v)) for k, v in wheres.items()]
            where_list = [a for a in format_where]
            where_str = "%s" % where_to_str(where_list)
        else: where_str = "%s" % wheres

        set_list, query_value = list(), list()
        for fc, fv in zip(format_col, values):
            set_list.append(" = ".join((fc, self.PLA)))
            query_value.append(fv)
        sets = ",".join(set_list)

        sql_list = ["UPDATE", ctx.get("path"), "SET", sets, "WHERE", where_str]
        sql = " ".join(sql_list)

        return sql, query_value

    def build(self, ctx, query_type):
        if query_type:
            return self.query_build(ctx)


class InsertSQL(BaseBuilder):
    def query_build(self, ctx):
        columns = ctx.get("column")
        values = ctx.get("value")
        format_col = FORMAT_COLUMNS(columns)
        # f_col_value = user_insert_format(columns)

        format_col_list, plas, query_value = list(), list(), list()
        for fc, fv in zip(format_col, values):
            format_col_list.append(fc)
            plas.append(self.PLA)
            query_value.append(fv)

        fc_str = "(%s)" % ",".join(format_col_list)
        fvp_str = "(%s)" % ",".join(plas)

        sql_list = ["INSERT INTO", ctx.get("path"), fc_str, "VALUES", fvp_str]
        sql = " ".join(sql_list)

        return sql, query_value

    def build(self, ctx, query_type):
        try:
            if query_type:
                return self.query_build(ctx)
            else:
                return "", None
        except Exception, e:
            raise e


class CreateSQL(BaseBuilder):
    def build(self, ctx):
        table_name = ctx.get("path")
        engine = ctx.get("engine")
        charset = ctx.get("charset")
        col_info = ctx.get("col_info")
        pk = ""

        info_list = []
        for each in col_info:
            fd_name, fd_type, fd_len, fd_default, fd_null = each
            fd_len_str = "(%s)" % fd_len if fd_len else ''
            fd_default_str = "DEFAULT %s" % U_VAL(fd_default) if fd_default else ''
            fd_null_str = 'NOT NULL' if fd_null is False else ''
            if fd_type == "AUTO":
                info = [U_COL(fd_name), "BIGINT(20)", "NOT NULL", "AUTO_INCREMENT"]
                pk = "PRIMARY KEY (%s)" % U_COL(fd_name)
            else:
                info = [U_COL(fd_name), fd_type, fd_len_str, fd_default_str, fd_null_str]
            info_str = " ".join(info)
            info_list.append(info_str)

        info_list.append(pk)
        all_info = "(\n%s\n)" % ",\n ".join(info_list)
        sql_list = ["CREATE TABLE", U_COL(table_name), all_info, "ENGINE=", engine, "CHARSET=", charset]
        sql = " ".join(sql_list)

        return sql

sel_builder = SelectSQL()
ins_builder = InsertSQL()
update_builder = UpdateSQL()
create_builder = CreateSQL()


def proc_sql(proc_name, parm):
    proc_head = "call %s" % proc_name
    parm_head = "("
    parm_body = ''

    if len(parm) > 0:
        for i in xrange(len(parm)):
            parm_head += "%s,"
        parm_head = parm_head % parm
        parm_body = parm_head[0:-1] + ')'

    fs = proc_head + parm_body
    return fs


def select_sql(ctx, query_type=True):
    ctx \
        .to_sql("SELECT", div=",") \
        .to_sql("FROM") \
        .to_sql("WHERE", div="AND") \
        .to_sql("GROUP BY", div=",") \
        .to_sql("ORDER BY", div=",") \
        .to_sql("LIMIT", div=",")

    return ctx.query()

    # return sel_builder.build(ctx, query_type)


def update_sql(ctx, query_type=True):
    ctx\
        .to_sql("UPDATE")\
        .to_sql("SET", div=",")\
        .to_sql("WHERE", div="AND")

    return ctx.query()
    # sql = update_builder.build(ctx, query_type)
    # return sql


def insert_sql(ctx, path, query_type=True):
    ctx\
        .to_sql("INSERT INTO", div=",")\
        .to_sql(path, div=",", parentheses=True)\
        .to_sql("VALUES", div=",", query_type=query_type, parentheses=True)

    return ctx.query()


def create_sql(ctx):
    return create_builder.build(ctx)
