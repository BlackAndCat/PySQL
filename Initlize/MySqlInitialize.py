from PySQL.Content import Content
from PySQL.util import SqlBuilder, Interpreter
from PySQL.util.MySqlHelper import MySqlHelper
from PySQL.util.Interpreter import mysql, sqlserver, db_type
from PySQL.models.Meta import Meta
from PySQL.Interface import SqlHelper


class MySqlInitialize(object):
    mapping = None
    session = None

    def __init__(self):
        self.mapping = Content.map
        self.session = Content.session

    def get_sql(self, sql_name):
        return Interpreter.MYSQL_SYS_SQL.get(sql_name)

    def init_conns(self, need_init_conns):
        pools = self.mapping.get_pools()

        for k, conn_info in pools.items():
            if isinstance(conn_info, SqlHelper):  # It's has been created
                continue

            db_name = conn_info.get("db")

            if k in need_init_conns \
                    or db_name in need_init_conns:
                conn = MySqlHelper(conn_info)

                pool_name = conn_info.get("conn_name")
                self.mapping.update_pool(pool_name, conn)

    def sel_tables(self, db_name):
        try:
            sql = self.get_sql("get_tables")

            tables = self.session.execute_select(sql, db_name=db_name)
            return [each.values()[0] for each in tables]
        except Exception, ex:
            raise Exception("[PySQL] Failed select table from db:[%s], ex:[%s]" % (db_name, str(ex)))

    def find_table(self, entities, tables):
        tb_names = set([tb.values()[0] for tb in tables])
        en_names = set(entities)

        return list(tb_names & en_names)

    def build_entity(self, db_name, table_name, en_cls):
        sql = ''
        if sqlserver():
            sql = self.get_sql("get_table_details") % (db_name, db_name, db_name, table_name)
        elif mysql():
            sql = self.get_sql("get_table_details") % (db_name, table_name)
        column_args = self.session.execute_select(sql, db_name=db_name)

        en_cls._en_column = en_cls.en_col_mapping()

        meta = Meta(en_cls, table_name, db_name, column_args, db_type())
        en_cls._meta = meta

        self.mapping.put_table_couple(table_name, db_name)
        self.mapping.put_entity_couple(table_name, en_cls)

    def create_table(self, en_cls, entity_name, db_name):
        en_col = en_cls.en_col_mapping()
        info_list = {}
        for field_name in en_col.values():
            col_info = getattr(en_cls, field_name)
            col_name = field_name
            field_type = col_info.field_type
            field_len = col_info.max_length
            field_default = col_info.default
            null_able = col_info.null

            info_list[col_info._order] = [col_name, field_type, field_len, field_default, null_able]

        ctx = {
            "path": entity_name,
            "col_info": [each[1] for each in sorted(info_list.items())],
            "engine": "innodb",
            "charset": "utf8"
        }
        sql = SqlBuilder.create_sql(ctx)
        self.session.execute_insert(sql, db_name=db_name)

    def init_tables(self, db_name):
        tbs = self.sel_tables(db_name)
        for tb in tbs:
            self.mapping.put_table_couple(tb, db_name)
        return tbs

    def create_map(self, all_entity):
        """
        :param all_entity: All need to create entity
        :return:
        """
        self.init_conns(all_entity.keys())
        err_sign = None

        for db_name in all_entity:
            try:
                tables_name = self.init_tables(db_name)
                entities = all_entity[db_name]

                for entity_name, parm in entities.items():
                    en_cls = all_entity[db_name][entity_name]["cls"]
                    if entity_name not in tables_name:
                        if parm.get("create") is False:
                            raise Exception(
                                "There is no table in database:[%s], table_name:[%s]" % (db_name, entity_name))

                        # It's need be create
                        self.create_table(en_cls, entity_name, db_name)

                    self.build_entity(db_name, entity_name, en_cls)
            except Exception, e:
                print str(e)
                err_sign = e

        if err_sign:
            raise err_sign
