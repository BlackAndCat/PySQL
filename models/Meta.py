from PySQL.util.SqlBuilder import mysql, sqlserver
from .Field import Field, COLUMN_SIGN, CHECK_LIST, LITTLE_ESCAPGE


class Meta(object):
    table_name = None

    def __init__(self, en_cls, table_name, db_name, column, DB_TYPE):
        self.table_name = table_name
        self.db_name = db_name
        self.db_type = DB_TYPE

        self.table_columns = []

        if sqlserver():
            self.set_column_sqlserver(column, en_cls)
        elif mysql():
            self.set_column_mysql(column, en_cls)

        self.full_path = self.build_full_path()

        self.en_cls = en_cls
        self.columns = {}

        for item in dir(en_cls):
            if item.startswith("_"): continue

            item_instance = getattr(en_cls, item)
            if not isinstance(item_instance, Field): continue

            self.columns[item] = item_instance

    def build_full_path(self):
        if sqlserver():
            return self.db_name + ".dbo." + self.table_name
        elif mysql():
            return self.db_name + "." + self.table_name

    def _set_pk(self, field):
        name = field.get("name")
        self.pk = name

    def little_escape(self, f):
        if f == "tinyint":
            return "BOOL"
        elif f == "bit":
            return "INT"
        elif f == "decimal":
            return "FLOAT"
        elif f == "timestamp":
            return "DATETIME"
        else:
            return f.upper()

    def set_column_mysql(self, column, en_cls):
        for table_field in column:
            # table_name = field.get("TABLE_SCHEMA")
            column_name = table_field.get("COLUMN_NAME")
            key_type = table_field.get("COLUMN_KEY")
            default = table_field.get("COLUMN_DEFAULT")

            nullable = table_field.get("IS_NULLABLE")
            table_data_type = table_field.get("DATA_TYPE")

            if key_type == "PRI":
                self.pk = column_name

            lower_col_name = column_name.lower()
            en_col_name = en_cls._en_column.get(lower_col_name)
            if en_col_name is None:
                self.table_columns.append(column_name)
                continue

            en_field = getattr(en_cls, en_col_name, None)
            if not isinstance(en_field, Field):
                ftype = CHECK_LIST.get(table_data_type)
                assert ftype is not None\
                    , "[PySQL] Error, Can not find original for field:[%s] -- type:[%s]" % (column_name, table_data_type)

                en_field = ftype()
                setattr(en_cls, en_col_name, en_field)

            en_field.null = False if nullable == "NO" else True
            en_field.primary_key = True if key_type == "PRI" else False
            if en_field.primary_key:
                self.pk = en_col_name

            en_field.column_name = column_name
            en_field.default = default

            en_field_type = en_field.field_type

            assert en_field_type == LITTLE_ESCAPGE.get(table_data_type, table_data_type.upper()) \
                   or en_field.primary_key\
                , "[PySQL]: Error, db field type isn't equals entity field type." \
                  "table_name:[%s]  filed:[%s] db:[%s] -- en:[%s]" \
                  % (self.table_name, en_col_name, table_data_type, en_field_type)

            if table_data_type in COLUMN_SIGN["number"]:
                field_len = table_field.get("NUMERIC_PRECISION")
            elif table_data_type in COLUMN_SIGN["float"]:
                field_len = table_field.get("NUMERIC_PRECISION")
                scale_len = table_field.get("NUMERIC_SCALE")
                en_field.scale = scale_len
            elif table_data_type in COLUMN_SIGN["char"]:
                field_len = table_field.get("CHARACTER_OCTET_LENGTH")
            elif table_data_type in COLUMN_SIGN["datetime"]:
                field_len = 24
            else:
                raise Exception("Unknow field type %s->%s" % (en_col_name, table_data_type))

            en_field.max_length = field_len

            en_field.bind(en_cls, en_col_name)

    def set_column_sqlserver(self, column, en_cls):
        for field in column:
            column_id = field.get("column_id")
            if column_id == 1:
                self._set_pk(field)

            key = field.get("name")

            self.table_col[key] = field
