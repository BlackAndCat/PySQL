MYSQL = "MySql"
SQL_SERVER = "SqlServer"
REDIS = 'Redis'


DB_TYPE = None

is_mysql = None

is_sqlserver = None


MYSQL_SYS_SQL = {
    "get_databases": "SHOW DATABASES;"
    , "get_tables": "SHOW TABLES;"
    , "get_table_details":
        "SELECT * "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = '%s' AND table_name = '%s'"
}


SQLSERVER_SYS_SQL = {
    "get_databases": "SELECT * FROM sys.databases"
    , "get_tables": "SELECT name FROM %s.sys.tables"
    , "get_table_details": """
            SELECT col.name as name
                , stype.name as type
                , case when col.is_nullable = 0 then 'not_null' else 'null' end as nullable
                , col.max_length as max_length
                , col.column_id
            FROM %s.sys.columns as col
                LEFT JOIN %s.sys.objects as obj
                ON col.object_id = obj.object_id
                LEFT JOIN %s.sys.types as stype
                ON col.system_type_id = stype.system_type_id
            WHERE obj.name = '%s' and stype.name != 'sysname'
            ORDER by col.column_id
    """
}


def mysql():
    global is_mysql
    if is_mysql is not None:
        return is_mysql
    elif is_mysql is None:
        if DB_TYPE == "MySql":
            is_mysql = True
        else:
            is_mysql = False

    return is_mysql


def sqlserver():
    global is_sqlserver
    if is_sqlserver is not None:
        return is_sqlserver
    elif is_sqlserver is None:
        if DB_TYPE == "SqlServer":
            is_sqlserver = True
        else:
            is_sqlserver = False

    return is_sqlserver


def db_type():
    return DB_TYPE
